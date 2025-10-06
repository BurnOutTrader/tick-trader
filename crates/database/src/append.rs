use anyhow::{Context, Result};
use duckdb::Connection;
use std::fs;
use std::path::{Path, PathBuf};
use tempfile::NamedTempFile;

/// Atomic “append” that never loses existing data:
/// 1) We read existing parquet and new batch parquet.
/// 2) UNION ALL and de-dup by a composite key (your choice).
/// 3) Sort by a stable ordering (e.g., ts, venue_seq, exec_id).
/// 4) Write to tmp with max compression.
/// 5) Atomic replacement.
///
/// Notes:
/// • If `existing_path` doesn’t exist, we just sort/dedup the new batch and move it in.
/// • `key_cols` MUST uniquely identify a row (e.g. ["time","venue_seq","exec_id"] for ticks).
/// • `order_by` defines on-disk order (improves scan locality).
pub fn append_merge_parquet(
    conn: &Connection,
    existing_path: &Path,
    new_batch_parquet: &Path,
    key_cols: &[&str],
    order_by: &[&str],
) -> Result<()> {
    // Ensure dirs for the final location exist
    if let Some(parent) = existing_path.parent() {
        fs::create_dir_all(parent)?;
    }

    let key_list = key_cols.join(", ");
    let order_list = if order_by.is_empty() {
        key_list.clone()
    } else {
        order_by.join(", ")
    };

    // When the file doesn't exist yet OR existing file is empty/corrupt, short-circuit: move the new parquet in place without invoking DuckDB.
    let mut exists = existing_path.exists();
    if exists {
        if let Ok(meta) = fs::metadata(existing_path) {
            if meta.len() == 0 {
                // Remove the zero-byte file to avoid DuckDB assertion when reading it
                let _ = fs::remove_file(existing_path);
                exists = false;
            }
        }
    }
    if !exists {
        if let Some(parent) = existing_path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::rename(new_batch_parquet, existing_path)
            .or_else(|_| {
                // Fallback: copy if cross-device rename fails
                fs::copy(new_batch_parquet, existing_path).map(|_| ())
            })
            .with_context(|| {
                format!(
                    "Failed to place first parquet at {}",
                    existing_path.display()
                )
            })?;
        return Ok(());
    }

    // Optionally allow bypassing DuckDB merge to diagnose crashes
    let bypass_merge = std::env::var("TICKTRADE_DISABLE_DUCKDB_MERGE")
        .ok()
        .as_deref()
        == Some("1");
    if bypass_merge {
        tracing::warn!(
            "TICKTRADE_DISABLE_DUCKDB_MERGE=1: replacing existing parquet without merge (debug mode)"
        );
        // Replace existing with incoming to avoid invoking DuckDB at all
        fs::rename(new_batch_parquet, existing_path)
            .or_else(|_| {
                if existing_path.exists() {
                    let _ = fs::remove_file(existing_path);
                }
                fs::copy(new_batch_parquet, existing_path).map(|_| ())
            })
            .with_context(|| {
                format!("Bypass-merge move failed into {}", existing_path.display())
            })?;
        return Ok(());
    }

    // Build SQL that merges existing + incoming with union-by-name, de-duplicates by key, orders deterministically
    let p_existing = existing_path.to_string_lossy().replace('\'', "''");
    let p_incoming = new_batch_parquet.to_string_lossy().replace('\'', "''");
    tracing::debug!(existing=%p_existing, incoming=%p_incoming, keys=%key_list, order=%order_list, "duckdb merge/parquet");
    let select_union = format!(
        "WITH unioned AS (
             SELECT * FROM read_parquet(['{p1}', '{p2}'], union_by_name=1)
         ),
         ranked AS (
             SELECT u.*, ROW_NUMBER() OVER (PARTITION BY {keys} ORDER BY {keys}) AS rn
             FROM unioned u
         )
         SELECT * EXCLUDE (rn) FROM ranked WHERE rn=1 ORDER BY {order_by}",
        p1 = p_existing,
        p2 = p_incoming,
        keys = key_list,
        order_by = order_list
    );

    // Write to temporary parquet with max compression.
    // We use ZSTD + small row groups to compress hard (tune ROW_GROUP_SIZE for your workload).
    let tmp = NamedTempFile::new_in(
        existing_path
            .parent()
            .unwrap_or_else(|| Path::new("../../../..")),
    )?;
    let tmp_path: PathBuf = tmp.path().to_path_buf();
    drop(tmp); // DuckDB will create/overwrite; we just reserved a path on the same filesystem.

    let copy_sql = format!(
        "COPY ({select_union})
         TO '{}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE {row_group})",
        tmp_path.display(),
        select_union = select_union,
        row_group = 512_000
    );
    tracing::debug!(tmp=%tmp_path.display().to_string(), "duckdb COPY starting");

    conn.execute_batch(&copy_sql)
        .with_context(|| "DuckDB COPY to tmp parquet failed")?;
    tracing::debug!("duckdb COPY finished");

    // Atomically replace: rename tmp -> final.
    // On Windows, std::fs::rename will replace the same volume; on POSIX it’s atomic.
    fs::rename(&tmp_path, existing_path)
        .or_else(|_| {
            // Fallback: remove and rename (rarely needed)
            if existing_path.exists() {
                let _ = fs::remove_file(existing_path);
            }
            fs::rename(&tmp_path, existing_path)
        })
        .with_context(|| {
            format!(
                "Failed to move merged parquet into {}",
                existing_path.display()
            )
        })?;

    Ok(())
}
