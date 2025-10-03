## Symbology
Instrument is the root + month code + year code
```rust
let instrument: Instrument = Instrument::from_str("MNQZ5").unwrap(),
```
Symbol is the root 
```rust
let symbol: String = "MNQ".to_string();
```