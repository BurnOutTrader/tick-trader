// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2025 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

use std::{num::NonZeroU32, sync::LazyLock};

use nautilus_model::identifiers::Venue;
use nautilus_network::ratelimiter::quota::Quota;
use ustr::Ustr;

pub const PROJECT_X: &str = "ProjectX";
pub static PROJECT_X_VENUE: LazyLock<Venue> = LazyLock::new(|| Venue::new(Ustr::from(PROJECT_X)));

pub static DEMO_DOMAIN: &str = "https://gateway-api-demo.s2f.projectx.com";
pub static DEMO_STREAM: &str = "https://gateway-rtc-demo.s2f.projectx.com";

// Global rate-limit key namespace for ProjectX HTTP client
pub const PX_GLOBAL_RATE_KEY: &str = "px:global";

pub static PX_REST_QUOTA: LazyLock<Quota> =
    LazyLock::new(|| Quota::per_minute(NonZeroU32::new(200).unwrap()));

pub static PX_BARS_QUOTA: LazyLock<Quota> = LazyLock::new(|| {
    Quota::with_period(std::time::Duration::from_secs(30))
        .unwrap()
        .allow_burst(NonZeroU32::new(50).unwrap())
});
