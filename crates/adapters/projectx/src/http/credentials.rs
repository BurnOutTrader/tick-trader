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

use std::fmt::Debug;

use ustr::Ustr;
use zeroize::ZeroizeOnDrop;

/// ProjectX credentials used to authenticate HTTP requests
///
/// Fields:
/// - firm: the ProjectX firm identifier
/// - user_name: the ProjectX login user name, redacted in Debug
/// - api_key: the ProjectX API key, stored as Ustr and redacted in Debug.
#[derive(Clone, ZeroizeOnDrop)]
pub struct PxCredential {
    pub firm: String,
    #[zeroize(skip)]
    pub user_name: String,
    #[zeroize(skip)]
    pub api_key: Ustr,
}

impl PxCredential {
    /// Create a new credential from explicit values
    pub fn new(firm: String, user_name: String, api_key: String) -> Self {
        Self {
            firm,
            user_name,
            api_key: api_key.into(),
        }
    }

    /// Load credentials from the process environment
    ///
    /// Expected variables: PX_FIRM, PX_USERNAME, PX_API_KEY.
    pub fn from_env() -> anyhow::Result<Self> {
        Ok(Self {
            firm: std::env::var("PX_FIRM")?,
            user_name: std::env::var("PX_USERNAME")?,
            api_key: std::env::var("PX_API_KEY")?.into(),
        })
    }
}

impl Debug for PxCredential {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(stringify!(Credential))
            .field("firm", &self.firm)
            .field("user_name", &"<redacted>")
            .field("api_key", &"<redacted>")
            .finish()
    }
}
