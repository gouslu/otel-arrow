// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! This crate contains the rust implementation of the OTEL Arrow Protocol.
//! It contains code for decoding OTAP Arrow record batches into OTLP protos,
//! and encoding OTLP protos into OTAP Messages. It also contains
//! the rust implementation of pdata.

#[allow(dead_code)]
pub(crate) mod arrays;
mod decode;

pub mod encode;
pub mod error;
pub mod otap;
pub mod otlp;
#[allow(dead_code)]
pub mod schema;
#[cfg(test)]
mod test_util;
#[cfg(test)]
mod validation;

pub mod pdata;
pub mod proto;

pub use decode::decoder::Consumer;
pub use encode::producer::Producer;

// Debug tests for EncodedLen visitor functionality
//pub mod debug_test;
