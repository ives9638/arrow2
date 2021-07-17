pub use alloc::total_allocated_bytes;

pub mod alloc;
pub mod array;
pub mod bitmap;
pub mod buffer;
mod endianess;
pub mod error;
pub mod trusted_len;
pub mod types;

pub mod compute;
pub mod datatypes;
pub mod io;
pub mod record_batch;
pub mod temporal_conversions;

pub mod ffi;
pub mod util;

// so that documentation gets test
mod api;
#[cfg(any(test, doctest))]
mod docs;
mod scalar;
