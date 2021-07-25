#![feature(hash_raw_entry)]

#[cfg(test)]
mod data_block_test;

mod data_block;
mod data_block_debug;

pub use data_block::DataBlock;
pub use data_block_debug::*;

pub trait As<T>
where
    T: Cast<Self> + islist,
    Self: Sized,
{
    fn _as(t: T) -> Result<Self, DowncastError>
    where
        Self: Sized;
}

pub trait Cast<T> {
    fn cast(self) -> Result<T, DowncastError>;
}
impl<A, B> Cast<A> for B
where
    A: As<B>,
{
    fn cast(self) -> Result<A, DowncastError> {
        A::_as(self)
    }
}
