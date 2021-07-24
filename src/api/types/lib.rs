use std::error::Error;
use std::fmt::Display;
use std::fmt;

#[derive(Copy, Clone, PartialEq, Eq, Debug)]
pub struct DowncastError {
    pub from: &'static str,
    pub to: &'static str,
}
impl Error for DowncastError {
    fn description(&self) -> &str {
        "invalid downcast"
    }
}
impl Display for DowncastError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "could not downcast \"{}\" to \"{}\"", self.from, self.to)
    }
}