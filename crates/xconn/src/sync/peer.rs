use crate::common::types::{Error, TransportType};
use std::fmt::Debug;

pub trait Peer: Debug + Send + Sync {
    fn kind(&self) -> TransportType;

    fn read(&self) -> Result<Vec<u8>, Error>;

    fn write(&self, data: Vec<u8>) -> Result<(), Error>;

    /// Close the transport and wake any pending read. Custom peers should override this.
    fn close(&self) -> Result<(), Error> {
        Err(Error::new("this synchronous peer does not implement close"))
    }
}
