use crate::messages::message::Message;
use crate::messages::types::Error;
use std::fmt::Debug;

pub trait Serializer: XClone + Debug + Send + Sync {
    fn serialize(&self, message: &dyn Message) -> Result<Vec<u8>, Error>;

    fn deserialize(&self, payload: Vec<u8>) -> Result<Box<dyn Message>, Error>;

    fn is_static(&self) -> bool;
}

pub trait XClone {
    fn clone_box(&self) -> Box<dyn Serializer>;
}

impl<T> XClone for T
where
    T: Serializer + Clone + 'static,
{
    fn clone_box(&self) -> Box<dyn Serializer> {
        Box::new(self.clone())
    }
}

impl Clone for Box<dyn Serializer> {
    fn clone(&self) -> Box<dyn Serializer> {
        self.clone_box()
    }
}
