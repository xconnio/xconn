use crate::messages::types::Error;
use rand_core::{OsRng, RngCore};
use std::sync::Mutex;

pub const ID_MIN: i64 = 1;
pub const ID_MAX: i64 = 1 << 53;

pub fn generate_session_id() -> Result<i64, Error> {
    let mut bytes = [0u8; 8];
    OsRng
        .try_fill_bytes(&mut bytes)
        .map_err(|e| Error::new(format!("failed to generate session ID: {e}")))?;
    // The domain has exactly 2^53 elements, so masking gives an unbiased sample.
    Ok((u64::from_le_bytes(bytes) & (ID_MAX as u64 - 1)) as i64 + 1)
}

#[derive(Debug)]
pub struct SessionScopeIDGenerator {
    id: Mutex<i64>,
}

impl SessionScopeIDGenerator {
    pub fn new() -> Self {
        Self { id: Mutex::new(0) }
    }

    pub fn next_id(&self) -> i64 {
        let mut id = self.id.lock().unwrap();

        if *id == ID_MAX {
            *id = 0;
        }

        *id += 1;
        *id
    }

    pub fn current_id(&self) -> i64 {
        *self.id.lock().unwrap()
    }

    /// Restore the counter without changing the next/wrap semantics.
    pub fn set_current_id(&self, id: i64) -> Result<(), Error> {
        if !(0..=ID_MAX).contains(&id) {
            return Err(Error::new("counter must be between 0 and ID_MAX"));
        }

        *self.id.lock().unwrap() = id;
        Ok(())
    }
}

impl Default for SessionScopeIDGenerator {
    fn default() -> Self {
        Self::new()
    }
}
