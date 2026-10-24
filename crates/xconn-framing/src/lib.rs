//! Transport-independent data-channel framing shared by native WebRTC and WASM.
use bytes::Bytes;
use std::fmt;

pub const RAW_CHANNEL_PROTOCOL: &str = "xconn.raw";

pub const MTU: usize = 16 * 1024;
pub const MAX_PACKET_SIZE: usize = 65535;
pub const DEFAULT_MAX_MESSAGE: usize = 16 * 1024 * 1024;

#[derive(Debug)]
pub struct Error(pub String);

impl fmt::Display for Error {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl std::error::Error for Error {}

/// Go-compatible chunk framing, with bounded memory and strict flag validation.
pub struct Assembler {
    buffer: Vec<u8>,
    max_message: usize,
}

impl Assembler {
    pub fn new(max_message: usize) -> Self {
        Self {
            buffer: Vec::new(),
            max_message,
        }
    }

    pub fn set_limit(&mut self, limit: usize) {
        self.max_message = limit;
    }

    pub fn feed(&mut self, chunk: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        if chunk.is_empty() || chunk.len() > MTU || chunk[0] > 1 {
            self.buffer.clear();
            return Err(Error("invalid WebRTC chunk".into()));
        }

        if self.buffer.len().saturating_add(chunk.len() - 1) > self.max_message {
            self.buffer.clear();
            return Err(Error("message exceeds receive limit".into()));
        }

        self.buffer.extend_from_slice(&chunk[1..]);

        if chunk[0] == 1 {
            Ok(Some(std::mem::take(&mut self.buffer)))
        } else {
            Ok(None)
        }
    }

    pub fn chunks(message: &[u8]) -> impl Iterator<Item = Bytes> + '_ {
        // Even an empty message needs a final marker.
        let total = message.len().div_ceil(MTU - 1).max(1);
        (0..total).map(move |index| {
            let start = index * (MTU - 1);
            let end = (start + MTU - 1).min(message.len());
            let mut chunk = Vec::with_capacity(end - start + 1);
            chunk.push(u8::from(index + 1 == total));
            chunk.extend_from_slice(&message[start..end]);
            Bytes::from(chunk)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chunk_boundaries_and_empty_messages_round_trip() {
        for size in [0, 1, MTU - 1, MTU, MTU * 4] {
            let input = vec![42; size];
            let mut decoder = Assembler::new(DEFAULT_MAX_MESSAGE);
            let mut result = None;
            for chunk in Assembler::chunks(&input) {
                result = decoder.feed(&chunk).unwrap();
            }
            assert_eq!(result.unwrap(), input);
        }
    }

    #[test]
    fn invalid_chunks_and_overflow_reset_the_partial_message() {
        let mut decoder = Assembler::new(3);
        assert!(decoder.feed(&[0, 1, 2]).unwrap().is_none());
        assert!(decoder.feed(&[1, 3, 4]).is_err());
        assert_eq!(decoder.feed(&[1, 9]).unwrap(), Some(vec![9]));
        assert!(decoder.feed(&[2]).is_err());
        assert!(decoder.feed(&[]).is_err());
    }
}
