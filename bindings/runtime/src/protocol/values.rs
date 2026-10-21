use xconn_core::wampproto;
// A tagged value transfer format for foreign memory, not a WAMP wire serializer.
use wampproto::messages::types::{ByteArray, Value};

pub const MAX_BYTES: usize = 64 * 1024 * 1024;
const MAX_DEPTH: usize = 64;

pub fn encode(value: &Value) -> Result<Vec<u8>, String> {
    fn write(value: &Value, out: &mut Vec<u8>, depth: usize) -> Result<(), String> {
        if depth > MAX_DEPTH {
            return Err("value nesting exceeds 64".into());
        }

        fn length(out: &mut Vec<u8>, size: usize) -> Result<(), String> {
            let size = u32::try_from(size).map_err(|_| "value length exceeds u32")?;
            out.extend_from_slice(&size.to_le_bytes());
            Ok(())
        }

        match value {
            Value::Null => out.push(0),
            Value::Bool(false) => out.push(1),
            Value::Bool(true) => out.push(2),
            Value::Int(value) => {
                out.push(3);
                out.extend_from_slice(&value.to_le_bytes());
            }
            Value::UInt(value) => {
                out.push(4);
                out.extend_from_slice(&value.to_le_bytes());
            }
            Value::Float(value) => {
                out.push(5);
                out.extend_from_slice(&value.to_le_bytes());
            }
            Value::Str(value) => {
                out.push(6);
                length(out, value.len())?;
                out.extend_from_slice(value.as_bytes());
            }
            Value::Bytes(value) => {
                out.push(7);
                length(out, value.len())?;
                out.extend_from_slice(value);
            }
            Value::List(values) => {
                out.push(8);
                length(out, values.len())?;

                for value in values {
                    write(value, out, depth + 1)?;
                }
            }
            Value::Dict(values) => {
                out.push(9);
                length(out, values.len())?;

                for (key, value) in values {
                    length(out, key.len())?;
                    out.extend_from_slice(key.as_bytes());
                    write(value, out, depth + 1)?;
                }
            }
        }

        if out.len() > MAX_BYTES {
            return Err("value transfer exceeds 64 MiB".into());
        }

        Ok(())
    }

    let mut out = Vec::new();
    write(value, &mut out, 0)?;
    Ok(out)
}

pub fn decode(input: &[u8]) -> Result<Value, String> {
    struct Reader<'a> {
        input: &'a [u8],
        offset: usize,
    }

    impl Reader<'_> {
        fn take(&mut self, len: usize) -> Result<&[u8], String> {
            let end = self.offset.checked_add(len).ok_or("invalid value length")?;
            let bytes = self.input.get(self.offset..end).ok_or("truncated value transfer")?;
            self.offset = end;
            Ok(bytes)
        }

        fn length(&mut self) -> Result<usize, String> {
            let len = u32::from_le_bytes(self.take(4)?.try_into().unwrap()) as usize;

            if len > MAX_BYTES || len > self.input.len().saturating_sub(self.offset) {
                return Err("invalid value length".into());
            }

            Ok(len)
        }

        fn string(&mut self) -> Result<String, String> {
            let len = self.length()?;
            String::from_utf8(self.take(len)?.to_vec()).map_err(|_| "invalid UTF-8 value".into())
        }

        fn value(&mut self, depth: usize) -> Result<Value, String> {
            if depth > MAX_DEPTH {
                return Err("value nesting exceeds 64".into());
            }

            Ok(match self.take(1)?[0] {
                0 => Value::Null,
                1 => Value::Bool(false),
                2 => Value::Bool(true),
                3 => Value::Int(i64::from_le_bytes(self.take(8)?.try_into().unwrap())),
                4 => Value::UInt(u64::from_le_bytes(self.take(8)?.try_into().unwrap())),
                5 => {
                    let number = f64::from_le_bytes(self.take(8)?.try_into().unwrap());

                    if !number.is_finite() {
                        return Err("non-finite numbers are not supported".into());
                    }

                    Value::Float(number)
                }
                6 => Value::Str(self.string()?),
                7 => {
                    let len = self.length()?;
                    Value::Bytes(ByteArray(self.take(len)?.to_vec()))
                }
                8 => {
                    let len = self.length()?;
                    let mut values = Vec::with_capacity(len);

                    for _ in 0..len {
                        values.push(self.value(depth + 1)?);
                    }

                    Value::List(values)
                }
                9 => {
                    let len = self.length()?;
                    let mut values = std::collections::HashMap::with_capacity(len);

                    for _ in 0..len {
                        let key = self.string()?;

                        if values.insert(key, self.value(depth + 1)?).is_some() {
                            return Err("duplicate dictionary key".into());
                        }
                    }

                    Value::Dict(values)
                }
                _ => return Err("unknown value tag".into()),
            })
        }
    }

    if input.len() > MAX_BYTES {
        return Err("value transfer exceeds 64 MiB".into());
    }

    let mut reader = Reader { input, offset: 0 };
    let value = reader.value(0)?;

    if reader.offset != input.len() {
        return Err("trailing value transfer data".into());
    }

    Ok(value)
}
