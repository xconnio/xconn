//! Owned-buffer C ABI. Never stores pointers into foreign memory.
use std::sync::{Mutex, OnceLock, mpsc};
use xconn_binding_runtime as bridge;
use xconn_binding_runtime::protocol::values;

fn completions() -> &'static (mpsc::Sender<u64>, Mutex<mpsc::Receiver<u64>>) {
    static QUEUE: OnceLock<(mpsc::Sender<u64>, Mutex<mpsc::Receiver<u64>>)> = OnceLock::new();
    QUEUE.get_or_init(|| {
        let (tx, rx) = mpsc::channel();
        (tx, Mutex::new(rx))
    })
}

fn completed(handle: u64) {
    let _ = completions().0.send(handle);
}

#[repr(C)]
pub struct XcResult {
    pub status: u32,
    pub data: *mut u8,
    pub len: usize,
}

fn output(result: Result<Vec<u8>, String>) -> *mut XcResult {
    let (status, bytes) = match result {
        Ok(bytes) => (0, bytes),
        Err(error) => (1, error.into_bytes()),
    };
    let len = bytes.len();
    let data = Box::into_raw(bytes.into_boxed_slice()).cast::<u8>();
    Box::into_raw(Box::new(XcResult { status, data, len }))
}

#[unsafe(no_mangle)]
pub extern "C" fn xc_abi_version() -> u32 {
    1
}

/// # Safety
/// Input must point to len readable bytes for the duration of this call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn xc_process(operation: u32, handle: u64, input: *const u8, len: usize) -> *mut XcResult {
    let result = std::panic::catch_unwind(|| {
        if len > values::MAX_BYTES || (input.is_null() && len != 0) {
            return Err("invalid input buffer".into());
        }

        let bytes = if len == 0 {
            &[]
        } else {
            unsafe { std::slice::from_raw_parts(input, len) }
        };
        let value = match operation {
            1 | 7 => bridge::Value::UInt(bridge::submit_with_completion(
                values::decode(bytes)?,
                (operation == 1).then_some(completed as fn(u64)),
            )),
            2 => {
                bridge::runtime().block_on(bridge::ready(handle))?;
                bridge::Value::Null
            }
            3 => bridge::take(handle)?,
            4 => {
                bridge::cancel(handle);
                bridge::Value::Null
            }
            5 => bridge::execute(values::decode(bytes)?)?,
            6 => bridge::Value::UInt(completions().1.lock().unwrap().recv().map_err(|e| e.to_string())?),
            _ => return Err("unsupported ABI operation".into()),
        };
        values::encode(&value)
    })
    .unwrap_or_else(|_| Err("native operation panicked".into()));
    output(result)
}

/// Post an opaque host-owned notification when a job becomes ready.
///
/// The post function is process-scoped (for example Dart's PostCObject), not an
/// isolate callback. A closed host port returns false and cancels the job.
/// # Safety
/// `message` must remain valid until `release` is called. `post` must copy any
/// data it needs before returning. Both function pointers must remain callable
/// for the process lifetime. Ownership of `message` transfers to this function.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn xc_notify(
    handle: u64,
    port: i64,
    message: *mut std::ffi::c_void,
    post: unsafe extern "C" fn(i64, *mut std::ffi::c_void) -> i8,
    release: unsafe extern "C" fn(*mut std::ffi::c_void),
) {
    struct Notification {
        message: usize,
        release: unsafe extern "C" fn(*mut std::ffi::c_void),
    }

    impl Drop for Notification {
        fn drop(&mut self) {
            unsafe { (self.release)(self.message as *mut std::ffi::c_void) };
        }
    }

    let notification = Notification {
        message: message as usize,
        release,
    };
    bridge::runtime().spawn(async move {
        let _ = bridge::ready(handle).await;
        let posted = unsafe { post(port, notification.message as *mut std::ffi::c_void) };

        if posted == 0 {
            bridge::cancel(handle);
        }

        drop(notification);
    });
}

/// # Safety
/// Pointer must come from xc_process and be released exactly once.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn xc_result_free(result: *mut XcResult) {
    if !result.is_null() {
        unsafe {
            let result = Box::from_raw(result);
            drop(Box::from_raw(std::ptr::slice_from_raw_parts_mut(
                result.data,
                result.len,
            )));
        }
    }
}

#[cfg(test)]
mod notification_tests {
    use super::*;
    use std::{ffi::c_void, time::Duration};

    struct Message {
        events: mpsc::Sender<&'static str>,
    }

    unsafe extern "C" fn closed_port(_: i64, message: *mut c_void) -> i8 {
        let message = unsafe { &*message.cast::<Message>() };
        message.events.send("posted").unwrap();
        0
    }

    unsafe extern "C" fn release(message: *mut c_void) {
        let message = unsafe { Box::from_raw(message.cast::<Message>()) };
        message.events.send("released").unwrap();
    }

    #[test]
    fn empty_owned_results_and_null_results_can_be_freed() {
        for bytes in [Ok(Vec::new()), Err(String::new())] {
            let result = output(bytes);
            assert!(!result.is_null());
            assert_eq!(unsafe { (*result).len }, 0);
            // Even an empty boxed slice has a non-null, aligned allocation pointer.
            assert!(!unsafe { (*result).data }.is_null());
            unsafe { xc_result_free(result) };
        }
        unsafe { xc_result_free(std::ptr::null_mut()) };
    }

    #[test]
    fn closed_host_port_cancels_unclaimed_job_and_releases_message() {
        let input = values::encode(&bridge::Value::Dict(std::collections::HashMap::from([(
            "method".into(),
            bridge::Value::Str("router_new".into()),
        )])))
        .unwrap();
        let result = unsafe { xc_process(7, 0, input.as_ptr(), input.len()) };
        assert_eq!(unsafe { (*result).status }, 0);
        let value = values::decode(unsafe { std::slice::from_raw_parts((*result).data, (*result).len) }).unwrap();
        unsafe { xc_result_free(result) };
        let bridge::Value::UInt(handle) = value else {
            panic!("expected job ID")
        };

        let (events, received) = mpsc::channel();
        let message = Box::into_raw(Box::new(Message { events })).cast();
        unsafe { xc_notify(handle, 1, message, closed_port, release) };
        assert_eq!(received.recv_timeout(Duration::from_secs(5)).unwrap(), "posted");
        assert_eq!(received.recv_timeout(Duration::from_secs(5)).unwrap(), "released");
        assert!(bridge::take(handle).is_err());
        assert!(received.try_recv().is_err());
        assert!(completions().1.lock().unwrap().try_recv().is_err());
    }
}
