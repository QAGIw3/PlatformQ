use std::os::raw::{c_char, c_void};
use std::mem;
use serde::Serialize;
use serde_json;

// --- Data Structures ---
#[derive(Serialize)]
struct AnalysisResult {
    status: String,
    summary: String,
}

// --- Memory Allocation (same as before) ---
#[no_mangle]
pub extern "C" fn allocate(size: usize) -> *mut c_void {
    let mut buffer = Vec::with_capacity(size);
    let ptr = buffer.as_mut_ptr();
    mem::forget(buffer);
    ptr
}

// --- WASM ABI Wrapper ---
#[no_mangle]
pub extern "C" fn run(ptr: *mut c_char, len: usize) -> *mut c_char {
    // 1. Read the input log string from memory
    let log_content_slice = unsafe { std::slice::from_raw_parts(ptr as *mut u8, len) };
    let log_content = std::str::from_utf8(log_content_slice).unwrap_or("");

    // 2. Business Logic: Analyze the log content
    let result = if log_content.to_lowercase().contains("error") || log_content.to_lowercase().contains("failure") {
        AnalysisResult {
            status: "FAILURE".to_string(),
            summary: "Failure detected in simulation log.".to_string(),
        }
    } else {
        AnalysisResult {
            status: "SUCCESS".to_string(),
            summary: "Simulation completed successfully.".to_string(),
        }
    };

    // 3. Serialize the output object to a JSON string
    let result_str = serde_json::to_string(&result).unwrap();

    // 4. Allocate new memory and write the result string to it
    let result_bytes = result_str.as_bytes();
    let result_ptr = allocate(result_bytes.len() + 1) as *mut u8;
    
    unsafe {
        std::ptr::copy_nonoverlapping(result_bytes.as_ptr(), result_ptr, result_bytes.len());
        *result_ptr.add(result_bytes.len()) = 0; // Null terminator
    }
    
    result_ptr as *mut c_char
}

fn main() {} 