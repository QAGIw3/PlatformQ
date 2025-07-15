use std::os::raw::{c_char, c_void};
use std::mem;

// --- Memory Allocation ---
// This function is called by the host to allocate memory in the WASM module.
#[no_mangle]
pub extern "C" fn allocate(size: usize) -> *mut c_void {
    let mut buffer = Vec::with_capacity(size);
    let ptr = buffer.as_mut_ptr();
    // "Forget" the buffer so its memory isn't reclaimed by Rust's memory manager.
    // The host is now responsible for this memory.
    mem::forget(buffer);
    ptr
}

// --- String Processing Logic ---
// This is the core logic of our function. It's a standard Rust function.
fn process_string(input: &str) -> String {
    format!("Hello, {}! This message is from inside the WASM module.", input)
}

// --- WASM ABI Wrapper ---
// This is the function that the host will call, adhering to our ABI.
#[no_mangle]
pub extern "C" fn run(ptr: *mut c_char, len: usize) -> *mut c_char {
    // 1. Read the input string from memory
    let input_slice = unsafe { std::slice::from_raw_parts(ptr as *mut u8, len) };
    let input_str = std::str::from_utf8(input_slice).unwrap_or("invalid utf-8");

    // 2. Process the string
    let result_str = process_string(input_str);

    // 3. Allocate new memory and write the result string to it
    let result_bytes = result_str.as_bytes();
    let result_ptr = allocate(result_bytes.len() + 1) as *mut u8; // +1 for null terminator
    
    unsafe {
        std::ptr::copy_nonoverlapping(result_bytes.as_ptr(), result_ptr, result_bytes.len());
        // Add the null terminator for C-style string compatibility
        *result_ptr.add(result_bytes.len()) = 0;
    }
    
    result_ptr as *mut c_char
}

// Dummy main function, not used when compiling to wasm32-unknown-unknown
fn main() {} 