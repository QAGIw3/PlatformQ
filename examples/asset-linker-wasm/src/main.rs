use std::os::raw::{c_char, c_void};
use std::mem;
use serde::{Deserialize, Serialize};
use serde_json;

// --- Data Structures ---
// These structs define the shape of the JSON data our WASM module will
// communicate with. `serde` handles the serialization and deserialization
// automatically, which is much safer and more robust than manual parsing.

/// This struct represents the expected input from the host.
/// It's a simplified version of our `DigitalAsset` model.
#[derive(Deserialize)]
struct Asset {
    asset_id: String,
    asset_name: String,
    asset_type: String,
    source_tool: Option<String>,
}

/// This struct represents the output we want to give back to the host.
/// It's a structured command telling the host which API call to make.
#[derive(Serialize)]
struct ApiCall {
    method: String,
    url: String,
    body: String,
}

// --- Memory Allocation (WASM ABI) ---
// This function is part of our custom Application Binary Interface (ABI).
// The host (our Rust/Python service) cannot directly access the WASM module's
// memory. To pass data, the host must call this function to request a
// chunk of memory of a certain size. This function returns a raw pointer
// to the allocated block, which the host can then write into.
#[no_mangle]
pub extern "C" fn allocate(size: usize) -> *mut c_void {
    let mut buffer = Vec::with_capacity(size);
    let ptr = buffer.as_mut_ptr();
    // `mem::forget` is crucial. It tells Rust's memory manager to "forget"
    // about this buffer, preventing it from being deallocated when `allocate`
    // returns. The ownership of this memory is effectively transferred to the
    // host. The host is responsible for managing this memory via the WASM runtime.
    mem::forget(buffer);
    ptr
}

// --- Main Handler (WASM ABI) ---
// This is the primary entrypoint that our host service will call.
// It follows the ABI: it takes a pointer and a length, and returns a pointer.
#[no_mangle]
pub extern "C" fn run(ptr: *mut c_char, len: usize) -> *mut c_char {
    // 1. Read and parse the input JSON from memory.
    // We reconstruct a Rust slice from the raw pointer and length provided by the host.
    let input_slice = unsafe { std::slice::from_raw_parts(ptr as *mut u8, len) };
    // We then parse this slice as a UTF-8 string, and then into our `Asset` struct.
    let input_str = std::str::from_utf8(input_slice).unwrap();
    let asset: Asset = serde_json::from_str(input_str).unwrap();

    // 2. Business Logic: This is the core purpose of the WASM module.
    // It takes the input data and transforms it into the desired output.
    // For this PoC, we'll hardcode the OpenProject URL and work package ID.
    let work_package_id = "42"; 
    let openproject_base_url = "https://openproject.platformq.dev";

    // Construct the comment that will be posted to OpenProject.
    let comment_body = format!(
        "A new Digital Asset was created: '{}' (Type: {}, ID: {}). Source: {}.",
        asset.asset_name,
        asset.asset_type,
        asset.asset_id,
        asset.source_tool.unwrap_or_else(|| "N/A".to_string())
    );

    // Construct the structured API call object that the host will execute.
    let api_call = ApiCall {
        method: "POST".to_string(),
        url: format!("{}/api/v3/work_packages/{}/activities", openproject_base_url, work_package_id),
        body: serde_json::json!({ "comment": { "raw": comment_body } }).to_string(),
    };

    // 3. Serialize the output object back into a JSON string.
    let result_str = serde_json::to_string(&api_call).unwrap();

    // 4. Allocate new memory and write the result string back to it.
    // This uses the same 'allocate' function defined above. The host will receive
    // a pointer to this new block of memory.
    let result_bytes = result_str.as_bytes();
    let result_ptr = allocate(result_bytes.len() + 1) as *mut u8; // +1 for null terminator
    
    unsafe {
        std::ptr::copy_nonoverlapping(result_bytes.as_ptr(), result_ptr, result_bytes.len());
        // We add a C-style null terminator to the end of the string. This is a
        // common convention that makes it easy for the host to know where the
        // string ends when reading from the pointer.
        *result_ptr.add(result_bytes.len()) = 0;
    }
    
    // Return the pointer to the result string.
    result_ptr as *mut c_char
}

// This main function is a no-op. It's required by the Rust compiler for binary
// crates, but it is not used when compiling to the `wasm32-unknown-unknown` target.
fn main() {} 