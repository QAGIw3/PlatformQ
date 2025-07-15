use std::os::raw::{c_char, c_void};
use std::mem;
use serde::{Deserialize, Serialize};
use serde_json;

// --- Data Structures ---
// These structs define the shape of the JSON we expect to receive
// and the JSON we will return.

#[derive(Deserialize)]
struct Asset {
    asset_id: String,
    asset_name: String,
    asset_type: String,
    source_tool: Option<String>,
}

#[derive(Serialize)]
struct ApiCall {
    method: String,
    url: String,
    body: String,
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
    // 1. Read and parse the input JSON from memory
    let input_slice = unsafe { std::slice::from_raw_parts(ptr as *mut u8, len) };
    let input_str = std::str::from_utf8(input_slice).unwrap();
    let asset: Asset = serde_json::from_str(input_str).unwrap();

    // 2. Business Logic: Construct the comment and the API call object
    // For this PoC, we'll hardcode the OpenProject URL and work package ID.
    let work_package_id = "42"; 
    let openproject_base_url = "https://openproject.platformq.dev";

    let comment_body = format!(
        "A new Digital Asset was created: '{}' (Type: {}, ID: {}). Source: {}.",
        asset.asset_name,
        asset.asset_type,
        asset.asset_id,
        asset.source_tool.unwrap_or("N/A".to_string())
    );

    let api_call = ApiCall {
        method: "POST".to_string(),
        url: format!("{}/api/v3/work_packages/{}/activities", openproject_base_url, work_package_id),
        body: serde_json::json!({ "comment": { "raw": comment_body } }).to_string(),
    };

    // 3. Serialize the output object to a JSON string
    let result_str = serde_json::to_string(&api_call).unwrap();

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