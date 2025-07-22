from wasmtime import Engine, Store

# --- Wasmtime Engine Setup ---
# These are created once and shared across all requests for performance.
wasm_engine = Engine()
wasm_store = Store(wasm_engine)
# --- 