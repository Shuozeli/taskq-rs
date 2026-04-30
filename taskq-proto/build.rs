//! Build script for `taskq-proto`.
//!
//! Compiles every `.fbs` file under `schema/` with the pure-Rust `flatc-rs`
//! pipeline:
//!   1. `flatc_rs_compiler::compile` parses, resolves includes, and runs the
//!      8-step semantic analyzer (yields a `ResolvedSchema`).
//!   2. `flatc_rs_codegen::generate_rust` emits Rust readers / builders into
//!      a single combined source file under `OUT_DIR`.
//!
//! All four schemas share `namespace taskq.v1;`, so the generated code emits
//! one `pub mod taskq { pub mod v1 { ... } }` tree containing every type.
//! `src/lib.rs` includes that file once and re-exports the inner module under
//! per-file aliases (`common`, `task_queue`, etc.) for ergonomic call sites.
//!
//! gRPC service stubs are intentionally NOT generated here. The `.fbs` files
//! contain `rpc_service` blocks that are parsed and carried in the resolved
//! schema; concrete server/client trait wiring happens in `taskq-cp` (Phase 5)
//! using `pure-grpc-rs` codegen on top of these bindings.

use std::path::PathBuf;

const SCHEMA_FILES: &[&str] = &[
    // common.fbs FIRST so its types are visible to the includers below.
    "schema/common.fbs",
    "schema/task_queue.fbs",
    "schema/task_worker.fbs",
    "schema/task_admin.fbs",
];

fn main() {
    // Re-run only when a `.fbs` file under schema/ changes.
    println!("cargo:rerun-if-changed=schema");
    for f in SCHEMA_FILES {
        println!("cargo:rerun-if-changed={f}");
    }

    let out_dir = std::env::var("OUT_DIR").expect("OUT_DIR not set by Cargo");
    let manifest_dir =
        PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR not set"));
    let schema_dir = manifest_dir.join("schema");

    let inputs: Vec<PathBuf> = SCHEMA_FILES.iter().map(|f| manifest_dir.join(f)).collect();

    let options = flatc_rs_compiler::CompilerOptions {
        include_paths: vec![schema_dir],
    };

    let result = flatc_rs_compiler::compile(&inputs, &options)
        .unwrap_or_else(|e| panic!("flatbuffers compilation failed: {e}"));

    let codegen_opts = flatc_rs_codegen::CodeGenOptions {
        // Emit owned `*T` Object API types alongside the zero-copy readers so
        // higher layers can move messages across .await boundaries without
        // wrestling with FlatBuffer lifetimes.
        gen_object_api: true,
        ..Default::default()
    };

    let code = flatc_rs_codegen::generate_rust(&result.schema, &codegen_opts)
        .unwrap_or_else(|e| panic!("flatbuffers codegen failed: {e}"));

    let out_file = PathBuf::from(&out_dir).join("taskq_v1_generated.rs");
    std::fs::write(&out_file, code)
        .unwrap_or_else(|e| panic!("failed to write {}: {e}", out_file.display()));
}
