//! Build script for `taskq-proto`.
//!
//! Drives FlatBuffers + Rust codegen through `grpc_build::compile_fbs`. The
//! pipeline parses every `.fbs` file under `schema/`, runs the
//! `flatbuffers-rs` semantic analyzer, and emits Rust readers / builders /
//! Object API types into a single output file under `OUT_DIR`.
//!
//! ## Output filename
//!
//! Per `compile_fbs`'s contract the output filename is derived from the FIRST
//! input file's stem. We pass `common.fbs` first so the file lands as
//! `common_generated.rs`. All four schemas share `namespace taskq.v1;`, so
//! every generated type ends up in one combined `pub mod taskq { pub mod v1 {
//! ... } }` tree -- the per-file split lives at the schema source layer
//! (where it bounds reviewer blast radius), not the Rust module tree.
//!
//! ## gRPC service stubs (deferred to Phase 5b)
//!
//! `compile_fbs` itself does NOT emit gRPC service stubs at the pinned
//! `pure-grpc-rs` revision (`120046b9...`). The path that would do so --
//! activating `flatc-rs-codegen/grpc` -- fails to compile against this pin
//! because of the upstream `2db926d` "use codegen-infra for schema
//! representation" refactor that moved `service_from_fbs` out of
//! `grpc-codegen::flatbuffers`. See the Phase 5b-pre report for the full
//! analysis.
//!
//! Driving service codegen directly via
//! `grpc_codegen::flatbuffers::generate_service_tokens` *does* work and emits
//! syntactically-correct trait stubs, but the emitted code references the
//! zero-copy `SubmitTaskRequest<'a>` types as if they were owned, and the
//! generated `tower::Service` adapter requires a `FlatBuffersCodec`
//! parameterised on owned types that impl `FlatBufferGrpcMessage`. Owned
//! wrappers + their codec impls are a Phase 5b deliverable (mirroring the
//! `pure-grpc-rs/examples/greeter-fbs` pattern), so emitting the stubs here
//! would only produce a non-compiling output. We hold off until Phase 5b
//! lands the owned-wrapper layer alongside the service-trait `impl` blocks.

use std::path::PathBuf;

const SCHEMA_FILES: &[&str] = &[
    // common.fbs FIRST so its types are visible to the includers below AND so
    // the generated output filename is `common_generated.rs`.
    "schema/common.fbs",
    "schema/task_queue.fbs",
    "schema/task_worker.fbs",
    "schema/task_admin.fbs",
];

fn main() {
    println!("cargo:rerun-if-changed=schema");
    for f in SCHEMA_FILES {
        println!("cargo:rerun-if-changed={f}");
    }

    let manifest_dir =
        PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR not set"));
    let schema_dir = manifest_dir.join("schema");

    let inputs: Vec<PathBuf> = SCHEMA_FILES.iter().map(|f| manifest_dir.join(f)).collect();
    let includes: Vec<PathBuf> = vec![schema_dir];

    grpc_build::compile_fbs(&inputs, &includes)
        .unwrap_or_else(|e| panic!("FlatBuffers + Rust codegen failed: {e}"));
}
