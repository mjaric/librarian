//! xtask — cargo-native build orchestration (no Makefile, no external
//! task runner). The one job cargo itself can't express: building the
//! wasm UI bundle and wiring it into `librarian-web`'s static dir.
//!
//!     cargo run -p xtask -- dist            # release wasm + wasm-bindgen
//!     cargo run -p xtask -- dist --debug    # dev build (faster)
//!
//! Prereqs (one-time): `rustup target add wasm32-unknown-unknown` and
//! `cargo install wasm-bindgen-cli --version <wasm-bindgen crate version>`.

use std::path::{Path, PathBuf};
use std::process::Command;

fn main() {
    let debug = std::env::args().any(|a| a == "--debug");
    if let Err(e) = dist(debug) {
        eprintln!("xtask: {e:#}");
        std::process::exit(1);
    }
}

fn repo_root() -> PathBuf {
    // xtask runs via `cargo run -p xtask` from anywhere in the workspace;
    // CARGO_MANIFEST_DIR points at crates/xtask regardless.
    Path::new(&std::env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("crates/xtask layout")
        .to_path_buf()
}

fn run(cmd: &mut Command) -> Result<(), String> {
    println!("xtask: {:?}", cmd);
    let status = cmd
        .status()
        .map_err(|e| format!("spawning {:?}: {e}", cmd.get_program()))?;
    if !status.success() {
        return Err(format!("{:?} exited with {status}", cmd.get_program()));
    }
    Ok(())
}

fn dist(debug: bool) -> Result<(), String> {
    let root = repo_root();
    let profile = if debug { "debug" } else { "release" };
    let out_dir = root.join("crates/librarian-web/static/pkg");

    run(Command::new(env_var_or("CARGO", "cargo"))
        .arg("build")
        .arg("-p")
        .arg("bookshelf-ui")
        .arg("--target")
        .arg("wasm32-unknown-unknown")
        .args(if debug {
            Vec::<&str>::new()
        } else {
            vec!["--release"]
        })
        .current_dir(&root))?;

    let wasm = root.join(format!(
        "target/wasm32-unknown-unknown/{profile}/bookshelf_ui.wasm"
    ));
    if !wasm.is_file() {
        return Err(format!("expected wasm artifact at {}", wasm.display()));
    }

    // wasm-bindgen-cli version must equal the wasm-bindgen crate version
    // pinned in bookshelf-ui's Cargo.toml.
    let mut bindgen = Command::new("wasm-bindgen");
    bindgen
        .arg("--target")
        .arg("web")
        .arg("--no-typescript")
        .arg("--out-name")
        .arg("bookshelf_ui")
        .arg("--out-dir")
        .arg(&out_dir)
        .arg(&wasm);
    run(&mut bindgen)?;

    println!("xtask: dist written to {}", out_dir.display());
    println!("xtask: next: cargo run -p librarian-web -- --config <toml>");
    Ok(())
}

fn env_var_or(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}
