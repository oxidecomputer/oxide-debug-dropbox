#!/bin/bash
#:
#: name = "build-and-test / illumos"
#: variety = "basic"
#: target = "helios"
#: rust_toolchain = "stable"
#:

set -o errexit
set -o pipefail
set -o xtrace

cargo --version
rustc --version

banner style
ptime -m cargo fmt -- --check

banner build
ptime -m cargo build --all-features --locked --all-targets --verbose

banner clippy
cargo clippy --all-targets -- --deny warnings

banner test
ptime -m cargo test --all-features --locked --verbose

banner docs
ptime -m cargo doc --no-deps --lib --bins --examples
