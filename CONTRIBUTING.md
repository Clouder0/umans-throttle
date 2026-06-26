# Contributing to umans-throttle

Thanks for your interest in contributing! This is a small, focused project —
the guidance below keeps it consistent and easy to review.

## Development setup

```bash
git clone https://github.com/Clouder0/umans-throttle.git
cd umans-throttle
cargo build
cargo test --all
```

Rust toolchain: stable. No nightly features required.

## Before opening a PR

1. **`cargo fmt --all`** — formatting must be clean.
2. **`cargo clippy --all-targets -- -D warnings`** — no warnings.
3. **`cargo test --all`** — all tests pass.

CI runs these three checks on every push/PR. Run them locally first to save a round-trip.

## Architecture notes

The core invariant is the **permit lifecycle**: a semaphore permit is acquired
before forwarding to upstream and released via RAII (`PermitBody::drop`) on
*every* path — natural stream end, client disconnect, idle timeout, upstream
error. Any change that touches the forwarding path must preserve this.

See [`docs/design.md`](docs/design.md) for the full design rationale.

## Tests

- **Integration tests** (`tests/proxy_integration.rs`): run by default, verify
  concurrency limits, queue timeouts, permit release on all paths, header
  passthrough, SSE streaming.
- **Benchmarks** (`tests/bench.rs`, `tests/bench_concurrent.rs`): `#[ignore]`d
  — run manually with `cargo test --release --test bench -- --nocapture --ignored`.

When adding a feature, add a test that would fail without it.

## Commit style

Conventional commits preferred (`feat:`, `fix:`, `docs:`, `refactor:`, `test:`,
`chore:`). Keep commits atomic — one logical change each.

## Reporting issues

Use [GitHub Issues](https://github.com/Clouder0/umans-throttle/issues). Include:
- What you expected vs. what happened
- Your config (redact any keys — the proxy stores none, but your config file is yours)
- Rust version (`rustc --version`) and OS
- Logs at `RUST_LOG=debug` if relevant

## License

By contributing, you agree your contributions are licensed under the MIT
License (see [`LICENSE`](LICENSE)).
