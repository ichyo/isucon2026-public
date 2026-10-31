.PHONY: isureal clean test lint

isureal:
	cargo build --release
	install -m 0755 target/release/isureal isureal

clean:
	cargo clean
	rm -f isureal

test:
	cargo test

lint:
	cargo fmt --check
	cargo clippy --all-targets -- -D warnings
