build:
	cargo buildserver:

server:
	cargo run --bin server > run_server.log 2>&1

client:
	cargo run --bin cmd > run_client.log 2>&1

format:
	cargo fmt --all

# requires clippy linter
# $ rustup component add clippy
lint:
	cargo clippy --all

test:
	cargo test
