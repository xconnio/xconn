.PHONY: build
build:
	cargo build --locked -p xconn --features router --bin xconn
