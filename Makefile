# SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-rs-nexus Authors
# SPDX-License-Identifier: Apache-2.0

.PHONY: default test lint doc package publish-dry-run clean help

default: test lint doc

test:
	cargo test

lint:
	cargo fmt --check
	cargo clippy --all-targets -- -D warnings

# Rustdoc warnings fail here rather than on docs.rs after publish. Missing
# docs are already a compile error (#![deny(missing_docs)] in lib.rs).
doc:
	RUSTDOCFLAGS="-D warnings" cargo doc --no-deps

# `package` allows a dirty tree (a mid-development listing aid)
package:
	cargo package --list --allow-dirty

publish-dry-run:
	cargo publish --dry-run

clean:
	cargo clean

help:
	@echo "make                 - test + lint + doc (the pre-push default)"
	@echo "make test            - unit tests + doctests"
	@echo "make lint            - rustfmt check + clippy (warnings denied)"
	@echo "make doc             - render API docs (rustdoc warnings denied)"
	@echo "make package         - list the files cargo would publish"
	@echo "make publish-dry-run - full publish verification, no upload"
	@echo "make clean           - remove target/"
