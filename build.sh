#!/usr/bin/env bash

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)

if [ ! -d "$SCRIPT_DIR/build" ]; then
	mkdir "$SCRIPT_DIR/build"
fi

bear --output "$SCRIPT_DIR/compile_commands.json" -- \
	clang \
	-I "$SCRIPT_DIR/std" \
	-I "$SCRIPT_DIR/src" \
	-O0 \
	-g \
	"$SCRIPT_DIR/test/test.c" \
	-o "$SCRIPT_DIR/build/test"
