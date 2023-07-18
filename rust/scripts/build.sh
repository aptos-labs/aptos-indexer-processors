#!/bin/sh

cargo build --locked --release -p processor
cp target/release/processor /usr/local/bin