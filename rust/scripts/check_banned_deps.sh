#!/bin/sh

# This script checks if the crate depends on external deps that it shouldn't. We run
# this in CI to make sure we don't accidentally reintroduce deps that would make the
# crate unusable for the CLI.
#
# While it would be more reliable to actually build the crate and check what libraries
# it links to, e.g. with otool, it is much cheaper to use cargo tree. As far as I can
# tell the entire Rust ecosystem makes use of these `x-sys` libraries to depend on
# external dynamically linked libraries.
#
# We can almost use cargo deny but it doesn't support checking specific build paths. We
# don't care if openssl-sys for example is used at build time (which it is, indirectly
# by shadow-rs), only at run time. See more here:
# https://github.com/EmbarkStudios/cargo-deny/issues/563
#
# It assumes cargo and friends are available.
#
# Run this from the rust/ directory.

cd "$(dirname "$0")"
cd ..

declare -a deps=("pq-sys" "openssl-sys")

for dep in "${deps[@]}"; do
    echo "Checking for banned dependency $dep..."

    # Check for deps. As you can see, we only check for MacOS right now.
    # We specify --no-default-features because we only care about these banned deps
    # for the local testnet use case, in which case it opts out of the default
    # features.
    out=`cargo tree --no-default-features -e features,no-build,no-dev --target aarch64-apple-darwin -p processor -i "$dep"`

    # If the exit status was non-zero, great, the dep couldn't be found.
    if [ $? -ne 0 ]; then
        continue
    fi

    # If the exit status was zero we have to check the output to see if the dep is in
    # use. If it is in the output, it is in use.
    if [[ $out != *"$dep"* ]]; then
        continue
    fi

    echo "Banned dependency $dep found!"
    exit 1
done

echo
echo "None of the banned dependencies are in use, great!"
exit 0
