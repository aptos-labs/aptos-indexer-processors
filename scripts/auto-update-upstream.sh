#!/bin/bash
current_date=$(date +'%Y-%m-%d')
git checkout -b "$current_date"

# Add new remote named 'upstream'
git remote add upstream "https://github.com/aptos-labs/aptos-indexer-processors.git"

# Merge upstream/main with the current branch
git fetch upstream
git merge upstream/main

cd rust
cargo build --locked --release -p processor

# Check if the build was successful
if [ $? -eq 0 ]; then
    echo "Can be merged"
    cd ..
    git add .
    git commit -m "Auto-merge $current_date"
    git config --global user.email "bot@indexer.xyz"
    git config --global user.name "Botß"
    git push origin "$current_date"
else
    echo "Build failed"
fi
