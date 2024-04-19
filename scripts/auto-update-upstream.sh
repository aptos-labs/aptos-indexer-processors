#!/bin/bash
git config --global user.email "bot@indexer.xyz"
git config --global user.name "Bot"

git checkout -b automerge

# Add new remote named 'upstream'
git remote add upstream "https://github.com/aptos-labs/aptos-indexer-processors.git"

# Merge upstream/main with the current branch
git fetch upstream/main
git merge upstream/main

cd rust
#cargo build --locked --release -p processor

# Check if the build was successful
if [ $? -eq 0 ]; then
    cd ..
    git add .
    git commit -m "Auto-merge"
    git push -f  origin automerge
    
    gh pr create --title "Autoupdate" --body "The upstream/main was merged and built successfully." --head automerge
else
    echo "Build failed"
    exit 1
fi
