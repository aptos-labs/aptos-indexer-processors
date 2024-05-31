## Purpose

Sometimes we might need to create move transactions that simulate edge cases. For example, we noticed that mint + burn the same token in a single transaction creates problems, and to repro we'd have to submit a blockchain transaction. This is an example of how to create a script.

Checkout how scripts work in: https://stackoverflow.com/questions/74627977/how-do-i-execute-a-move-script-with-the-aptos-cli.

This script attempts to get the signer of the resource account and deploy code to the resource account from the admin account.

## How to run this code?

`aptos move compile && aptos move publish --named-addresses test_addr=default && aptos move run-script --compiled-script-path build/run_script/bytecode_scripts/main.mv --profile blah`

- If you haven't created a profile before run `aptos init --profile blah`

## Notes

- This is meant to be a template. You should build your own script.
- In `Move.toml`, you can change the revision for the frameworks to test new code, from `main` for example.
