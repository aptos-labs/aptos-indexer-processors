# Aptos Indexer Protos

## Regenerating code
Install deps:
```
poetry install
```

Generate code:
```
poetry run poe generate
```

## Publishing
To publish the package, follow these steps.

First, make sure you have updated the changelog and bumped the package version if necessary.

Configure Poetry with the PyPi credentials:
```
poetry config pypi-token.pypi <token>
```

You can get the token from our credential management system, search for PyPi.

Build and publish:
```
poetry publish --build
```
