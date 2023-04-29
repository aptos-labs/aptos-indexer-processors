### run locally

```bash
docker compose up --build --force-recreate
```

## Development

### Install

```bash
poetry install
```

### linting and autoformatting

```bash
poetry run poe pyright # typecheck
poetry run poe format # autoformat via black
```
