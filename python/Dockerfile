FROM python:3.11

# System deps
RUN pip install "poetry==1.4.2"

WORKDIR /app
COPY poetry.lock pyproject.toml /app/

# Copy files and folders
COPY /aptos-indexer-protos/ /app/aptos-indexer-protos
COPY /utils/ /app/utils
COPY /processors/ /app/processors

# Project initialization
RUN poetry config virtualenvs.create false \
    && poetry install --only main

CMD ["poetry", "run", "python", "-m", "processors.main", "--config", "/app/config/config.yaml"]
