FROM python:3.11

# System deps
RUN pip install "poetry==1.4.2"

WORKDIR /app
COPY python/poetry.lock python/pyproject.toml /app/


# Project initialization
RUN poetry config virtualenvs.create false \
    && poetry install --only main

# Copy files and folders
COPY ./config.yaml /app/configs/config.yaml
COPY python/*.py /app/
COPY python/aptos /app/aptos

CMD ["poetry", "run", "python", "grpc_client.py", "--config", "configs/config.yaml"]