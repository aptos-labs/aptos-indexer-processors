FROM python:3.11

# System deps
RUN pip install "poetry==1.4.2"

WORKDIR /app
COPY poetry.lock pyproject.toml /app/


# Project initialization
RUN poetry config virtualenvs.create false \
    && poetry install --only main

# Copy files and folders
COPY *.py /app/
COPY /aptos /app/aptos
COPY /processors/examples/* /app/

CMD ["poetry", "run", "python", "/processors/examples/example_processor.py", "--config", "/app/config/config.yaml"]
