FROM python:3.11

# System deps
RUN pip install "poetry==1.4.2"

WORKDIR /app
COPY poetry.lock pyproject.toml /app/

# Project initialization
RUN poetry config virtualenvs.create false \
    && poetry install --only main

# Copy files and folders
COPY /utils/ /app/utils
COPY /processors/ /app/processors

CMD ["poetry", "run", "python", "-m", "processors.main", "--config", "/app/config/config.yaml"]
