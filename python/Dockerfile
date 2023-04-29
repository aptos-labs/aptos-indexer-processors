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

CMD ["poetry", "run", "python", "processor.py", "--config", "/app/config/config.yaml"]
