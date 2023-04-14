FROM python:3.9

ADD ./config.yaml configs/config.yaml

COPY python/*.py .
COPY python/aptos ./aptos

RUN pip install grpcio grpcio-tools psycopg2 pyyaml sqlalchemy
CMD ["python", "grpc_client.py", "--config", "configs/config.yaml"]