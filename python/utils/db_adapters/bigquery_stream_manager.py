"""Wrapper around BigQuery call."""
from __future__ import annotations
from typing import Any, Iterable
import logging
from google.cloud import bigquery_storage_v1beta2 as bigquery_storage
from google.cloud.bigquery_storage_v1beta2 import exceptions as bqstorage_exceptions

from google.cloud.bigquery_storage_v1beta2 import BigQueryWriteClient, types, writer
from google.protobuf import descriptor_pb2
from google.protobuf.descriptor import Descriptor


WRITE_ROWS_BATCH_SIZE = 100


class DefaultStreamManager:  # pragma: no cover
    """Manage access to the _default stream write streams."""

    def __init__(
        self,
        table_path: str,
        message_protobuf_descriptor: Descriptor,
        bigquery_storage_write_client: bigquery_storage.BigQueryWriteClient,
    ):
        # Uncomment this to run in debug mode: --log=DEBUG
        # logging.basicConfig(
        #     level=logging.DEBUG,
        #     format="%(asctime)s [%(levelname)s] %(message)s",
        #     handlers=[
        #         # logging.FileHandler("debug.log"),
        #         logging.StreamHandler()
        #     ],
        # )
        """Init."""
        self.stream_name = f"{table_path}/_default"
        self.message_protobuf_descriptor = message_protobuf_descriptor
        self.write_client = bigquery_storage_write_client
        self.append_rows_stream = None

    def _init_stream(self):
        """Init the underlying stream manager."""
        # Create a template with fields needed for the first request.
        request_template = types.AppendRowsRequest()
        # The initial request must contain the stream name.
        request_template.write_stream = self.stream_name
        # So that BigQuery knows how to parse the serialized_rows, generate a
        # protocol buffer representation of our message descriptor.
        proto_schema = types.ProtoSchema()
        proto_descriptor = descriptor_pb2.DescriptorProto()  # pylint: disable=no-member
        self.message_protobuf_descriptor.CopyToProto(proto_descriptor)
        proto_schema.proto_descriptor = proto_descriptor
        proto_data = types.AppendRowsRequest.ProtoData()
        proto_data.writer_schema = proto_schema
        request_template.proto_rows = proto_data
        # Create an AppendRowsStream using the request template created above.
        self.append_rows_stream = writer.AppendRowsStream(
            self.write_client, request_template
        )

    def send_appendrowsrequest(
        self, request: types.AppendRowsRequest
    ) -> writer.AppendRowsFuture:
        """Send request to the stream manager. Init the stream manager if needed."""
        try:
            if self.append_rows_stream is None:
                self._init_stream()
            if self.append_rows_stream is None:
                raise Exception("Error initializizing default write stream")
            else:
                return self.append_rows_stream.send(request)
        except bqstorage_exceptions.StreamClosedError:
            # the stream needs to be reinitialized
            if self.append_rows_stream is not None:
                self.append_rows_stream.close()
            self.append_rows_stream = None
            raise

    # Use as a context manager

    def __enter__(self) -> DefaultStreamManager:
        """Enter the context manager. Return the stream name."""
        self._init_stream()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """Exit the context manager : close the stream."""
        if self.append_rows_stream is not None:
            # Shutdown background threads and close the streaming connection.
            self.append_rows_stream.close()


class BigqueryWriteManager:
    """Encapsulation for bigquery client."""

    def __init__(
        self,
        project_id: str,
        dataset_id: str,
        table_id: str,
        pb2_descriptor: Descriptor,
    ):  # pragma: no cover
        """Create a BigQueryManager."""
        self.bigquery_storage_write_client = BigQueryWriteClient()

        self.table_path = self.bigquery_storage_write_client.table_path(
            project_id, dataset_id, table_id
        )
        self.pb2_descriptor = pb2_descriptor
        self.proto_rows = types.ProtoRows()

    def batch_rows(self, pb_rows: Iterable[Any]) -> None:
        # Create a batch of row data by appending proto2 serialized bytes to the
        # serialized_rows repeated field.
        for row in pb_rows:
            self.proto_rows.serialized_rows.append(row.SerializeToString())

        if len(self.proto_rows.serialized_rows) > WRITE_ROWS_BATCH_SIZE:
            self.write_rows()
            # Reset proto_rows after write
            self.proto_rows = types.ProtoRows()

    def write_rows(self) -> None:
        """Write data rows."""
        with DefaultStreamManager(
            self.table_path, self.pb2_descriptor, self.bigquery_storage_write_client
        ) as target_stream_manager:
            # Create an append row request containing the rows
            request = types.AppendRowsRequest()
            proto_data = types.AppendRowsRequest.ProtoData()
            proto_data.rows = self.proto_rows
            request.proto_rows = proto_data

            future = target_stream_manager.send_appendrowsrequest(request)

            # Wait for the append row requests to finish.
            future.result()
