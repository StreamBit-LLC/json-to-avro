import functools
from typing import Callable, Optional

from schema_registry.client import AsyncSchemaRegistryClient, SchemaRegistryClient
from schema_registry.client.utils import SchemaVersion

from json_to_avro.avro_schema.avro_schema_candidate import AvroSchemaCandidate
from json_to_avro.avro_schema.registered_avro_schema import RegisteredAvroSchema, RegisteredAvroSchemaId
from loguru import logger


def ensure_backwards_transitive_compatibility(func: Callable) -> Callable:
    @functools.wraps(func)
    def wraps(*args):
        self, subject, *_ = args
        if subject not in self.current_schema_table:
            logger.debug("Setting compatibility to BACKWARD_TRANSITIVE for %s" % subject)
            self.schema_registry_client.update_compatibility("BACKWARD_TRANSITIVE", subject)
        return func(*args)
    return wraps


class SchemaProvider:
    def __init__(
        self,
        current_schema_table: dict[str, RegisteredAvroSchema],
        schema_registry_client: SchemaRegistryClient,
    ):
        self.current_schema_table = current_schema_table
        self.schema_registry_client = schema_registry_client

    @classmethod
    def from_schema_registry_client(cls, client: SchemaRegistryClient | AsyncSchemaRegistryClient):
        current_schema_table: dict[str, RegisteredAvroSchema] = {}
        return cls(current_schema_table, client)

    def __getitem__(self, subject: str) -> RegisteredAvroSchema:
        """Unsafe version of `get`
        :raises KeyError
        """
        return self.current_schema_table[subject]

    @ensure_backwards_transitive_compatibility
    def get(self, subject_name: str) -> RegisteredAvroSchema | None:
        if subject_name in self.current_schema_table:
            return self.current_schema_table[subject_name]

        schema_version: SchemaVersion | None = self.schema_registry_client.get_schema(
            subject_name, version="latest"
        )
        if schema_version:
            self.current_schema_table[
                subject_name
            ] = RegisteredAvroSchema.from_schema_version(schema_version)
            return self.current_schema_table[subject_name]

        return None

    def _set(
        self, subject_name: str, schema: RegisteredAvroSchema
    ) -> RegisteredAvroSchemaId:
        self.current_schema_table[subject_name] = schema
        return schema.schema_id

    def register_and_set(
        self, subject_name: str, candidate: AvroSchemaCandidate
    ) -> RegisteredAvroSchemaId:
        schema_id: int = self.schema_registry_client.register(
            subject_name, candidate.schema.schema_str
        )
        registered_schema = RegisteredAvroSchema(
            candidate.schema, RegisteredAvroSchemaId(schema_id)
        )

        return self._set(subject_name, registered_schema)
