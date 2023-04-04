import dataclasses
from loguru import logger
from schema_registry.client import AsyncSchemaRegistryClient, SchemaRegistryClient
from schema_registry.serializers import AvroMessageSerializer, AsyncAvroMessageSerializer
from typing import (
    TypeAlias,
)
from enum import Enum

from .avro_schema import RegisteredAvroSchemaId, AvroSchemaCandidate, RegisteredAvroSchema
from .schema_provider import SchemaProvider
from .json_to_avro.avroable_data import AvroableData
# class OperationMode(Enum):
#     Sync = "sync"
#     Async = "async"

@dataclasses.dataclass
class JsonToAvroConfig:
    # operation_mode: OperationMode
    schema_registry_url: str


# class AsyncJsonToAvro:
#     schema_registry_client: AsyncSchemaRegistryClient
#     serializer: AsyncAvroMessageSerializer
#
#     @classmethod
#     def from_config(cls, config: JsonToAvroConfig):
#         schema_registry = AsyncSchemaRegistryClient(url=config.schema_registry_url)
#         return cls(schema_registry, AsyncAvroMessageSerializer(schema_registry))
#
#     async def serialize_as_avro(self, msg: AvroableData):
#         raise NotImplementedError("This is not yet supported. Hopefully soon!")





class JsonToAvro:
    def __init__(self, provider: SchemaProvider, serializer: AvroMessageSerializer):
        self.provider = provider
        self.serializer = serializer

    @classmethod
    def from_config(cls, config: JsonToAvroConfig):
        schema_registry = SchemaRegistryClient(url=config.schema_registry_url)
        return cls(SchemaProvider({}, schema_registry), AvroMessageSerializer(schema_registry))

    def serialize_as_avro(
        self,
        msg: AvroableData,
    ) -> bytes:
        schema_candidate = AvroSchemaCandidate.from_avroable_data(msg)
        maybe_existing_registered_schema = self.provider.get(msg.subject_name)
        logger.debug("Existing Schema: %s" % maybe_existing_registered_schema)
        schema_id: RegisteredAvroSchemaId = (
            maybe_existing_registered_schema.schema_id
            if maybe_existing_registered_schema is not None
            else self.provider.register_and_set(msg.subject_name, schema_candidate)
        )
        logger.debug("Registered Schema: %s", self.provider[msg.subject_name])

        try:
            return self.serializer.encode_record_with_schema_id(schema_id, msg.data)
        except (ValueError, KeyError, TypeError, AttributeError) as e:
            logger.debug(f"Schema mismatch: {e}")
            logger.debug(
                "existing schema: %s" % maybe_existing_registered_schema.schema
                if maybe_existing_registered_schema is not None
                else None,
            )
            logger.debug("Message data: %s" % msg.data)
            new_schema_candidate = AvroSchemaCandidate(
                self.provider[msg.subject_name].schema + schema_candidate.schema
            )
            logger.debug("New Schema Candidate: %s" % new_schema_candidate)
            schema_id = self.provider.register_and_set(msg.subject_name, new_schema_candidate)

            return self.serializer.encode_record_with_schema_id(schema_id, msg.data)


