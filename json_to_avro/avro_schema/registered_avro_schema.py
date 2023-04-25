import dataclasses
from schema_registry.client.utils import SchemaVersion
from typing import NewType

from .mergeable_avro_schema import MergeableAvroSchema

RegisteredAvroSchemaId = NewType("RegisteredAvroSchemaId", int)


@dataclasses.dataclass
class RegisteredAvroSchema:
    schema: MergeableAvroSchema
    schema_id: RegisteredAvroSchemaId

    @classmethod
    def from_schema_version(
        cls, schema_version: SchemaVersion
    ) -> "RegisteredAvroSchema":
        return cls(
            MergeableAvroSchema(schema_version.schema.flat_schema),
            RegisteredAvroSchemaId(schema_version.schema_id),
        )
