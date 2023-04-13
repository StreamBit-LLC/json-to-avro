import dataclasses

from loguru import logger
from schema_registry.client import SchemaRegistryClient
from schema_registry.serializers import AvroMessageSerializer

from json_to_avro.avro_schema import RegisteredAvroSchemaId, AvroSchemaCandidate
from json_to_avro.schema_provider import SchemaProvider
from .avroable_data import AvroableData


@dataclasses.dataclass
class JsonToAvroConfig:
    schema_registry_url: str


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


