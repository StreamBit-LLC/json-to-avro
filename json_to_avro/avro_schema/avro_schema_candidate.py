import dataclasses

from .mergeable_avro_schema import MergeableAvroSchema
from json_to_avro.json_to_avro.avroable_data import AvroableData


@dataclasses.dataclass
class AvroSchemaCandidate:
    schema: MergeableAvroSchema

    @classmethod
    def from_avroable_data(cls, msg: AvroableData) -> "AvroSchemaCandidate":
        return cls(MergeableAvroSchema.from_avroable_data(msg))