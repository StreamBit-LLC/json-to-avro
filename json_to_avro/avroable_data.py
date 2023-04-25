import dataclasses
from typing import TypeAlias

Json: TypeAlias = str | float | int | bool | dict[str, "Json"] | list["Json"] | None


@dataclasses.dataclass
class AvroableData:
    record_name: str
    subject_name: str
    data: Json
