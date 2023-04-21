import dataclasses
import json
from typing import (
    Any,
)

from json_to_avro.avroable_data import AvroableData

PYTHON_TO_AVRO_TYPES = {
    str: "string",
    int: "long",
    float: "double",
    bool: "boolean",
    list: "array",
    dict: "record",
}

@dataclasses.dataclass
class MergeableAvroSchema:
    schema_dict: dict

    def __add__(self, other: "MergeableAvroSchema") -> "MergeableAvroSchema":
        return self.merge_schemas(self.schema_dict, other.schema_dict)

    @classmethod
    def merge_schemas(
        cls, self_data: dict, other_data: dict
    ) -> "MergeableAvroSchema":
        return cls(cls._merge_schemas(self_data, other_data))

    @classmethod
    def _merge_schemas(
        cls,
        self_data: Any,
        other_data: Any,
    ) -> Any:
        combined_fields: list[Any] = []
        for fst, snd in list(
            zip(
                sorted(self_data["fields"], key=lambda r: r["name"]),
                sorted(other_data["fields"], key=lambda r: r["name"]),
            )
        ):
            combined_fields = [*combined_fields, fst, snd]

        existing_schema_fields = set(field["name"] for field in self_data["fields"])
        updated_fields: dict[Any, Any] = {}

        for field in combined_fields:
            match updated_fields.get(field["name"]), field:
                # if updated field and the new are the same, overwrite
                case (existing, new) if existing == new:
                    updated_fields[field["name"]] = field
                # if the field already exists in the schema, add it to the update set
                case (None, {"name": name}) if name in existing_schema_fields:
                    updated_fields[name] = field
                # if the field is not in the update set AND IT IS NOT in the existing registered schema,
                # create a union of null and the new type, so that backwards compatibility is maintained
                case (
                    None,
                    {
                        "name": name,
                        "type": (
                            "string" | "boolean" | "long" | "double" | dict(_)
                        ) as t,
                    },
                ) if name not in existing_schema_fields:
                    updated_fields[name] = {
                        "name": name,
                        "type": ["null", t],
                        "default": None,
                    }
                # if the existing field is a null type, and the new field is not null and not a union,
                # create a union of null and the type, with null as the default
                case (
                    {"name": name, "type": "null"},
                    {
                        "name": _,
                        "type": (
                            "string" | "boolean" | "long" | "double" | dict(_)
                        ) as t,
                    },
                ):
                    updated_fields[name] = {
                        **updated_fields[name],
                        "type": ["null", t],
                        "default": None,
                    }
                # if the existing field is not a null type and is not a union, and the new field is a null type,
                # create a union of null and the type, with null as the default
                case (
                    {
                        "name": name,
                        "type": (
                            "string" | "boolean" | "long" | "double" | dict(_)
                        ) as t,
                    },
                    {"name": _, "type": "null"},
                ):
                    updated_fields[name] = {
                        **updated_fields[name],
                        "type": ["null", t],
                        "default": None,
                    }
                # if the existing field is primitive and the new field is primitive or complex,
                # and neither are  Union types, and neither is null,
                # create a union of the two types, with no default
                case (
                    {
                        "name": name,
                        "type": (
                            "string" | "boolean" | "long" | "double" | dict(_)
                        ) as t1,
                    },
                    {
                        "name": _,
                        "type": ("string" | "boolean" | "long" | "double") as t2,
                    },
                ) if t1 != t2:
                    updated_fields[name] = {
                        **updated_fields[name],
                        "type": [t1, t2],
                    }
                # if the existing and new fields are different complex types, create a union of the two types
                case (
                    {"type": {"type": type_name1} as t1},
                    {"type": {"type": type_name2} as t2},
                ) if type_name1 != type_name2:
                    updated_fields[field["name"]] = {
                        **updated_fields[field["name"]],
                        "type": [t1, t2],
                    }
                # if existing is a union of null and a primitive type, and the new field is a null type,
                # create a union of null and the primitive type, with null as the default
                case (
                    {
                        "name": name,
                        "type": [
                            "null",
                            ("string" | "boolean" | "long" | "double" | dict(_)) as t,
                        ],
                    },
                    {"name": _, "type": "null"},
                ):
                    updated_fields[name] = {
                        **updated_fields[name],
                        "type": ["null", t],
                        "default": None,
                    }
                # If existing is null and new is a union of null and another type,
                #  return the union of null and another type
                case (
                    {"name": _, "type": "null"},
                    {
                        "name": name,
                        "type": [
                            "null",
                            ("string" | "boolean" | "long" | "double" | dict(_)) as t,
                        ],
                    },
                ):
                    updated_fields[name] = {
                        **updated_fields[name],
                        "type": ["null", t],
                        "default": None,
                    }
                # if existing is a union of types and the new field is a single type,
                # create a union of the two types, with no default
                case (
                    {"name": name, "type": list(types)},
                    {
                        "name": _,
                        "type": ("string" | "boolean" | "long" | "double") as t,
                    },
                ) if t not in types:
                    updated_fields[name] = {
                        **updated_fields[name],
                        "type": [*types, t],
                    }
                case (
                    {"name": name, "type": list(types1)},
                    {"name": _, "type": list(types2), "default": None}
                ) if types1 == types2 and "default" not in updated_fields[name]:
                    updated_fields[name] = {
                        **updated_fields[name],
                        "type": types1,
                        "default": None
                    }
                case (
                    {"name": _, "type": list(types1), "default": None},
                    {"name": name, "type": list(types2)},
                ) if types1 == types2 and "default" not in field:
                    updated_fields[name] = {
                        **updated_fields[name],
                        "type": types1,
                        "default": None
                    }
                case (
                    {
                        "name": _,
                        "type": ("string" | "boolean" | "long" | "double") as t,
                    },
                    {"name": name, "type": list(types)},
                ) if t not in types:
                    updated_fields[name] = {
                        **updated_fields[name],
                        "type": [*types, t],
                    }
                # if new is a union of types and the existing field is a single type,
                # create a union of the two types, with no default
                case (
                    {
                        "name": _,
                        "type": (
                            "string" | "boolean" | "long" | "double" | dict(_)
                        ) as t,
                    },
                    {"name": name, "type": list(types)},
                ) if t not in types:
                    updated_fields[name] = {
                        **updated_fields[name],
                        "type": [*types, t],
                    }
                case (
                    {"name": name, "type": list(types)},
                    {
                        "name": _,
                        "type": (
                            "string" | "boolean" | "long" | "double" | "null"
                        ) as t,
                    },
                ) if t in types:
                    updated_fields[name] = {
                        **updated_fields[name],
                        "type": types,
                    }
                case (
                    {
                        "name": _,
                        "type": (
                            "string" | "boolean" | "long" | "double" | "null"
                        ) as t,
                    },
                    {"name": name, "type": list(types)},
                ) if t in types:
                    updated_fields[name] = {
                        **updated_fields[name],
                        "type": types,
                    }
                case (
                    {
                        "name": name,
                        "type": {"type": "array", "items": dict(items_type1)},
                    },
                    {
                        "name": _,
                        "type": {"type": "array", "items": ["null", dict(items_type2)]},
                    },
                ):
                    updated_fields[name] = {
                        **updated_fields[name],
                        "type": {
                            "type": "array",
                            "items": [
                                "null",
                                cls._merge_schemas(items_type1, items_type2),
                            ],
                        },
                        "default": [],
                    }

                case (
                    {
                        "name": _,
                        "type": {"type": "array", "items": ["null", dict(items_type1)]},
                    },
                    {
                        "name": name,
                        "type": {"type": "array", "items": dict(items_type2)},
                    },
                ):
                    updated_fields[name] = {
                        **updated_fields[name],
                        "type": {
                            "type": "array",
                            "items": [
                                "null",
                                cls._merge_schemas(items_type1, items_type2),
                            ],
                        },
                        "default": [],
                    }
                case (
                    {
                        "name": name,
                        "type": {"type": "array", "items": "null"},
                    },
                    {
                        "name": _,
                        "type": {"type": "array", "items": ["null", dict(_)]},
                    },
                ):
                    updated_fields[name] = {**updated_fields[name], **field}
                case (
                    {
                        "name": _,
                        "type": {"type": "array", "items": ["null", dict(_)]},
                    },
                    {
                        "name": name,
                        "type": {"type": "array", "items": "null"},
                    },
                ):
                    updated_fields[name] = {**updated_fields[name]}
                case (
                    {
                        "name": name,
                        "type": {"type": "array", "items": ["null", dict(items_type1)]},
                    },
                    {
                        "name": _,
                        "type": {"type": "array", "items": ["null", dict(items_type2)]},
                    },
                ):
                    updated_fields[name] = {
                        **updated_fields[name],
                        "type": {
                            "type": "array",
                            "items": [
                                "null",
                                cls._merge_schemas(items_type1, items_type2),
                            ],
                        },
                        "default": [],
                    }

                case (
                    {"name": name, "type": list(t1)},
                    {"name": _, "type": list(t2)},
                ) if t1 != t2:
                    types_set = {*t1, *t2}
                    types = (
                        sorted([t for t in types_set])
                        if "null" not in types_set
                        else ["null", *sorted([t for t in types_set if t != "null"])]
                    )
                    updated_fields[name] = {**updated_fields[name], "type": types}
                case (
                    {"name": _, "type": list(types)},
                    {"name": name, "type": "null"},
                ) if "null" not in types:
                    updated_fields[name] = {
                        **updated_fields[name],
                        "type": ["null", *types],
                        "default": None,
                    }
                case (
                    {"name": name, "type": "null"},
                    {"name": _, "type": list(types)},
                ) if "null" not in types:
                    updated_fields[name] = {
                        **updated_fields[name],
                        "type": ["null", *types],
                        "default": None,
                    }

                # if both fields are array types, merge the items
                case (
                    {"name": name, "type": {"type": "array", "items": dict(items)}},
                    {"name": _, "type": {"type": "array", "items": dict(items2)}},
                ):
                    updated_fields[name] = {
                        **updated_fields[name],
                        "type": {
                            "type": "array",
                            "items": cls._merge_schemas(items, items2),
                        },
                    }

                # if existing is a union of null and an array type, and the new field is an array type,
                # create a union of null and the array type, merging the items, with null as the default
                case (
                    {
                        "name": name,
                        "type": ["null", {"type": "array", "items": dict(items)}],
                    },
                    {"name": _, "type": {"type": "array", "items": dict(items2)}},
                ):
                    updated_fields[name] = {
                        **updated_fields[name],
                        "type": [
                            "null",
                            {
                                "type": "array",
                                "items": cls._merge_schemas(items, items2),
                            },
                        ],
                        "default": None,
                    }
                # if the existing field is an array<record> type and the new field is an array<null> type (based on empty array)
                # create a union of null and the array type with no default
                case (
                    {"name": name, "type": {"type": "array", "items": dict(items)}},
                    {
                        "name": _,
                        "type": {"type": "array", "items": "null"},
                    },
                ):
                    updated_fields[name] = {
                        **updated_fields[name],
                        "type": {
                            "type": "array",
                            "items": ["null", items],
                        },
                        "default": [],
                    }
                # if the existing field is an array<record> type and the new field is an array<null> type (based on empty array)
                # create a single array type, with empty array as the default
                case (
                    {"name": name, "type": {"type": "array", "items": "null"}},
                    {"name": _, "type": {"type": "array", "items": dict(items)}},
                ):
                    updated_fields[name] = {
                        **updated_fields[name],
                        "type": {
                            "type": "array",
                            "items": ["null", items],
                        },
                        "default": [],
                    }
                # if the existing field is an array type, and the new field is a union of null and an array type,
                # create a union of null and the array type, merging the items, with null as the default
                case (
                    {"name": _, "type": {"type": "array", "items": dict(items2)}},
                    {
                        "name": name,
                        "type": ["null", {"type": "array", "items": dict(items)}],
                    },
                ):
                    updated_fields[name] = {
                        **updated_fields[name],
                        "type": [
                            "null",
                            {
                                "type": "array",
                                "items": cls._merge_schemas(items, items2),
                            },
                        ],
                        "default": None,
                    }

                case _:
                    print(type(field["type"]))
                    print(type(updated_fields[field["name"]]["type"]))
                    raise NotImplementedError(
                        f"Can't merge {field} with {updated_fields[field['name']]}"
                    )

        return dict(
            name=self_data["name"],
            type="record",
            fields=sorted(list(updated_fields.values()), key=lambda f: f["name"]),
        )

    @property
    def schema_str(self) -> str:
        return json.dumps(self.schema_dict)

    @classmethod
    def from_avroable_data(cls, msg: AvroableData) -> "MergeableAvroSchema":
        return cls(
            schema_dict=cls.convert_json_to_avro_schema(
                msg.data, msg.record_name
            ),
        )

    @staticmethod
    def convert_json_to_avro_schema(
        json_data: Any, name: str
    ) -> dict:
        avro_schema: dict = {"type": "record", "name": name, "fields": []}

        for key, value in json_data.items():
            match value:
                case dict(_):
                    avro_schema["fields"].append(
                        {
                            "name": key,
                            "type": MergeableAvroSchema.convert_json_to_avro_schema(
                                value, f"{name}_{key}"
                            ),
                        }
                    )

                case [dict(_), *_] as items:
                    avro_schema["fields"].append(
                        {
                            "name": key,
                            "type": {
                                "type": "array",
                                "items": MergeableAvroSchema.sum_schemas(
                                    items, f"{name}_{key}_item"
                                ),
                            },
                        },
                    )
                case None:
                    avro_schema["fields"].append(
                        {"name": key, "type": "null", "default": None}
                    )
                case (str(_) | int(_) | float(_) | bool(_)):
                    avro_schema["fields"].append(
                        {
                            "name": key,
                            "type": PYTHON_TO_AVRO_TYPES[type(value)],
                        }
                    )
                case []:
                    avro_schema["fields"].append(
                        {
                            "name": key,
                            "type": {
                                "type": "array",
                                "items": "null",
                                "default": [],
                            },
                        }
                    )
                case _:
                    raise NotImplementedError(f"Can't convert {value} to avro schema")

        return avro_schema

    @classmethod
    def sum_schemas(cls, items, name):
        item_iter = items.__iter__()
        schema = cls(cls.convert_json_to_avro_schema(next(item_iter), name))
        for item in item_iter:
            schema += cls(cls.convert_json_to_avro_schema(item, name))

        return schema.schema_dict