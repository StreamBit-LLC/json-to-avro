from json_to_avro.avro_schema import (
    RegisteredAvroSchema,
    AvroSchemaCandidate,
)

from json_to_avro.avro_schema.mergeable_avro_schema import MergeableAvroSchema


# mas: Mergeable Avro Schema


def test_custom_fields_2():
    avro_schema = MergeableAvroSchema.convert_json_to_avro_schema(
        {
            "custom_fields": [
                {
                    "name": "IVR PIN #",
                    "value": None,
                    "type": "text",
                    "group": "Default",
                    "created_at": "2023-03-12T00:18:48+00:00",
                    "updated_at": "2023-03-12T00:18:48+00:00",
                    "is_required": False,
                },
                {
                    "name": "IVR ID #",
                    "value": None,
                    "type": "text",
                    "group": "Default",
                    "created_at": "2023-03-12T00:18:48+00:00",
                    "updated_at": "2023-03-12T00:18:48+00:00",
                    "is_required": False,
                },
                {
                    "name": "Certified Payroll Required?",
                    "value": False,
                    "type": "checkbox",
                    "group": "Default",
                    "created_at": "2023-03-12T00:18:48+00:00",
                    "updated_at": "2023-03-12T00:18:48+00:00",
                    "is_required": False,
                },
                {
                    "name": "Unit / Location #",
                    "value": 3,
                    "type": "text",
                    "group": "Default",
                    "created_at": "2023-03-12T00:18:48+00:00",
                    "updated_at": "2023-03-12T00:18:48+00:00",
                    "is_required": False,
                },
                {
                    "name": "Route Day",
                    "value": "Mon",
                    "type": "select",
                    "group": "Default",
                    "created_at": "2023-03-12T00:18:48+00:00",
                    "updated_at": "2023-03-12T00:18:48+00:00",
                    "is_required": False,
                },
                {
                    "name": "Estimated Completion Date",
                    "value": None,
                    "type": "date",
                    "group": "Default",
                    "created_at": "2023-03-12T00:18:48+00:00",
                    "updated_at": "2023-03-12T00:18:48+00:00",
                    "is_required": False,
                },
            ]
        },
        "item",
    )
    expected = {
        "fields": [
            {
                "name": "custom_fields",
                "type": {
                    "items": {
                        "fields": [
                            {"name": "name", "type": "string"},
                            {
                                "default": None,
                                "name": "value",
                                "type": ["null", "boolean", "long", "string"],
                            },
                            {"name": "type", "type": "string"},
                            {"name": "group", "type": "string"},
                            {"name": "created_at", "type": "string"},
                            {"name": "updated_at", "type": "string"},
                            {"name": "is_required", "type": "boolean"},
                        ],
                        "name": "custom_fields_item",
                        "type": "record",
                    },
                    "type": "array",
                },
            }
        ],
        "name": "item",
        "type": "record",
    }
    assert avro_schema == expected


def test_merge_the_schemas():
    s1 = MergeableAvroSchema(
        {
            "fields": [
                {
                    "name": "custom_fields",
                    "type": {
                        "items": {
                            "fields": [
                                {"name": "created_at", "type": "string"},
                                {"name": "group", "type": "string"},
                                {"name": "is_required", "type": "boolean"},
                                {"name": "name", "type": "string"},
                                {"name": "type", "type": "string"},
                                {"name": "updated_at", "type": "string"},
                                {
                                    "default": None,
                                    "name": "value",
                                    "type": ["null", "long", "boolean"],
                                },
                            ],
                            "name": "custom_fields_item",
                            "type": "record",
                        },
                        "type": "array",
                    },
                }
            ],
            "name": "item",
            "type": "record",
        }
    )
    s2 = MergeableAvroSchema(
        {
            "fields": [
                {
                    "name": "custom_fields",
                    "type": {
                        "items": {
                            "fields": [
                                {"name": "name", "type": "string"},
                                {
                                    "default": None,
                                    "name": "value",
                                    "type": ["null", "boolean", "long", "string"],
                                },
                                {"name": "type", "type": "string"},
                                {"name": "group", "type": "string"},
                                {"name": "created_at", "type": "string"},
                                {"name": "updated_at", "type": "string"},
                                {"name": "is_required", "type": "boolean"},
                            ],
                            "name": "custom_fields_item",
                            "type": "record",
                        },
                        "type": "array",
                    },
                }
            ],
            "name": "item",
            "type": "record",
        }
    )

    actual = s1 + s2
    expected = MergeableAvroSchema(
        {
            "fields": [
                {
                    "name": "custom_fields",
                    "type": {
                        "items": {
                            "fields": [
                                {"name": "created_at", "type": "string"},
                                {"name": "group", "type": "string"},
                                {"name": "is_required", "type": "boolean"},
                                {"name": "name", "type": "string"},
                                {"name": "type", "type": "string"},
                                {"name": "updated_at", "type": "string"},
                                {
                                    "default": None,
                                    "name": "value",
                                    "type": ["null", "long", "boolean", "string"],
                                },
                            ],
                            "name": "custom_fields_item",
                            "type": "record",
                        },
                        "type": "array",
                    },
                }
            ],
            "name": "item",
            "type": "record",
        }
    )

    assert actual.schema_dict["fields"] == expected.schema_dict["fields"]


def test_something_else():
    d1 = {
        "name": "item",
        "type": "record",
        "fields": [
            {
                "name": "agents",
                "type": {
                    "type": "array",
                    "items": {
                        "type": "record",
                        "name": "agents_item",
                        "fields": [
                            {"name": "id", "type": "long"},
                            {"name": "first_name", "type": "string"},
                            {"name": "last_name", "type": "string"},
                        ],
                    },
                },
            }
        ],
    }
    d2 = {
        "name": "item",
        "type": "record",
        "fields": [
            {
                "default": [],
                "name": "agents",
                "type": {
                    "type": "array",
                    "items": [
                        "null",
                        {
                            "type": "record",
                            "name": "agents_item",
                            "fields": [
                                {"name": "id", "type": "long"},
                                {"name": "first_name", "type": "string"},
                                {"name": "last_name", "type": "string"},
                            ],
                        },
                    ],
                },
            }
        ],
    }

    s1 = MergeableAvroSchema(schema_dict=d1)
    s2 = MergeableAvroSchema(schema_dict=d2)

    actual = s1 + s2
    expected = MergeableAvroSchema(
        schema_dict={
            "name": "item",
            "type": "record",
            "fields": [
                {
                    "default": [],
                    "name": "agents",
                    "type": {
                        "type": "array",
                        "items": [
                            "null",
                            {
                                "type": "record",
                                "name": "agents_item",
                                "fields": [
                                    {"name": "first_name", "type": "string"},
                                    {"name": "id", "type": "long"},
                                    {"name": "last_name", "type": "string"},
                                ],
                            },
                        ],
                    },
                }
            ],
        }
    )

    assert actual == expected


def test_something_else_else():
    d1 = {
        "name": "item",
        "type": "record",
        "fields": [
            {
                "name": "expenses",
                "type": {"type": "array", "items": "null", "default": []},
            }
        ],
    }
    d2 = {
        "name": "item",
        "type": "record",
        "fields": [
            {
                "default": [],
                "name": "expenses",
                "type": {
                    "type": "array",
                    "items": [
                        "null",
                        {
                            "type": "record",
                            "name": "expenses_item",
                            "fields": [
                                {
                                    "default": None,
                                    "name": "purchased_from",
                                    "type": "null",
                                },
                                {"default": None, "name": "notes", "type": "null"},
                                {"name": "amount", "type": "long"},
                                {"name": "is_billable", "type": "boolean"},
                                {"default": None, "name": "date", "type": "null"},
                                {"name": "created_at", "type": "string"},
                                {"default": None, "name": "updated_at", "type": "null"},
                                {"default": None, "name": "user", "type": "null"},
                                {"default": None, "name": "category", "type": "null"},
                                {
                                    "default": None,
                                    "name": "qbo_class_id",
                                    "type": "null",
                                },
                                {
                                    "default": None,
                                    "name": "qbd_class_id",
                                    "type": "null",
                                },
                            ],
                        },
                    ],
                },
            }
        ],
    }
    s1 = MergeableAvroSchema(schema_dict=d1)
    s2 = MergeableAvroSchema(schema_dict=d2)

    actual = s1 + s2
    expected = MergeableAvroSchema(
        {
            "name": "item",
            "type": "record",
            "fields": [
                {
                    "default": [],
                    "name": "expenses",
                    "type": {
                        "type": "array",
                        "items": [
                            "null",
                            {
                                "type": "record",
                                "name": "expenses_item",
                                "fields": [
                                    {
                                        "default": None,
                                        "name": "purchased_from",
                                        "type": "null",
                                    },
                                    {"default": None, "name": "notes", "type": "null"},
                                    {"name": "amount", "type": "long"},
                                    {"name": "is_billable", "type": "boolean"},
                                    {"default": None, "name": "date", "type": "null"},
                                    {"name": "created_at", "type": "string"},
                                    {
                                        "default": None,
                                        "name": "updated_at",
                                        "type": "null",
                                    },
                                    {"default": None, "name": "user", "type": "null"},
                                    {
                                        "default": None,
                                        "name": "category",
                                        "type": "null",
                                    },
                                    {
                                        "default": None,
                                        "name": "qbo_class_id",
                                        "type": "null",
                                    },
                                    {
                                        "default": None,
                                        "name": "qbd_class_id",
                                        "type": "null",
                                    },
                                ],
                            },
                        ],
                    },
                }
            ],
        }
    )

    assert actual == expected


def test_something_else_the_third():
    d2 = {
        "name": "item",
        "type": "record",
        "fields": [
            {
                "name": "expenses",
                "type": {"type": "array", "items": "null", "default": []},
            }
        ],
    }
    d1 = {
        "name": "item",
        "type": "record",
        "fields": [
            {
                "default": [],
                "name": "expenses",
                "type": {
                    "type": "array",
                    "items": [
                        "null",
                        {
                            "type": "record",
                            "name": "expenses_item",
                            "fields": [
                                {
                                    "default": None,
                                    "name": "purchased_from",
                                    "type": "null",
                                },
                                {"default": None, "name": "notes", "type": "null"},
                                {"name": "amount", "type": "long"},
                                {"name": "is_billable", "type": "boolean"},
                                {"default": None, "name": "date", "type": "null"},
                                {"name": "created_at", "type": "string"},
                                {"default": None, "name": "updated_at", "type": "null"},
                                {"default": None, "name": "user", "type": "null"},
                                {"default": None, "name": "category", "type": "null"},
                                {
                                    "default": None,
                                    "name": "qbo_class_id",
                                    "type": "null",
                                },
                                {
                                    "default": None,
                                    "name": "qbd_class_id",
                                    "type": "null",
                                },
                            ],
                        },
                    ],
                },
            }
        ],
    }

    s1 = MergeableAvroSchema(d1)
    s2 = MergeableAvroSchema(d2)

    actual = s1 + s2
    expected = MergeableAvroSchema(
        schema_dict={
            "fields": [
                {
                    "default": [],
                    "name": "expenses",
                    "type": {
                        "items": [
                            "null",
                            {
                                "fields": [
                                    {
                                        "default": None,
                                        "name": "purchased_from",
                                        "type": "null",
                                    },
                                    {"default": None, "name": "notes", "type": "null"},
                                    {"name": "amount", "type": "long"},
                                    {"name": "is_billable", "type": "boolean"},
                                    {"default": None, "name": "date", "type": "null"},
                                    {"name": "created_at", "type": "string"},
                                    {
                                        "default": None,
                                        "name": "updated_at",
                                        "type": "null",
                                    },
                                    {"default": None, "name": "user", "type": "null"},
                                    {
                                        "default": None,
                                        "name": "category",
                                        "type": "null",
                                    },
                                    {
                                        "default": None,
                                        "name": "qbo_class_id",
                                        "type": "null",
                                    },
                                    {
                                        "default": None,
                                        "name": "qbd_class_id",
                                        "type": "null",
                                    },
                                ],
                                "name": "expenses_item",
                                "type": "record",
                            },
                        ],
                        "type": "array",
                    },
                }
            ],
            "name": "item",
            "type": "record",
        }
    )

    assert actual == expected


def test():
    d1 = {
        "name": "jobs",
        "type": "record",
        "fields": [
            {
                "name": "documents",
                "type": {"type": "array", "items": "null", "default": []},
            }
        ],
    }
    d2 = {
        "name": "jobs",
        "type": "record",
        "fields": [
            {
                "name": "documents",
                "type": ["null", {"default": [], "type": "array", "items": "null"}],
            }
        ],
    }

    s1 = MergeableAvroSchema(d1)
    s2 = MergeableAvroSchema(d2)

    actual = s2 + s1
    expected = MergeableAvroSchema(
        schema_dict={
            "fields": [
                {
                    "default": None,
                    "name": "documents",
                    "type": ["null", {"default": [], "type": "array", "items": "null"}],
                }
            ],
            "name": "jobs",
            "type": "record",
        }
    )

    assert actual == expected
