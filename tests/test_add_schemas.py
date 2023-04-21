import pytest

from json_to_avro.avro_schema import (
    MergeableAvroSchema,
    RecordAvroType,
)


def test_when_field_types_are_identical_returns_the_field_unchanged():
    schema1 = MergeableAvroSchema(
        RecordAvroType(
            name="jobs",
            type="record",
            fields=[
                {"name": "id", "type": "long"},
            ],
        )
    )
    schema2 = MergeableAvroSchema(
        RecordAvroType(
            name="jobs",
            type="record",
            fields=[
                {"name": "id", "type": "long"},
            ],
        )
    )

    actual = schema1 + schema2
    expected = MergeableAvroSchema(
        RecordAvroType(
            name="jobs",
            type="record",
            fields=[
                {"name": "id", "type": "long"},
            ],
        )
    )

    assert actual == expected


def test_when_field_is_in_new_schema_and_not_existing_return_field_as_union_of_null_and_new_type() -> (
    None
):
    schema1 = MergeableAvroSchema(
        RecordAvroType(
            name="jobs",
            type="record",
            fields=[
                {"name": "id", "type": "long"},
            ],
        )
    )
    schema2 = MergeableAvroSchema(
        RecordAvroType(
            name="jobs",
            type="record",
            fields=[
                {"name": "id", "type": "long"},
                {"name": "number", "type": "string"},
            ],
        )
    )

    actual = schema1 + schema2
    expected = MergeableAvroSchema(
        RecordAvroType(
            name="jobs",
            type="record",
            fields=[
                {"name": "id", "type": "long"},
                {"name": "number", "type": ["null", "string"], "default": None},
            ],
        )
    )

    assert actual == expected


def test_if_existing_field_is_null_type_and_new_field_is_not_null_and_not_union_creates_union_of_null_and_new_type() -> (
    None
):
    schema1 = MergeableAvroSchema(
        RecordAvroType(
            name="jobs",
            type="record",
            fields=[
                {"name": "id", "type": "long"},
                {"name": "number", "type": "null"},
            ],
        )
    )
    schema2 = MergeableAvroSchema(
        RecordAvroType(
            name="jobs",
            type="record",
            fields=[
                {"name": "id", "type": "long"},
                {"name": "number", "type": "string"},
            ],
        )
    )

    actual = schema1 + schema2
    expected = MergeableAvroSchema(
        RecordAvroType(
            name="jobs",
            type="record",
            fields=[
                {"name": "id", "type": "long"},
                {"name": "number", "type": ["null", "string"], "default": None},
            ],
        )
    )

    assert actual == expected


def test_if_existing_field_is_null_type_and_new_field_is_complex_type_and_not_union_creates_union_of_null_and_new_type() -> (
    None
):
    schema1 = MergeableAvroSchema(
        RecordAvroType(
            name="jobs",
            type="record",
            fields=[
                {"name": "id", "type": "long"},
                {"name": "number", "type": "null"},
            ],
        )
    )
    schema2 = MergeableAvroSchema(
        RecordAvroType(
            name="jobs",
            type="record",
            fields=[
                {"name": "id", "type": "long"},
                {
                    "name": "number",
                    "type": {
                        "type": "record",
                        "name": "number",
                        "fields": [
                            {"name": "id", "type": "long"},
                            {"name": "number", "type": "string"},
                        ],
                    },
                },
            ],
        )
    )

    actual = schema1 + schema2
    expected = MergeableAvroSchema(
        RecordAvroType(
            name="jobs",
            type="record",
            fields=[
                {"name": "id", "type": "long"},
                {
                    "name": "number",
                    "type": [
                        "null",
                        {
                            "type": "record",
                            "name": "number",
                            "fields": [
                                {"name": "id", "type": "long"},
                                {"name": "number", "type": "string"},
                            ],
                        },
                    ],
                    "default": None,
                },
            ],
        )
    )

    assert actual == expected


def test_if_new_field_is_null_type_and_existing_field_is_not_null_and_not_union_creates_union_of_null_and_new_type() -> (
    None
):
    schema1 = MergeableAvroSchema(
        RecordAvroType(
            name="jobs",
            type="record",
            fields=[
                {"name": "id", "type": "long"},
                {"name": "number", "type": "string"},
            ],
        )
    )
    schema2 = MergeableAvroSchema(
        RecordAvroType(
            name="jobs",
            type="record",
            fields=[
                {"name": "id", "type": "long"},
                {"name": "number", "type": "null"},
            ],
        )
    )

    actual = schema1 + schema2
    expected = MergeableAvroSchema(
        RecordAvroType(
            name="jobs",
            type="record",
            fields=[
                {"name": "id", "type": "long"},
                {"name": "number", "type": ["null", "string"], "default": None},
            ],
        )
    )

    assert actual == expected


def test_returns_field_as_optional_when_new_schema_has_null_type() -> None:
    schema1 = MergeableAvroSchema(
        RecordAvroType(
            name="jobs",
            type="record",
            fields=[
                {"name": "id", "type": "long"},
            ],
        )
    )
    schema2 = MergeableAvroSchema(
        RecordAvroType(
            name="jobs",
            type="record",
            fields=[{"name": "id", "type": "null"}],
        )
    )

    actual = schema1 + schema2
    expected = MergeableAvroSchema(
        RecordAvroType(
            name="jobs",
            type="record",
            fields=[
                {"name": "id", "type": ["null", "long"], "default": None},
            ],
        )
    )

    assert actual == expected


def test_returns_field_merge_additional_type_into_type_array_when_type_is_already_an_array() -> (
    None
):
    schema1 = MergeableAvroSchema(
        RecordAvroType(
            name="jobs",
            type="record",
            fields=[
                {"name": "id", "type": ["long", "double"]},
            ],
        )
    )
    schema2 = MergeableAvroSchema(
        RecordAvroType(
            name="jobs",
            type="record",
            fields=[{"name": "id", "type": "null"}],
        )
    )

    actual = schema1 + schema2
    expected = MergeableAvroSchema(
        RecordAvroType(
            name="jobs",
            type="record",
            fields=[
                {"name": "id", "type": ["null", "long", "double"], "default": None},
            ],
        )
    )

    assert actual == expected


def test_existing_field_and_new_field_are_primitives_not_union_and_neither_null_create_union_of_both_types() -> (
    None
):
    schema1 = MergeableAvroSchema(
        RecordAvroType(
            name="jobs",
            type="record",
            fields=[
                {"name": "id", "type": "long"},
                {"name": "number", "type": "string"},
            ],
        )
    )
    schema2 = MergeableAvroSchema(
        RecordAvroType(
            name="jobs",
            type="record",
            fields=[
                {"name": "id", "type": "long"},
                {"name": "number", "type": "double"},
            ],
        )
    )

    actual = schema1 + schema2
    expected = MergeableAvroSchema(
        RecordAvroType(
            name="jobs",
            type="record",
            fields=[
                {"name": "id", "type": "long"},
                {"name": "number", "type": ["string", "double"]},
            ],
        )
    )

    assert actual == expected


def test_existing_field_is_primitive_and_new_field_is_complex_and_neither_union_and_neither_null_create_union_of_both_types() -> (
    None
):
    schema1 = MergeableAvroSchema(
        RecordAvroType(
            name="jobs",
            type="record",
            fields=[
                {"name": "id", "type": "long"},
                {"name": "number", "type": "string"},
            ],
        )
    )
    schema2 = MergeableAvroSchema(
        RecordAvroType(
            name="jobs",
            type="record",
            fields=[
                {"name": "id", "type": "long"},
                {
                    "name": "number",
                    "type": {
                        "type": "record",
                        "name": "number",
                        "fields": [
                            {"name": "id", "type": "long"},
                            {"name": "number", "type": "string"},
                        ],
                    },
                },
            ],
        )
    )

    actual = schema1 + schema2
    expected = MergeableAvroSchema(
        RecordAvroType(
            name="jobs",
            type="record",
            fields=[
                {"name": "id", "type": "long"},
                {
                    "name": "number",
                    "type": [
                        "string",
                        {
                            "type": "record",
                            "name": "number",
                            "fields": [
                                {"name": "id", "type": "long"},
                                {"name": "number", "type": "string"},
                            ],
                        },
                    ],
                },
            ],
        )
    )

    assert actual == expected


def test_existing_field_is_complex_and_new_field_is_primitive_and_neither_union_and_neither_null_create_union_of_both_types() -> (
    None
):
    schema1 = MergeableAvroSchema(
        RecordAvroType(
            name="jobs",
            type="record",
            fields=[
                {"name": "id", "type": "long"},
                {
                    "name": "number",
                    "type": {
                        "type": "record",
                        "name": "number",
                        "fields": [
                            {"name": "id", "type": "long"},
                            {"name": "number", "type": "string"},
                        ],
                    },
                },
            ],
        )
    )
    schema2 = MergeableAvroSchema(
        RecordAvroType(
            name="jobs",
            type="record",
            fields=[
                {"name": "id", "type": "long"},
                {"name": "number", "type": "string"},
            ],
        )
    )

    actual = schema1 + schema2
    expected = MergeableAvroSchema(
        RecordAvroType(
            name="jobs",
            type="record",
            fields=[
                {"name": "id", "type": "long"},
                {
                    "name": "number",
                    "type": [
                        {
                            "type": "record",
                            "name": "number",
                            "fields": [
                                {"name": "id", "type": "long"},
                                {"name": "number", "type": "string"},
                            ],
                        },
                        "string",
                    ],
                },
            ],
        )
    )

    assert actual == expected


def test_existing_field_is_array_type_and_new_field_is_record_and_neither_union_and_neither_null_create_union_of_both_types() -> (
    None
):
    schema1 = MergeableAvroSchema(
        RecordAvroType(
            name="jobs",
            type="record",
            fields=[
                {"name": "id", "type": "long"},
                {
                    "name": "number",
                    "type": {
                        "type": "array",
                        "items": {
                            "type": "record",
                            "name": "number",
                            "fields": [
                                {"name": "id", "type": "long"},
                                {"name": "number", "type": "string"},
                            ],
                        },
                    },
                },
            ],
        )
    )
    schema2 = MergeableAvroSchema(
        RecordAvroType(
            name="jobs",
            type="record",
            fields=[
                {"name": "id", "type": "long"},
                {
                    "name": "number",
                    "type": {
                        "type": "record",
                        "name": "number",
                        "fields": [
                            {"name": "id", "type": "long"},
                            {"name": "number", "type": "string"},
                        ],
                    },
                },
            ],
        )
    )

    actual = schema1 + schema2
    expected = MergeableAvroSchema(
        RecordAvroType(
            name="jobs",
            type="record",
            fields=[
                {"name": "id", "type": "long"},
                {
                    "name": "number",
                    "type": [
                        {
                            "type": "array",
                            "items": {
                                "type": "record",
                                "name": "number",
                                "fields": [
                                    {"name": "id", "type": "long"},
                                    {"name": "number", "type": "string"},
                                ],
                            },
                        },
                        {
                            "type": "record",
                            "name": "number",
                            "fields": [
                                {"name": "id", "type": "long"},
                                {"name": "number", "type": "string"},
                            ],
                        },
                    ],
                },
            ],
        )
    )

    assert actual == expected


def test_existing_field_is_record_type_and_new_field_is_array_and_neither_union_and_neither_null_create_union_of_both_types() -> (
    None
):
    schema1 = MergeableAvroSchema(
        RecordAvroType(
            name="jobs",
            type="record",
            fields=[
                {"name": "id", "type": "long"},
                {
                    "name": "number",
                    "type": {
                        "type": "record",
                        "name": "number",
                        "fields": [
                            {"name": "id", "type": "long"},
                            {"name": "number", "type": "string"},
                        ],
                    },
                },
            ],
        )
    )
    schema2 = MergeableAvroSchema(
        RecordAvroType(
            name="jobs",
            type="record",
            fields=[
                {"name": "id", "type": "long"},
                {
                    "name": "number",
                    "type": {
                        "type": "array",
                        "items": {
                            "type": "record",
                            "name": "number",
                            "fields": [
                                {"name": "id", "type": "long"},
                                {"name": "number", "type": "string"},
                            ],
                        },
                    },
                },
            ],
        )
    )

    actual = schema1 + schema2
    expected = MergeableAvroSchema(
        RecordAvroType(
            name="jobs",
            type="record",
            fields=[
                {"name": "id", "type": "long"},
                {
                    "name": "number",
                    "type": [
                        {
                            "type": "record",
                            "name": "number",
                            "fields": [
                                {"name": "id", "type": "long"},
                                {"name": "number", "type": "string"},
                            ],
                        },
                        {
                            "type": "array",
                            "items": {
                                "type": "record",
                                "name": "number",
                                "fields": [
                                    {"name": "id", "type": "long"},
                                    {"name": "number", "type": "string"},
                                ],
                            },
                        },
                    ],
                },
            ],
        )
    )

    assert actual == expected


def test_existing_field_is_union_type_with_null_and_new_field_is_null_return_existing():
    schema1 = MergeableAvroSchema(
        RecordAvroType(
            name="jobs",
            type="record",
            fields=[
                {"name": "id", "type": "long"},
                {"name": "number", "type": ["null", "string"]},
            ],
        )
    )
    schema2 = MergeableAvroSchema(
        RecordAvroType(
            name="jobs",
            type="record",
            fields=[
                {"name": "id", "type": "long"},
                {"name": "number", "type": "null"},
            ],
        )
    )

    actual = schema1 + schema2
    expected = MergeableAvroSchema(
        RecordAvroType(
            name="jobs",
            type="record",
            fields=[
                {"name": "id", "type": "long"},
                {"name": "number", "type": ["null", "string"], "default": None},
            ],
        )
    )

    assert actual == expected


def test_existing_field_is_null_and_new_field_is_union_type_with_null_return_new():
    schema1 = MergeableAvroSchema(
        RecordAvroType(
            name="jobs",
            type="record",
            fields=[
                {"name": "id", "type": "long"},
                {"name": "number", "type": "null"},
            ],
        )
    )
    schema2 = MergeableAvroSchema(
        RecordAvroType(
            name="jobs",
            type="record",
            fields=[
                {"name": "id", "type": "long"},
                {"name": "number", "type": ["null", "string"]},
            ],
        )
    )

    actual = schema1 + schema2
    expected = MergeableAvroSchema(
        RecordAvroType(
            name="jobs",
            type="record",
            fields=[
                {"name": "id", "type": "long"},
                {"name": "number", "type": ["null", "string"], "default": None},
            ],
        )
    )

    assert actual == expected


def test_existing_is_a_union_types_and_new_is_a_different_non_null_type_return_union_of_both_types():
    schema1 = MergeableAvroSchema(
        RecordAvroType(
            name="jobs",
            type="record",
            fields=[
                {"name": "id", "type": "long"},
                {"name": "number", "type": ["null", "string"], "default": None},
            ],
        )
    )
    schema2 = MergeableAvroSchema(
        RecordAvroType(
            name="jobs",
            type="record",
            fields=[
                {"name": "id", "type": "long"},
                {"name": "number", "type": "long"},
            ],
        )
    )

    actual = schema1 + schema2
    expected = MergeableAvroSchema(
        RecordAvroType(
            name="jobs",
            type="record",
            fields=[
                {"name": "id", "type": "long"},
                {"name": "number", "type": ["null", "string", "long"], "default": None},
            ],
        )
    )

    assert actual == expected


def test_existing_is_a_union_type_with_no_null_and_new_is_a_null_type_return_union_of_both_types_with_default_null():
    schema1 = MergeableAvroSchema(
        RecordAvroType(
            name="jobs",
            type="record",
            fields=[
                {"name": "id", "type": "long"},
                {"name": "number", "type": ["string", "long"]},
            ],
        )
    )
    schema2 = MergeableAvroSchema(
        RecordAvroType(
            name="jobs",
            type="record",
            fields=[
                {"name": "id", "type": "long"},
                {"name": "number", "type": "null"},
            ],
        )
    )

    actual = schema1 + schema2
    expected = MergeableAvroSchema(
        RecordAvroType(
            name="jobs",
            type="record",
            fields=[
                {"name": "id", "type": "long"},
                {"name": "number", "type": ["null", "string", "long"], "default": None},
            ],
        )
    )

    assert actual == expected


def test_existing_non_null_non_union_type_and_new_is_union_type_return_union_of_both_types():
    schema1 = MergeableAvroSchema(
        RecordAvroType(
            name="jobs",
            type="record",
            fields=[
                {"name": "id", "type": "long"},
                {"name": "number", "type": "long"},
            ],
        )
    )
    schema2 = MergeableAvroSchema(
        RecordAvroType(
            name="jobs",
            type="record",
            fields=[
                {"name": "id", "type": "long"},
                {"name": "number", "type": ["double", "string"]},
            ],
        )
    )

    actual = schema1 + schema2
    expected = MergeableAvroSchema(
        RecordAvroType(
            name="jobs",
            type="record",
            fields=[
                {"name": "id", "type": "long"},
                {"name": "number", "type": ["double", "string", "long"]},
            ],
        )
    )

    assert actual == expected


def test_existing_is_null_and_new_is_union_of_non_null_types_return_union_of_both_types():
    schema1 = MergeableAvroSchema(
        RecordAvroType(
            name="jobs",
            type="record",
            fields=[
                {"name": "id", "type": "long"},
                {"name": "number", "type": "null"},
            ],
        )
    )
    schema2 = MergeableAvroSchema(
        RecordAvroType(
            name="jobs",
            type="record",
            fields=[
                {"name": "id", "type": "long"},
                {"name": "number", "type": ["double", "string"]},
            ],
        )
    )

    actual = schema1 + schema2
    expected = MergeableAvroSchema(
        RecordAvroType(
            name="jobs",
            type="record",
            fields=[
                {"name": "id", "type": "long"},
                {
                    "name": "number",
                    "type": ["null", "double", "string"],
                    "default": None,
                },
            ],
        )
    )

    assert actual == expected


def test_existing_is_union_and_new_is_primitive_type_already_present_in_union():
    schema1 = MergeableAvroSchema(
        RecordAvroType(
            name="jobs",
            type="record",
            fields=[
                {"name": "id", "type": "long"},
                {"name": "number", "type": ["double", "string"]},
            ],
        )
    )
    schema2 = MergeableAvroSchema(
        RecordAvroType(
            name="jobs",
            type="record",
            fields=[
                {"name": "id", "type": "long"},
                {"name": "number", "type": "string"},
            ],
        )
    )

    actual = schema1 + schema2
    expected = MergeableAvroSchema(
        RecordAvroType(
            name="jobs",
            type="record",
            fields=[
                {"name": "id", "type": "long"},
                {"name": "number", "type": ["double", "string"]},
            ],
        )
    )

    assert actual == expected


def test_existing_is_a_union_of_a_complex_type_and_primitive_and_new_is_the_same_complex_type_which_should_be_merged():
    schema1 = MergeableAvroSchema(
        RecordAvroType(
            name="jobs",
            type="record",
            fields=[
                {"name": "id", "type": "long"},
                {
                    "name": "number",
                    "type": [
                        {
                            "type": "record",
                            "name": "number",
                            "fields": [
                                {"name": "id", "type": "long"},
                                {"name": "value", "type": "long"},
                            ],
                        },
                        "string",
                    ],
                },
            ],
        )
    )
    schema2 = MergeableAvroSchema(
        RecordAvroType(
            name="jobs",
            type="record",
            fields=[
                {"name": "id", "type": "long"},
                {
                    "name": "number",
                    "type": {
                        "type": "record",
                        "name": "number",
                        "fields": [
                            {"name": "id", "type": "long"},
                            {"name": "value", "type": "null"},
                        ],
                    },
                },
            ],
        )
    )

    actual = schema1 + schema2
    expected = MergeableAvroSchema(
        RecordAvroType(
            name="jobs",
            type="record",
            fields=[
                {"name": "id", "type": "long"},
                {
                    "name": "number",
                    "type": [
                        {
                            "type": "record",
                            "name": "number",
                            "fields": [
                                {"name": "id", "type": "long"},
                                {
                                    "name": "value",
                                    "type": ["null", "long"],
                                    "default": None,
                                },
                            ],
                        },
                        "string",
                    ],
                },
            ],
        )
    )

    assert actual == expected


def test_existing_is_primitive_type_and_new_is_union_type_with_existing_primitive_type():
    schema1 = MergeableAvroSchema(
        RecordAvroType(
            name="jobs",
            type="record",
            fields=[
                {"name": "id", "type": "long"},
                {"name": "number", "type": "string"},
            ],
        )
    )
    schema2 = MergeableAvroSchema(
        RecordAvroType(
            name="jobs",
            type="record",
            fields=[
                {"name": "id", "type": "long"},
                {"name": "number", "type": ["double", "string"]},
            ],
        )
    )

    actual = schema1 + schema2
    expected = MergeableAvroSchema(
        RecordAvroType(
            name="jobs",
            type="record",
            fields=[
                {"name": "id", "type": "long"},
                {"name": "number", "type": ["double", "string"]},
            ],
        )
    )

    assert actual == expected


def test_recursively_merge_schemas() -> None:
    schema1 = MergeableAvroSchema(
        dict(
            name="jobs",
            type="record",
            fields=[
                {"name": "id", "type": ["long", "double"]},
                {
                    "name": "printable_work_order",
                    "type": {
                        "type": "array",
                        "items": {
                            "type": "record",
                            "name": "printable_work_order_item",
                            "fields": [
                                {"name": "name", "type": "null"},
                                {"name": "url", "type": "string"},
                            ],
                        },
                    },
                },
            ],
        )
    )
    schema2 = MergeableAvroSchema(
        RecordAvroType(
            name="jobs",
            type="record",
            fields=[
                {"name": "id", "type": "null"},
                {
                    "name": "printable_work_order",
                    "type": {
                        "type": "array",
                        "items": {
                            "type": "record",
                            "name": "printable_work_order_item",
                            "fields": [
                                {"name": "name", "type": "string"},
                                {"name": "url", "type": "string"},
                            ],
                        },
                    },
                },
            ],
        )
    )

    actual = schema1 + schema2
    expected = MergeableAvroSchema(
        schema_dict={
            "name": "jobs",
            "type": "record",
            "fields": [
                {"default": None, "name": "id", "type": ["null", "long", "double"]},
                {
                    "name": "printable_work_order",
                    "type": "array",
                    "items": {
                        "type": "record",
                        "name": "printable_work_order_item",
                        "fields": [
                            {
                                "name": "name",
                                "type": ["null", "string"],
                                "default": None,
                            },
                            {"name": "url", "type": "string"},
                        ],
                    },
                },
            ],
        }
    )

    assert actual == expected


def test_merge_array_items_existing_items_a_record_new_items_are_null():
    schema1 = MergeableAvroSchema(
        RecordAvroType(
            name="jobs",
            type="record",
            fields=[
                {"name": "id", "type": "long"},
                {
                    "name": "printable_work_order",
                    "type": {
                        "type": "array",
                        "items": {
                            "type": "record",
                            "name": "printable_work_order_item",
                            "fields": [
                                {"name": "url", "type": "string"},
                            ],
                        },
                    },
                },
            ],
        )
    )
    schema2 = MergeableAvroSchema(
        RecordAvroType(
            name="jobs",
            type="record",
            fields=[
                {"name": "id", "type": "long"},
                {
                    "name": "printable_work_order",
                    "type": {
                        "type": "array",
                        "items": "null",
                    },
                },
            ],
        )
    )

    actual = schema1 + schema2
    expected = MergeableAvroSchema(
        schema_dict={
            "name": "jobs",
            "type": "record",
            "fields": [
                {"name": "id", "type": "long"},
                {
                    "name": "printable_work_order",
                    "type": {
                        "type": "array",
                        "items": [
                            "null",
                            {
                                "type": "record",
                                "name": "printable_work_order_item",
                                "fields": [
                                    {"name": "url", "type": "string"},
                                ],
                            },
                        ],
                    },
                    "default": [],
                },
            ],
        }
    )

    assert actual == expected


def test_merge_array_items_existing_are_null_a_record_new_items_are_record():
    schema1 = MergeableAvroSchema(
        RecordAvroType(
            name="jobs",
            type="record",
            fields=[
                {"name": "id", "type": "long"},
                {
                    "name": "printable_work_order",
                    "type": {
                        "type": "array",
                        "items": "null",
                    },
                },
            ],
        )
    )
    schema2 = MergeableAvroSchema(
        RecordAvroType(
            name="jobs",
            type="record",
            fields=[
                {"name": "id", "type": "long"},
                {
                    "name": "printable_work_order",
                    "type": {
                        "type": "array",
                        "items": {
                            "type": "record",
                            "name": "printable_work_order_item",
                            "fields": [
                                {"name": "url", "type": "string"},
                            ],
                        },
                    },
                },
            ],
        )
    )

    actual = schema1 + schema2
    expected = MergeableAvroSchema(
        schema_dict={
            "name": "jobs",
            "type": "record",
            "fields": [
                {"name": "id", "type": "long"},
                {
                    "name": "printable_work_order",
                    "type": {
                        "type": "array",
                        "items": {
                            "type": "record",
                            "name": "printable_work_order_item",
                            "fields": [
                                {"name": "url", "type": "string"},
                            ],
                        },
                    },
                    "default": [],
                },
            ],
        }
    )

    assert actual == expected

    # NotImplementedError: Can't merge
    # {'name': 'custom_fields', 'type': {'type': 'array', 'items': {'type': 'record', 'name': 'custom_fields_item', 'fields': [{'name': 'name', 'type': 'string'}, {'name': 'value', 'type': 'null', 'default': None}, {'name': 'type', 'type': 'string'}, {'name': 'group', 'type': 'string'}, {'name': 'created_at', 'type': 'string'}, {'name': 'updated_at', 'type': 'string'}, {'name': 'is_required', 'type': 'boolean'}]}}} with {'name': 'custom_fields', 'type': {'default': [], 'type': 'array', 'items': 'null'}}
    ...


def test_recursively_merge_schemas_with_existing_null() -> None:
    schema1 = MergeableAvroSchema(
        RecordAvroType(
            name="jobs",
            type="record",
            fields=[
                {"name": "id", "type": ["long", "double"]},
                {
                    "name": "printable_work_order",
                    "type": "array",
                    "items": {
                        "type": "record",
                        "name": "printable_work_order_item",
                        "fields": [
                            {"name": "name", "type": "null"},
                            {"name": "url", "type": "string"},
                        ],
                    },
                },
            ],
        )
    )
    schema2 = MergeableAvroSchema(
        RecordAvroType(
            name="jobs",
            type="record",
            fields=[
                {"name": "id", "type": "null"},
                {
                    "name": "printable_work_order",
                    "type": "array",
                    "items": {
                        "type": "record",
                        "name": "printable_work_order_item",
                        "fields": [
                            {"name": "name", "type": "string"},
                            {"name": "url", "type": "string"},
                        ],
                    },
                },
            ],
        )
    )

    actual = schema1 + schema2
    expected = MergeableAvroSchema(
        schema_dict={
            "name": "jobs",
            "type": "record",
            "fields": [
                {"default": None, "name": "id", "type": ["null", "long", "double"]},
                {
                    "name": "printable_work_order",
                    "type": "array",
                    "items": {
                        "type": "record",
                        "name": "printable_work_order_item",
                        "fields": [
                            {
                                "name": "name",
                                "type": ["null", "string"],
                                "default": None,
                            },
                            {"name": "url", "type": "string"},
                        ],
                    },
                },
            ],
        }
    )

    assert actual == expected


# existing schema:
x = MergeableAvroSchema(
    schema_dict={
        "type": "record",
        "name": "jobs",
        "fields": [
            {"name": "id", "type": "long"},
            {"name": "number", "type": "string"},
            {"default": None, "name": "check_number", "type": "null"},
            {"name": "priority", "type": "string"},
            {"name": "description", "type": "string"},
            {"name": "tech_notes", "type": "string"},
            {"default": None, "name": "completion_notes", "type": "null"},
            {"name": "payment_status", "type": "string"},
            {"name": "taxes_fees_total", "type": "long"},
            {"name": "drive_labor_total", "type": "long"},
            {"name": "billable_expenses_total", "type": "long"},
            {"name": "total", "type": "long"},
            {"name": "payments_deposits_total", "type": "long"},
            {"name": "due_total", "type": "long"},
            {"name": "cost_total", "type": "long"},
            {"name": "duration", "type": "long"},
            {"name": "time_frame_promised_start", "type": "string"},
            {"name": "time_frame_promised_end", "type": "string"},
            {"default": None, "name": "start_date", "type": "null"},
            {"default": None, "name": "end_date", "type": "null"},
            {"name": "created_at", "type": "string"},
            {"name": "updated_at", "type": "string"},
            {"default": None, "name": "closed_at", "type": "null"},
            {"default": None, "name": "customer_id", "type": "null"},
            {"default": None, "name": "customer_name", "type": "null"},
            {"default": None, "name": "parent_customer", "type": "null"},
            {"name": "status", "type": "string"},
            {"default": None, "name": "sub_status", "type": "null"},
            {"default": None, "name": "contact_first_name", "type": "null"},
            {"default": None, "name": "contact_last_name", "type": "null"},
            {"default": None, "name": "street_1", "type": "null"},
            {"default": None, "name": "street_2", "type": "null"},
            {"default": None, "name": "city", "type": "null"},
            {"default": None, "name": "state_prov", "type": "null"},
            {"default": None, "name": "postal_code", "type": "null"},
            {"default": None, "name": "location_name", "type": "null"},
            {"name": "is_gated", "type": "boolean"},
            {"default": None, "name": "gate_instructions", "type": "null"},
            {"name": "category", "type": "string"},
            {"default": None, "name": "source", "type": "null"},
            {"name": "payment_type", "type": "string"},
            {"name": "customer_payment_terms", "type": "string"},
            {"default": None, "name": "project", "type": "null"},
            {"default": None, "name": "phase", "type": "null"},
            {"name": "po_number", "type": "string"},
            {"name": "contract", "type": "string"},
            {"name": "note_to_customer", "type": "string"},
            {"default": None, "name": "called_in_by", "type": "null"},
            {"name": "is_requires_follow_up", "type": "boolean"},
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
            },
            {
                "name": "custom_fields",
                "type": {"default": [], "type": "array", "items": "null"},
            },
            {
                "name": "pictures",
                "type": {"default": [], "type": "array", "items": "null"},
            },
            {
                "name": "documents",
                "type": {"default": [], "type": "array", "items": "null"},
            },
            {
                "name": "equipment",
                "type": {"default": [], "type": "array", "items": "null"},
            },
            {
                "name": "techs_assigned",
                "type": {"default": [], "type": "array", "items": "null"},
            },
            {
                "name": "tasks",
                "type": {
                    "type": "array",
                    "items": {
                        "type": "record",
                        "name": "tasks_item",
                        "fields": [
                            {"name": "type", "type": "string"},
                            {"name": "description", "type": "string"},
                            {"default": None, "name": "start_time", "type": "null"},
                            {"default": None, "name": "start_date", "type": "null"},
                            {"default": None, "name": "end_date", "type": "null"},
                            {"name": "is_completed", "type": "boolean"},
                            {"name": "created_at", "type": "string"},
                            {"name": "updated_at", "type": "string"},
                        ],
                    },
                },
            },
            {
                "name": "notes",
                "type": {"default": [], "type": "array", "items": "null"},
            },
            {
                "name": "products",
                "type": {"default": [], "type": "array", "items": "null"},
            },
            {
                "name": "services",
                "type": {
                    "type": "array",
                    "items": {
                        "type": "record",
                        "name": "services_item",
                        "fields": [
                            {"name": "name", "type": "string"},
                            {"name": "description", "type": "string"},
                            {"name": "multiplier", "type": "long"},
                            {"name": "rate", "type": "long"},
                            {"name": "total", "type": "long"},
                            {"name": "cost", "type": "long"},
                            {"name": "actual_cost", "type": "long"},
                            {"name": "item_index", "type": "long"},
                            {"name": "parent_index", "type": "long"},
                            {"name": "created_at", "type": "string"},
                            {"name": "updated_at", "type": "string"},
                            {"name": "is_show_rate_items", "type": "boolean"},
                            {"default": None, "name": "tax", "type": "null"},
                            {"name": "service", "type": "string"},
                            {"name": "service_list_id", "type": "long"},
                            {"name": "service_rate_id", "type": "long"},
                            {"default": None, "name": "pattern_row_id", "type": "null"},
                            {"default": None, "name": "qbo_class_id", "type": "null"},
                            {"default": None, "name": "qbd_class_id", "type": "null"},
                        ],
                    },
                },
            },
            {
                "name": "other_charges",
                "type": {
                    "type": "array",
                    "items": {
                        "type": "record",
                        "name": "other_charges_item",
                        "fields": [
                            {"name": "name", "type": "string"},
                            {"name": "rate", "type": "long"},
                            {"name": "total", "type": "long"},
                            {"name": "charge_index", "type": "long"},
                            {"name": "parent_index", "type": "long"},
                            {"name": "is_percentage", "type": "boolean"},
                            {"name": "is_discount", "type": "boolean"},
                            {"name": "created_at", "type": "string"},
                            {"name": "updated_at", "type": "string"},
                            {"name": "other_charge", "type": "string"},
                            {"default": None, "name": "applies_to", "type": "null"},
                            {
                                "default": None,
                                "name": "service_list_id",
                                "type": "null",
                            },
                            {"name": "other_charge_id", "type": "long"},
                            {"default": None, "name": "pattern_row_id", "type": "null"},
                            {"default": None, "name": "qbo_class_id", "type": "null"},
                            {"default": None, "name": "qbd_class_id", "type": "null"},
                        ],
                    },
                },
            },
            {
                "name": "labor_charges",
                "type": {"default": [], "type": "array", "items": "null"},
            },
            {
                "name": "expenses",
                "type": {"default": [], "type": "array", "items": "null"},
            },
            {
                "name": "payments",
                "type": {"default": [], "type": "array", "items": "null"},
            },
            {
                "name": "invoices",
                "type": {"default": [], "type": "array", "items": "null"},
            },
            {
                "name": "signatures",
                "type": {"default": [], "type": "array", "items": "null"},
            },
            {
                "name": "printable_work_order",
                "type": {
                    "type": "array",
                    "items": {
                        "type": "record",
                        "name": "printable_work_order_item",
                        "fields": [
                            {"name": "name", "type": "string"},
                            {"name": "url", "type": "string"},
                        ],
                    },
                },
            },
            {
                "name": "visits",
                "type": {"default": [], "type": "array", "items": "null"},
            },
        ],
    }
)


def test_merge_schemas_with_nested_records_with_default_null():
    schema1 = MergeableAvroSchema(
        schema_dict={
            "fields": [
                {
                    "name": "other_charges",
                    "type": {
                        "items": {
                            "fields": [
                                {"default": None, "name": "applies_to", "type": "null"},
                                {"name": "charge_index", "type": "long"},
                                {
                                    "name": "other_charge",
                                    "type": ["null", "string"],
                                },
                            ],
                            "name": "jobs_other_charges_item",
                            "type": "record",
                        },
                        "type": "array",
                    },
                }
            ],
            "name": "jobs",
            "type": "record",
        }
    )
    schema_2 = MergeableAvroSchema(
        schema_dict={
            "fields": [
                {
                    "name": "other_charges",
                    "type": {
                        "items": {
                            "fields": [
                                {"default": None, "name": "applies_to", "type": "null"},
                                {"name": "charge_index", "type": "long"},
                                {
                                    "name": "other_charge",
                                    "type": ["null", "string"],
                                    "default": None
                                },
                            ],
                            "name": "jobs_other_charges_item",
                            "type": "record",
                        },
                        "type": "array",
                    },
                }
            ],
            "name": "jobs",
            "type": "record",
        }
    )
    actual = schema1 + schema_2
    expected = MergeableAvroSchema(
        schema_dict={
            "fields": [
                {
                    "name": "other_charges",
                    "type": {
                        "items": {
                            "fields": [
                                {"default": None, "name": "applies_to", "type": "null"},
                                {"name": "charge_index", "type": "long"},
                                {
                                    "default": None,
                                    "name": "other_charge",
                                    "type": ["null", "string"],
                                },
                            ],
                            "name": "jobs_other_charges_item",
                            "type": "record",
                        },
                        "type": "array",
                    },
                }
            ],
            "name": "jobs",
            "type": "record",
        }
    )

    assert actual == expected


def test_merge_schemas_with_nested_records_with_default_null_reverse_order():
    schema1 = MergeableAvroSchema(
        schema_dict={
            "fields": [
                {
                    "name": "other_charges",
                    "type": {
                        "items": {
                            "fields": [
                                {"default": None, "name": "applies_to", "type": "null"},
                                {"name": "charge_index", "type": "long"},
                                {
                                    "default": None,
                                    "name": "other_charge",
                                    "type": ["null", "string"],
                                },
                            ],
                            "name": "jobs_other_charges_item",
                            "type": "record",
                        },
                        "type": "array",
                    },
                }
            ],
            "name": "jobs",
            "type": "record",
        }
    )
    schema_2 = MergeableAvroSchema(
        schema_dict={
            "fields": [
                {
                    "name": "other_charges",
                    "type": {
                        "items": {
                            "fields": [
                                {"default": None, "name": "applies_to", "type": "null"},
                                {"name": "charge_index", "type": "long"},
                                {
                                    "name": "other_charge",
                                    "type": ["null", "string"],
                                },
                            ],
                            "name": "jobs_other_charges_item",
                            "type": "record",
                        },
                        "type": "array",
                    },
                }
            ],
            "name": "jobs",
            "type": "record",
        }
    )
    actual = schema1 + schema_2
    expected = MergeableAvroSchema(
        schema_dict={
            "fields": [
                {
                    "name": "other_charges",
                    "type": {
                        "items": {
                            "fields": [
                                {"default": None, "name": "applies_to", "type": "null"},
                                {"name": "charge_index", "type": "long"},
                                {
                                    "default": None,
                                    "name": "other_charge",
                                    "type": ["null", "string"],
                                },
                            ],
                            "name": "jobs_other_charges_item",
                            "type": "record",
                        },
                        "type": "array",
                    },
                }
            ],
            "name": "jobs",
            "type": "record",
        }
    )

    assert actual == expected
