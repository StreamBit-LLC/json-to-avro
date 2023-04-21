import typing
from unittest.mock import patch

import pytest
from schema_registry.client import AsyncSchemaRegistryClient, SchemaRegistryClient
from schema_registry.client.schema import AvroSchema
from schema_registry.client.utils import SchemaVersion

from json_to_avro.avro_schema import RegisteredAvroSchema


class SchemaMetadata(typing.TypedDict):
    subject: str
    schema_id: int
    schema_version: int | str


@pytest.fixture
def schema_metadata_jobs_test() -> SchemaMetadata:
    return {
        "subject": "jobs-test-value",
        "schema_id": 7,
        "schema_version": 1,
    }


@pytest.fixture
def schema_version_jobs_test(schema_metadata_jobs_test) -> SchemaVersion:
    return SchemaVersion(
        subject=schema_metadata_jobs_test["subject"],
        schema_id=schema_metadata_jobs_test["schema_id"],
        schema=AvroSchema(
            schema={
                "type": "record",
                "name": "jobs",
                "fields": [
                    {"name": "id", "type": "long"},
                    {"name": "number", "type": "string"},
                    {"name": "check_number", "type": "null", "default": None},
                    {"name": "priority", "type": "string"},
                    {"name": "description", "type": "string"},
                    {"name": "tech_notes", "type": "string"},
                    {"name": "completion_notes", "type": "null", "default": None},
                    {"name": "payment_status", "type": "string"},
                    {"name": "taxes_fees_total", "type": "double"},
                    {"name": "drive_labor_total", "type": "long"},
                    {"name": "billable_expenses_total", "type": "long"},
                    {"name": "total", "type": "double"},
                    {"name": "payments_deposits_total", "type": "long"},
                    {"name": "due_total", "type": "double"},
                    {"name": "cost_total", "type": "long"},
                    {"name": "duration", "type": "long"},
                    {"name": "time_frame_promised_start", "type": "string"},
                    {
                        "name": "time_frame_promised_end",
                        "type": "null",
                        "default": None,
                    },
                    {"name": "start_date", "type": "string"},
                    {"name": "end_date", "type": "null", "default": None},
                    {"name": "created_at", "type": "string"},
                    {"name": "updated_at", "type": "string"},
                    {"name": "closed_at", "type": "null", "default": None},
                    {"name": "customer_id", "type": "long"},
                    {"name": "customer_name", "type": "string"},
                    {"name": "parent_customer", "type": "null", "default": None},
                    {"name": "status", "type": "string"},
                    {"name": "sub_status", "type": "null", "default": None},
                    {"name": "contact_first_name", "type": "string"},
                    {"name": "contact_last_name", "type": "string"},
                    {"name": "street_1", "type": "string"},
                    {"name": "street_2", "type": "string"},
                    {"name": "city", "type": "string"},
                    {"name": "state_prov", "type": "string"},
                    {"name": "postal_code", "type": "string"},
                    {"name": "location_name", "type": "null", "default": None},
                    {"name": "is_gated", "type": "boolean"},
                    {"name": "gate_instructions", "type": "null", "default": None},
                    {"name": "category", "type": "string"},
                    {"name": "source", "type": "string"},
                    {"name": "payment_type", "type": "string"},
                    {"name": "customer_payment_terms", "type": "string"},
                    {"name": "project", "type": "null", "default": None},
                    {"name": "phase", "type": "null", "default": None},
                    {"name": "po_number", "type": "null", "default": None},
                    {"name": "contract", "type": "null", "default": None},
                    {"name": "note_to_customer", "type": "null", "default": None},
                    {"name": "called_in_by", "type": "null", "default": None},
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
                        "type": {
                            "type": "array",
                            "items": {
                                "name": "custom_fields_item",
                                "type": "record",
                                "fields": [
                                    {"name": "created_at", "type": "string"},
                                    {"name": "group", "type": "string"},
                                    {"name": "is_required", "type": "boolean"},
                                    {"name": "name", "type": "string"},
                                    {"name": "type", "type": "string"},
                                    {"name": "updated_at", "type": "string"},
                                    {
                                        "name": "value",
                                        "type": ["null", "boolean", "string"],
                                        "default": None,
                                    },
                                ],
                            },
                        },
                    },
                    {
                        "name": "pictures",
                        "type": {"type": "array", "items": "null", "default": []},
                    },
                    {
                        "name": "documents",
                        "type": {"type": "array", "items": "null", "default": []},
                    },
                    {
                        "name": "equipment",
                        "type": {"type": "array", "items": "null", "default": []},
                    },
                    {
                        "name": "techs_assigned",
                        "type": {
                            "type": "array",
                            "items": {
                                "type": "record",
                                "name": "techs_assigned_item",
                                "fields": [
                                    {"name": "id", "type": "long"},
                                    {"name": "first_name", "type": "string"},
                                    {"name": "last_name", "type": "string"},
                                ],
                            },
                        },
                    },
                    {
                        "name": "tasks",
                        "type": {"type": "array", "items": "null", "default": []},
                    },
                    {
                        "name": "notes",
                        "type": {"type": "array", "items": "null", "default": []},
                    },
                    {
                        "name": "products",
                        "type": {"type": "array", "items": "null", "default": []},
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
                                    {"name": "tax", "type": "string"},
                                    {"name": "service", "type": "string"},
                                    {"name": "service_list_id", "type": "long"},
                                    {"name": "service_rate_id", "type": "long"},
                                    {"name": "pattern_row_id", "type": "long"},
                                    {
                                        "name": "qbo_class_id",
                                        "type": "null",
                                        "default": None,
                                    },
                                    {
                                        "name": "qbd_class_id",
                                        "type": "null",
                                        "default": None,
                                    },
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
                                    {"name": "rate", "type": "double"},
                                    {"name": "total", "type": "double"},
                                    {"name": "charge_index", "type": "long"},
                                    {"name": "parent_index", "type": "long"},
                                    {"name": "is_percentage", "type": "boolean"},
                                    {"name": "is_discount", "type": "boolean"},
                                    {"name": "created_at", "type": "string"},
                                    {"name": "updated_at", "type": "string"},
                                    {"name": "other_charge", "type": "string"},
                                    {
                                        "name": "applies_to",
                                        "type": "null",
                                        "default": None,
                                    },
                                    {
                                        "name": "service_list_id",
                                        "type": "null",
                                        "default": None,
                                    },
                                    {"name": "other_charge_id", "type": "long"},
                                    {"name": "pattern_row_id", "type": "long"},
                                    {
                                        "name": "qbo_class_id",
                                        "type": "null",
                                        "default": None,
                                    },
                                    {
                                        "name": "qbd_class_id",
                                        "type": "null",
                                        "default": None,
                                    },
                                ],
                            },
                        },
                    },
                    {
                        "name": "labor_charges",
                        "type": {"type": "array", "items": "null", "default": []},
                    },
                    {
                        "name": "expenses",
                        "type": {"type": "array", "items": "null", "default": []},
                    },
                    {
                        "name": "payments",
                        "type": {"type": "array", "items": "null", "default": []},
                    },
                    {
                        "name": "invoices",
                        "type": {"type": "array", "items": "null", "default": []},
                    },
                    {
                        "name": "signatures",
                        "type": {"type": "array", "items": "null", "default": []},
                    },
                    {
                        "name": "printable_work_order",
                        "type": {
                            "type": "array",
                            "items": {
                                "name": "printable_work_order_item",
                                "type": "record",
                                "fields": [
                                    {"name": "name", "type": "string"},
                                    {"name": "url", "type": "string"},
                                ],
                            },
                        },
                    },
                    {
                        "name": "visits",
                        "type": {"type": "array", "items": "null", "default": []},
                    },
                ],
            }
        ),
        version=schema_metadata_jobs_test["schema_version"],
    )


@pytest.fixture
def schema_metadata() -> SchemaMetadata:
    return {
        "subject": "service-fusion-api-v1-jobs-value",
        "schema_id": 6,
        "schema_version": 1,
    }


@pytest.fixture
def schema_version(schema_metadata: SchemaMetadata) -> SchemaVersion:
    return SchemaVersion(
        subject=schema_metadata["subject"],
        schema_id=schema_metadata["schema_id"],
        schema=AvroSchema(
            schema={
                "type": "record",
                "name": "jobs",
                "fields": [
                    {"name": "id", "type": "long"},
                    {"default": None, "name": "city", "type": ["null", "string"]},
                    {"default": None, "name": "phase", "type": ["null", "string"]},
                    {"name": "total", "type": ["long", "double"]},
                    {"name": "number", "type": "string"},
                    {"default": None, "name": "source", "type": ["null", "string"]},
                    {"name": "status", "type": "string"},
                    {"default": None, "name": "project", "type": ["null", "string"]},
                    {"default": None, "name": "category", "type": ["null", "string"]},
                    {"default": None, "name": "contract", "type": ["null", "string"]},
                    {"name": "duration", "type": "long"},
                    {"default": None, "name": "end_date", "type": ["null", "string"]},
                    {"name": "is_gated", "type": "boolean"},
                    {"name": "priority", "type": "string"},
                    {"default": None, "name": "street_1", "type": ["null", "string"]},
                    {"default": None, "name": "street_2", "type": ["null", "string"]},
                    {"default": None, "name": "closed_at", "type": ["null", "string"]},
                    {"name": "due_total", "type": ["long", "double"]},
                    {"default": None, "name": "po_number", "type": ["null", "string"]},
                    {"name": "cost_total", "type": ["long", "double"]},
                    {"default": None, "name": "created_at", "type": ["null", "string"]},
                    {"default": None, "name": "start_date", "type": ["null", "string"]},
                    {"default": None, "name": "state_prov", "type": ["null", "string"]},
                    {"default": None, "name": "sub_status", "type": ["null", "string"]},
                    {"default": None, "name": "tech_notes", "type": ["null", "string"]},
                    {"name": "updated_at", "type": "string"},
                    {"default": None, "name": "customer_id", "type": ["null", "long"]},
                    {
                        "default": None,
                        "name": "description",
                        "type": ["null", "string"],
                    },
                    {
                        "default": None,
                        "name": "postal_code",
                        "type": ["null", "string"],
                    },
                    {
                        "default": None,
                        "name": "called_in_by",
                        "type": ["null", "string"],
                    },
                    {
                        "default": None,
                        "name": "check_number",
                        "type": ["null", "string"],
                    },
                    {
                        "default": None,
                        "name": "payment_type",
                        "type": ["null", "string"],
                    },
                    {
                        "default": None,
                        "name": "customer_name",
                        "type": ["null", "string"],
                    },
                    {
                        "default": None,
                        "name": "location_name",
                        "type": ["null", "string"],
                    },
                    {"name": "payment_status", "type": "string"},
                    {
                        "default": None,
                        "name": "parent_customer",
                        "type": ["null", "string"],
                    },
                    {
                        "default": None,
                        "name": "completion_notes",
                        "type": ["null", "string"],
                    },
                    {
                        "default": None,
                        "name": "note_to_customer",
                        "type": ["null", "string"],
                    },
                    {"name": "taxes_fees_total", "type": ["long", "double"]},
                    {
                        "default": None,
                        "name": "contact_last_name",
                        "type": ["null", "string"],
                    },
                    {"name": "drive_labor_total", "type": "long"},
                    {
                        "default": None,
                        "name": "gate_instructions",
                        "type": ["null", "string"],
                    },
                    {
                        "default": None,
                        "name": "contact_first_name",
                        "type": ["null", "string"],
                    },
                    {"name": "is_requires_follow_up", "type": "boolean"},
                    {
                        "default": None,
                        "name": "customer_payment_terms",
                        "type": ["null", "string"],
                    },
                    {"name": "billable_expenses_total", "type": "long"},
                    {"name": "payments_deposits_total", "type": ["long", "double"]},
                    {
                        "default": None,
                        "name": "time_frame_promised_end",
                        "type": ["null", "string"],
                    },
                    {
                        "default": None,
                        "name": "time_frame_promised_start",
                        "type": ["null", "string"],
                    },
                    {
                        "default": None,
                        "name": "services",
                        "type": [
                            "null",
                            {
                                "type": "array",
                                "items": {
                                    "type": "record",
                                    "name": "Service",
                                    "fields": [
                                        {"name": "name", "type": "string"},
                                        {
                                            "default": None,
                                            "name": "description",
                                            "type": ["null", "string"],
                                        },
                                        {
                                            "default": None,
                                            "name": "multiplier",
                                            "type": ["null", "long", "double"],
                                        },
                                        {
                                            "default": None,
                                            "name": "rate",
                                            "type": ["null", "long", "double"],
                                        },
                                        {
                                            "default": None,
                                            "name": "total",
                                            "type": ["null", "long", "double"],
                                        },
                                        {
                                            "default": None,
                                            "name": "cost",
                                            "type": ["null", "long", "double"],
                                        },
                                        {
                                            "default": None,
                                            "name": "actual_cost",
                                            "type": ["null", "long", "double"],
                                        },
                                        {
                                            "default": None,
                                            "name": "item_index",
                                            "type": ["null", "long", "double"],
                                        },
                                        {
                                            "default": None,
                                            "name": "parent_index",
                                            "type": ["null", "long", "double"],
                                        },
                                        {
                                            "default": None,
                                            "name": "created_at",
                                            "type": ["null", "string"],
                                        },
                                        {
                                            "default": None,
                                            "name": "updated_at",
                                            "type": ["null", "string"],
                                        },
                                        {
                                            "name": "is_show_rate_items",
                                            "type": "boolean",
                                        },
                                        {
                                            "default": None,
                                            "name": "tax",
                                            "type": ["null", "string"],
                                        },
                                        {"name": "service", "type": "string"},
                                        {"name": "service_list_id", "type": "long"},
                                        {"name": "service_rate_id", "type": "long"},
                                        {
                                            "default": None,
                                            "name": "pattern_row_id",
                                            "type": ["null", "string"],
                                        },
                                        {
                                            "default": None,
                                            "name": "qbo_class_id",
                                            "type": ["null", "string"],
                                        },
                                        {
                                            "default": None,
                                            "name": "qbd_class_id",
                                            "type": ["null", "string"],
                                        },
                                    ],
                                },
                            },
                        ],
                    },
                    {
                        "default": None,
                        "name": "printable_work_order",
                        "type": [
                            "null",
                            {
                                "type": "array",
                                "items": {
                                    "type": "record",
                                    "name": "PrintableWorkOrderItem",
                                    "fields": [
                                        {
                                            "default": None,
                                            "name": "name",
                                            "type": ["null", "string"],
                                        },
                                        {
                                            "default": None,
                                            "name": "url",
                                            "type": ["null", "string"],
                                        },
                                    ],
                                },
                            },
                        ],
                    },
                ],
                "__fastavro_parsed": True,
                "__named_schemas": {
                    "jobs": {
                        "type": "record",
                        "name": "jobs",
                        "fields": [
                            {"name": "id", "type": "long"},
                            {
                                "default": None,
                                "name": "city",
                                "type": ["null", "string"],
                            },
                            {
                                "default": None,
                                "name": "phase",
                                "type": ["null", "string"],
                            },
                            {"name": "total", "type": ["long", "double"]},
                            {"name": "number", "type": "string"},
                            {
                                "default": None,
                                "name": "source",
                                "type": ["null", "string"],
                            },
                            {"name": "status", "type": "string"},
                            {
                                "default": None,
                                "name": "project",
                                "type": ["null", "string"],
                            },
                            {
                                "default": None,
                                "name": "category",
                                "type": ["null", "string"],
                            },
                            {
                                "default": None,
                                "name": "contract",
                                "type": ["null", "string"],
                            },
                            {"name": "duration", "type": "long"},
                            {
                                "default": None,
                                "name": "end_date",
                                "type": ["null", "string"],
                            },
                            {"name": "is_gated", "type": "boolean"},
                            {"name": "priority", "type": "string"},
                            {
                                "default": None,
                                "name": "street_1",
                                "type": ["null", "string"],
                            },
                            {
                                "default": None,
                                "name": "street_2",
                                "type": ["null", "string"],
                            },
                            {
                                "default": None,
                                "name": "closed_at",
                                "type": ["null", "string"],
                            },
                            {"name": "due_total", "type": ["long", "double"]},
                            {
                                "default": None,
                                "name": "po_number",
                                "type": ["null", "string"],
                            },
                            {"name": "cost_total", "type": ["long", "double"]},
                            {
                                "default": None,
                                "name": "created_at",
                                "type": ["null", "string"],
                            },
                            {
                                "default": None,
                                "name": "start_date",
                                "type": ["null", "string"],
                            },
                            {
                                "default": None,
                                "name": "state_prov",
                                "type": ["null", "string"],
                            },
                            {
                                "default": None,
                                "name": "sub_status",
                                "type": ["null", "string"],
                            },
                            {
                                "default": None,
                                "name": "tech_notes",
                                "type": ["null", "string"],
                            },
                            {"name": "updated_at", "type": "string"},
                            {
                                "default": None,
                                "name": "customer_id",
                                "type": ["null", "long"],
                            },
                            {
                                "default": None,
                                "name": "description",
                                "type": ["null", "string"],
                            },
                            {
                                "default": None,
                                "name": "postal_code",
                                "type": ["null", "string"],
                            },
                            {
                                "default": None,
                                "name": "called_in_by",
                                "type": ["null", "string"],
                            },
                            {
                                "default": None,
                                "name": "check_number",
                                "type": ["null", "string"],
                            },
                            {
                                "default": None,
                                "name": "payment_type",
                                "type": ["null", "string"],
                            },
                            {
                                "default": None,
                                "name": "customer_name",
                                "type": ["null", "string"],
                            },
                            {
                                "default": None,
                                "name": "location_name",
                                "type": ["null", "string"],
                            },
                            {"name": "payment_status", "type": "string"},
                            {
                                "default": None,
                                "name": "parent_customer",
                                "type": ["null", "string"],
                            },
                            {
                                "default": None,
                                "name": "completion_notes",
                                "type": ["null", "string"],
                            },
                            {
                                "default": None,
                                "name": "note_to_customer",
                                "type": ["null", "string"],
                            },
                            {"name": "taxes_fees_total", "type": ["long", "double"]},
                            {
                                "default": None,
                                "name": "contact_last_name",
                                "type": ["null", "string"],
                            },
                            {"name": "drive_labor_total", "type": "long"},
                            {
                                "default": None,
                                "name": "gate_instructions",
                                "type": ["null", "string"],
                            },
                            {
                                "default": None,
                                "name": "contact_first_name",
                                "type": ["null", "string"],
                            },
                            {"name": "is_requires_follow_up", "type": "boolean"},
                            {
                                "default": None,
                                "name": "customer_payment_terms",
                                "type": ["null", "string"],
                            },
                            {"name": "billable_expenses_total", "type": "long"},
                            {
                                "name": "payments_deposits_total",
                                "type": ["long", "double"],
                            },
                            {
                                "default": None,
                                "name": "time_frame_promised_end",
                                "type": ["null", "string"],
                            },
                            {
                                "default": None,
                                "name": "time_frame_promised_start",
                                "type": ["null", "string"],
                            },
                            {
                                "default": None,
                                "name": "services",
                                "type": [
                                    "null",
                                    {
                                        "type": "array",
                                        "items": {
                                            "type": "record",
                                            "name": "Service",
                                            "fields": [
                                                {"name": "name", "type": "string"},
                                                {
                                                    "default": None,
                                                    "name": "description",
                                                    "type": ["null", "string"],
                                                },
                                                {
                                                    "default": None,
                                                    "name": "multiplier",
                                                    "type": ["null", "long", "double"],
                                                },
                                                {
                                                    "default": None,
                                                    "name": "rate",
                                                    "type": ["null", "long", "double"],
                                                },
                                                {
                                                    "default": None,
                                                    "name": "total",
                                                    "type": ["null", "long", "double"],
                                                },
                                                {
                                                    "default": None,
                                                    "name": "cost",
                                                    "type": ["null", "long", "double"],
                                                },
                                                {
                                                    "default": None,
                                                    "name": "actual_cost",
                                                    "type": ["null", "long", "double"],
                                                },
                                                {
                                                    "default": None,
                                                    "name": "item_index",
                                                    "type": ["null", "long", "double"],
                                                },
                                                {
                                                    "default": None,
                                                    "name": "parent_index",
                                                    "type": ["null", "long", "double"],
                                                },
                                                {
                                                    "default": None,
                                                    "name": "created_at",
                                                    "type": ["null", "string"],
                                                },
                                                {
                                                    "default": None,
                                                    "name": "updated_at",
                                                    "type": ["null", "string"],
                                                },
                                                {
                                                    "name": "is_show_rate_items",
                                                    "type": "boolean",
                                                },
                                                {
                                                    "default": None,
                                                    "name": "tax",
                                                    "type": ["null", "string"],
                                                },
                                                {"name": "service", "type": "string"},
                                                {
                                                    "name": "service_list_id",
                                                    "type": "long",
                                                },
                                                {
                                                    "name": "service_rate_id",
                                                    "type": "long",
                                                },
                                                {
                                                    "default": None,
                                                    "name": "pattern_row_id",
                                                    "type": ["null", "string"],
                                                },
                                                {
                                                    "default": None,
                                                    "name": "qbo_class_id",
                                                    "type": ["null", "string"],
                                                },
                                                {
                                                    "default": None,
                                                    "name": "qbd_class_id",
                                                    "type": ["null", "string"],
                                                },
                                            ],
                                        },
                                    },
                                ],
                            },
                            {
                                "default": None,
                                "name": "printable_work_order",
                                "type": [
                                    "null",
                                    {
                                        "type": "array",
                                        "items": {
                                            "type": "record",
                                            "name": "PrintableWorkOrderItem",
                                            "fields": [
                                                {
                                                    "default": None,
                                                    "name": "name",
                                                    "type": ["null", "string"],
                                                },
                                                {
                                                    "default": None,
                                                    "name": "url",
                                                    "type": ["null", "string"],
                                                },
                                            ],
                                        },
                                    },
                                ],
                            },
                        ],
                    },
                    "Service": {
                        "type": "record",
                        "name": "Service",
                        "fields": [
                            {"name": "name", "type": "string"},
                            {
                                "default": None,
                                "name": "description",
                                "type": ["null", "string"],
                            },
                            {
                                "default": None,
                                "name": "multiplier",
                                "type": ["null", "long", "double"],
                            },
                            {
                                "default": None,
                                "name": "rate",
                                "type": ["null", "long", "double"],
                            },
                            {
                                "default": None,
                                "name": "total",
                                "type": ["null", "long", "double"],
                            },
                            {
                                "default": None,
                                "name": "cost",
                                "type": ["null", "long", "double"],
                            },
                            {
                                "default": None,
                                "name": "actual_cost",
                                "type": ["null", "long", "double"],
                            },
                            {
                                "default": None,
                                "name": "item_index",
                                "type": ["null", "long", "double"],
                            },
                            {
                                "default": None,
                                "name": "parent_index",
                                "type": ["null", "long", "double"],
                            },
                            {
                                "default": None,
                                "name": "created_at",
                                "type": ["null", "string"],
                            },
                            {
                                "default": None,
                                "name": "updated_at",
                                "type": ["null", "string"],
                            },
                            {"name": "is_show_rate_items", "type": "boolean"},
                            {
                                "default": None,
                                "name": "tax",
                                "type": ["null", "string"],
                            },
                            {"name": "service", "type": "string"},
                            {"name": "service_list_id", "type": "long"},
                            {"name": "service_rate_id", "type": "long"},
                            {
                                "default": None,
                                "name": "pattern_row_id",
                                "type": ["null", "string"],
                            },
                            {
                                "default": None,
                                "name": "qbo_class_id",
                                "type": ["null", "string"],
                            },
                            {
                                "default": None,
                                "name": "qbd_class_id",
                                "type": ["null", "string"],
                            },
                        ],
                    },
                    "PrintableWorkOrderItem": {
                        "type": "record",
                        "name": "PrintableWorkOrderItem",
                        "fields": [
                            {
                                "default": None,
                                "name": "name",
                                "type": ["null", "string"],
                            },
                            {
                                "default": None,
                                "name": "url",
                                "type": ["null", "string"],
                            },
                        ],
                    },
                },
            }
        ),
        version=schema_metadata["schema_version"],
    )


@pytest.fixture
def schema_registry_client_with_jobs_cached_schema(
    schema_version_jobs_test,
) -> SchemaRegistryClient:
    client = SchemaRegistryClient(url="http://localhost:8081")
    client._cache_schema(
        schema_version_jobs_test.schema,
        schema_version_jobs_test.schema_id,
        schema_version_jobs_test.subject,
        schema_version_jobs_test.version,
    )
    return client


@pytest.fixture
def schema_registry_client_with_cached_schema(
    schema_version: SchemaVersion,
) -> AsyncSchemaRegistryClient:
    client = AsyncSchemaRegistryClient(url="http://localhost:8081")
    client._cache_schema(
        schema_version.schema,
        schema_version.schema_id,
        schema_version.subject,
        schema_version.version,
    )
    return client


@pytest.fixture
def schema_registry_client_with_patched_request_method_for_get_schema(
    schema_version: SchemaVersion,
) -> AsyncSchemaRegistryClient:
    client = AsyncSchemaRegistryClient(url="http://localhost:8081")

    async def mock_get_subject(
        *_: tuple[typing.Any], **__: dict[str, typing.Any]
    ) -> SchemaVersion:
        return schema_version

    with patch.object(client, "get_schema", mock_get_subject):
        yield client


@pytest.fixture
def schema_registry_client_with_patched_request_method_for_get_schema_not_found(
    schema_version: SchemaVersion,
) -> AsyncSchemaRegistryClient:
    client = AsyncSchemaRegistryClient(url="http://localhost:8081")

    async def mock_get_schema(
        *_: tuple[typing.Any], **__: dict[str, typing.Any]
    ) -> typing.Optional[SchemaVersion]:
        return None

    with patch.object(client, "get_schema", mock_get_schema):
        yield client


@pytest.fixture
def schema_registry_client_with_patched_request_method_register(
    schema_version: SchemaVersion,
) -> AsyncSchemaRegistryClient:
    client = AsyncSchemaRegistryClient(url="http://localhost:8081")

    async def mock_register(
        *_: tuple[typing.Any], **__: dict[str, typing.Any]
    ) -> typing.Optional[SchemaVersion]:
        return schema_version.schema_id

    with patch.object(client, "register", mock_register):
        yield client


@pytest.fixture
def current_schema_table_with_cached_schema(
    schema_version: SchemaVersion,
) -> typing.MutableMapping[str, RegisteredAvroSchema]:
    current_schema_table = dict()
    current_schema_table[
        schema_version.subject
    ] = RegisteredAvroSchema.from_schema_version(schema_version)
    return current_schema_table


@pytest.fixture
def airbyte_msg() -> dict:
    return dict(
        _airbyte_ab_id="abc123",
        _airbyte_stream="jobs",
        _airbyte_emitted_at=123456789,
        _airbyte_data={
            "id": 1,
            "number": "123",
            "check_number": None,
            "priority": "high",
            "printable_work_order": [
                {
                    "name": "Print With Rates",
                    "url": "https://admin.servicefusion.com/printJobWithRates?jobId=Sz6bz0GNuv5gIaLQ1mJeXeLorFxh_Uo0TYghWKC5mas",
                },
                {
                    "name": "Print Without Rates",
                    "url": "https://admin.servicefusion.com/printJobWithoutRates?jobId=Sz6bz0GNuv5gIaLQ1mJeXeLorFxh_Uo0TYghWKC5mas",
                },
                {
                    "name": "Download As Pdf",
                    "url": "https://admin.servicefusion.com/downloadJobAsPdf?jobId=Sz6bz0GNuv5gIaLQ1mJeXeLorFxh_Uo0TYghWKC5mas",
                },
                {
                    "name": "Download As Excel",
                    "url": "https://admin.servicefusion.com/downloadJobAsExcel?jobId=Sz6bz0GNuv5gIaLQ1mJeXeLorFxh_Uo0TYghWKC5mas",
                },
            ],
        },
    )


@pytest.fixture
def airbyte_msg_data_with_custom_fields():
    return {
        "id": 1015625811,
        "number": "1015625811",
        "check_number": None,
        "priority": "Normal",
        "description": "Window Cleaning: Exterior",
        "tech_notes": "APOLLO BEACH\r\nOUT ONLY $522\r\n2hr 15 min w/ 3\n",
        "completion_notes": None,
        "payment_status": "Paid in Full",
        "taxes_fees_total": 522,
        "drive_labor_total": 0,
        "billable_expenses_total": 0,
        "total": 522,
        "payments_deposits_total": 522,
        "due_total": 0,
        "cost_total": 0,
        "duration": 8100,
        "time_frame_promised_start": "11:00",
        "time_frame_promised_end": "11:30",
        "start_date": "2023-03-10",
        "end_date": None,
        "created_at": "2023-03-09T15:25:49+00:00",
        "updated_at": "2023-03-11T15:43:41+00:00",
        "closed_at": "2023-03-10T12:35:22+00:00",
        "customer_id": 47203397,
        "customer_name": "Nathan Morris",
        "parent_customer": None,
        "status": "Invoiced",
        "sub_status": None,
        "contact_first_name": "Nathan",
        "contact_last_name": "Morris",
        "street_1": "1425 Jumana Loop",
        "street_2": None,
        "city": "Apollo Beach",
        "state_prov": "FL",
        "postal_code": "33572",
        "location_name": "Primary Address",
        "is_gated": False,
        "gate_instructions": None,
        "category": "74E",
        "source": "74E",
        "payment_type": "Direct Bill",
        "customer_payment_terms": "Due Upon Receipt",
        "project": None,
        "phase": None,
        "po_number": None,
        "contract": None,
        "note_to_customer": "We uphold the highest industry standards for glass cleaning tools and methods but must inform and educate its customers about the inherent risk of scratches when cleaning glass. Given the facts below, we cannot be held liable for glass scratches. Minuscule glass particles (or “glass fines”) may exist on the pane surface. This flaw is common for tempered or hurricane-proof glass often installed in Florida. During a normal cleaning process, these glass fines can break off and cause hairline scratches. Removal of paint, adhesives, calcium deposits, or construction debris may require the use of scrubbing pads or scrapers, which increases the risk of scratched glass, and is a separate service from standard window cleaning. When cleaning glass to remove calcium deposits, some brands of tinted or soft glass may be micro-scratched with vinyl buffing pads. Pre-existing scratches may be visible or apparent after the glass is cleaned.  \r\n\r\nTerms of payment: The total amount stated is due upon completion. Where applicable, credit cards will be charged for the total amount upon completion based on the credit card information provided in advance. All late payments (over 30 days) may bear interest at the highest rate permissible under Florida law calculated daily and compounded monthly. Customer shall also be responsible for paying all reasonable costs incurred in collecting any late payments, including, without limitation, attorneys’ fees.",
        "called_in_by": None,
        "is_requires_follow_up": False,
        "agents": [],
        "custom_fields": [
            {
                "name": "FW Division",
                "value": None,
                "type": "select",
                "group": "Default",
                "created_at": "2023-03-09T15:25:49+00:00",
                "updated_at": "2023-03-09T15:25:49+00:00",
                "is_required": False,
            },
            {
                "name": "Man Hours Budgeted",
                "value": 0,
                "type": "numericinput",
                "group": "Default",
                "created_at": "2023-03-09T15:25:49+00:00",
                "updated_at": "2023-03-09T15:25:49+00:00",
                "is_required": False,
            },
            {
                "name": "Total Man Hours Recorded",
                "value": 0,
                "type": "numericinput",
                "group": "Default",
                "created_at": "2023-03-09T15:25:49+00:00",
                "updated_at": "2023-03-09T15:25:49+00:00",
                "is_required": False,
            },
            {
                "name": "Store #",
                "value": None,
                "type": "text",
                "group": "Default",
                "created_at": "2023-03-09T15:25:49+00:00",
                "updated_at": "2023-03-09T15:25:49+00:00",
                "is_required": False,
            },
            {
                "name": "Man Hours Spent To Date",
                "value": 0,
                "type": "numericinput",
                "group": "Default",
                "created_at": "2023-03-09T15:25:49+00:00",
                "updated_at": "2023-03-09T15:25:49+00:00",
                "is_required": False,
            },
            {
                "name": "Certified Payroll Required?",
                "value": False,
                "type": "checkbox",
                "group": "Default",
                "created_at": "2023-03-09T15:25:49+00:00",
                "updated_at": "2023-03-09T15:25:49+00:00",
                "is_required": False,
            },
            {
                "name": "IVR PIN #",
                "value": None,
                "type": "text",
                "group": "Default",
                "created_at": "2023-03-09T15:25:49+00:00",
                "updated_at": "2023-03-09T15:25:49+00:00",
                "is_required": False,
            },
            {
                "name": "IVR ID #",
                "value": None,
                "type": "text",
                "group": "Default",
                "created_at": "2023-03-09T15:25:49+00:00",
                "updated_at": "2023-03-09T15:25:49+00:00",
                "is_required": False,
            },
            {
                "name": "Route Day",
                "value": None,
                "type": "select",
                "group": "Default",
                "created_at": "2023-03-09T15:25:49+00:00",
                "updated_at": "2023-03-09T15:25:49+00:00",
                "is_required": False,
            },
            {
                "name": "Estimated Completion Date",
                "value": None,
                "type": "date",
                "group": "Default",
                "created_at": "2023-03-09T15:25:49+00:00",
                "updated_at": "2023-03-09T15:25:49+00:00",
                "is_required": False,
            },
            {
                "name": "ZohoSyncDate",
                "value": None,
                "type": "date",
                "group": "Default",
                "created_at": "2023-03-09T15:25:49+00:00",
                "updated_at": "2023-03-09T15:25:49+00:00",
                "is_required": False,
            },
        ],
        "pictures": [],
        "documents": [],
        "equipment": [],
        "techs_assigned": [
            {"id": 980456847, "first_name": "Shelby", "last_name": "Walker"}
        ],
        "tasks": [],
        "notes": [],
        "products": [],
        "services": [
            {
                "name": "Window Cleaning (Exterior Only)",
                "description": "Clean Exterior Windows ONLY to remove organic build-up. Includes: All window glass, frames, sills, and screens",
                "multiplier": 1,
                "rate": 522,
                "total": 522,
                "cost": 0,
                "actual_cost": 0,
                "item_index": 3,
                "parent_index": 0,
                "created_at": "2023-03-09T15:25:49+00:00",
                "updated_at": "2023-03-09T15:25:49+00:00",
                "is_show_rate_items": False,
                "tax": None,
                "service": "Window Cleaning - Ext. Only (E)",
                "service_list_id": 106831485,
                "service_rate_id": 8421737,
                "pattern_row_id": None,
                "qbo_class_id": None,
                "qbd_class_id": None,
            }
        ],
        "other_charges": [],
        "labor_charges": [
            {
                "drive_time": 0,
                "drive_time_rate": 16,
                "drive_time_cost": 0,
                "drive_time_start": None,
                "drive_time_end": None,
                "is_drive_time_billed": False,
                "labor_time": 133,
                "labor_time_rate": 16,
                "labor_time_cost": 0,
                "labor_time_start": "10:21",
                "labor_time_end": "12:35",
                "labor_date": "2023-03-10",
                "is_labor_time_billed": False,
                "total": 0,
                "created_at": "2023-03-10T10:21:31+00:00",
                "updated_at": "2023-03-10T12:35:21+00:00",
                "is_status_generated": True,
                "user": "Shelby Walker",
                "visit_id": None,
                "qbo_class_id": None,
                "qbd_class_id": None,
            }
        ],
        "expenses": [
            {
                "purchased_from": None,
                "notes": None,
                "amount": 0,
                "is_billable": False,
                "date": None,
                "created_at": "2022-12-16T11:58:00+00:00",
                "updated_at": None,
                "user": None,
                "category": None,
                "qbo_class_id": None,
                "qbd_class_id": None,
            },
            {
                "purchased_from": None,
                "notes": None,
                "amount": 0,
                "is_billable": False,
                "date": None,
                "created_at": "2022-12-16T11:58:00+00:00",
                "updated_at": None,
                "user": None,
                "category": None,
                "qbo_class_id": None,
                "qbd_class_id": None,
            },
            {
                "purchased_from": None,
                "notes": None,
                "amount": 0,
                "is_billable": False,
                "date": None,
                "created_at": "2022-12-16T11:58:00+00:00",
                "updated_at": None,
                "user": None,
                "category": None,
                "qbo_class_id": None,
                "qbd_class_id": None,
            },
            {
                "purchased_from": None,
                "notes": None,
                "amount": 0,
                "is_billable": False,
                "date": None,
                "created_at": "2023-03-10T12:35:21+00:00",
                "updated_at": None,
                "user": None,
                "category": None,
                "qbo_class_id": None,
                "qbd_class_id": None,
            },
            {
                "purchased_from": None,
                "notes": None,
                "amount": 0,
                "is_billable": False,
                "date": None,
                "created_at": "2023-03-10T12:35:21+00:00",
                "updated_at": None,
                "user": None,
                "category": None,
                "qbo_class_id": None,
                "qbd_class_id": None,
            },
            {
                "purchased_from": None,
                "notes": None,
                "amount": 0,
                "is_billable": False,
                "date": None,
                "created_at": "2023-03-10T12:35:21+00:00",
                "updated_at": None,
                "user": None,
                "category": None,
                "qbo_class_id": None,
                "qbd_class_id": None,
            },
        ],
        "payments": [
            {
                "transaction_type": "AUTH_CAPTURE",
                "transaction_token": "4902516578",
                "transaction_id": None,
                "payment_transaction_id": 14126585,
                "original_transaction_id": None,
                "apply_to": "INV",
                "amount": 522,
                "memo": None,
                "authorization_code": "148433",
                "bill_to_street_address": "1425 Jumana Loop",
                "bill_to_postal_code": "33572",
                "bill_to_country": None,
                "reference_number": None,
                "is_resync_qbo": False,
                "created_at": "2023-03-11T15:43:41+00:00",
                "updated_at": "2023-03-11T15:43:41+00:00",
                "received_on": "2023-03-11T20:43:41+00:00",
                "qbo_synced_date": "2023-03-11T20:43:42+00:00",
                "qbo_id": 135818,
                "qbd_id": None,
                "customer": "Nathan Morris",
                "type": "AmEx 3767***1000",
                "invoice_id": 1006728897,
                "gateway_id": 980190962,
                "receipt_id": None,
            }
        ],
        "invoices": [
            {
                "id": 1006728897,
                "number": 7049395,
                "currency": "$",
                "po_number": None,
                "terms": "DUR",
                "customer_message": "Terms of payment: The total amount stated is due upon completion. Where applicable, credit cards will be charged for the total amount upon completion based on the credit card information provided in advance. All late payments (over 30 days) may bear interest at the highest rate permissible under Florida law calculated daily and compounded monthly. Customer shall also be responsible for paying all reasonable costs incurred in collecting any late payments, including, without limitation, attorneys’ fees.",
                "notes": "Window Cleaning: Exterior",
                "pay_online_url": "https://app.servicefusion.com/invoiceOnline?id=SsJrDJujHb_TsAMq6LTyZ1FxPj1kBXEAHXDdYZCL7vs&key=Dkba0q4R1tCGLPBck7OFLMxVvUQk7J7IEFnYzN48psg&templateId=XP7IYMcE0o6kqID9tTsa2-X2PEBqCtoct64_lkQkd2g",
                "qbo_invoice_no": 7049395,
                "qbo_sync_token": None,
                "qbo_synced_date": "2023-03-10T11:35:24+00:00",
                "qbo_id": 135755,
                "qbd_id": None,
                "total": 522,
                "is_paid": True,
                "date": "2023-03-10T12:35:21+00:00",
                "mail_send_date": "2023-03-10T11:35:45+00:00",
                "created_at": "2023-03-10T12:35:21+00:00",
                "updated_at": "2023-03-10T12:35:21+00:00",
                "customer": "Nathan Morris",
                "customer_contact": "Nathan Morris",
                "payment_terms": None,
                "bill_to_customer_id": None,
                "bill_to_customer_location_id": None,
                "bill_to_customer_contact_id": None,
                "bill_to_email_id": None,
                "bill_to_phone_id": None,
            }
        ],
        "signatures": [],
        "printable_work_order": [
            {
                "name": "Print With Rates",
                "url": "https://admin.servicefusion.com/printJobWithRates?jobId=u816VOxSnbE2uB-EYrCkgqFotbkpJVRt1x8MAMv-7Yw",
            },
            {
                "name": "Print Without Rates",
                "url": "https://admin.servicefusion.com/printJobWithoutRates?jobId=u816VOxSnbE2uB-EYrCkgqFotbkpJVRt1x8MAMv-7Yw",
            },
            {
                "name": "Download As Pdf",
                "url": "https://admin.servicefusion.com/downloadJobAsPdf?jobId=u816VOxSnbE2uB-EYrCkgqFotbkpJVRt1x8MAMv-7Yw",
            },
            {
                "name": "Download As Excel",
                "url": "https://admin.servicefusion.com/downloadJobAsExcel?jobId=u816VOxSnbE2uB-EYrCkgqFotbkpJVRt1x8MAMv-7Yw",
            },
        ],
        "visits": [],
    }
