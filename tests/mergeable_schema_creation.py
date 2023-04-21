import pytest
from schema_registry.client.utils import SchemaVersion

from airbyte_json_to_avro.airbyte_json_to_avro import (
    RegisteredAvroSchema,
    RegisteredAvroSchemaId,
    MergeableAvroSchema,
    AirbyteMessage,
    AvroSchemaCandidate,
    WrongSchemaConfigurationException,
)


def test_create_single_schema_based_on_array_items():
    avro_schema = MergeableAvroSchema.convert_json_to_avro_schema(
        {
            "custom_fields": [
                {
                    "created_at": "2023-03-09T15:25:49+00:00",
                    "group": "Default",
                    "is_required": False,
                    "name": "FW Division",
                    "type": "select",
                    "updated_at": "2023-03-09T15:25:49+00:00",
                    "value": None,
                },
                {
                    "created_at": "2023-03-09T15:25:49+00:00",
                    "group": "Default",
                    "is_required": False,
                    "name": "Man Hours Budgeted",
                    "type": "numericinput",
                    "updated_at": "2023-03-09T15:25:49+00:00",
                    "value": 0,
                },
                {
                    "created_at": "2023-03-09T15:25:49+00:00",
                    "group": "Default",
                    "is_required": False,
                    "name": "Total Man Hours Recorded",
                    "type": "numericinput",
                    "updated_at": "2023-03-09T15:25:49+00:00",
                    "value": 0,
                },
                {
                    "created_at": "2023-03-09T15:25:49+00:00",
                    "group": "Default",
                    "is_required": False,
                    "name": "Store #",
                    "type": "text",
                    "updated_at": "2023-03-09T15:25:49+00:00",
                    "value": None,
                },
                {
                    "created_at": "2023-03-09T15:25:49+00:00",
                    "group": "Default",
                    "is_required": False,
                    "name": "Man Hours Spent To Date",
                    "type": "numericinput",
                    "updated_at": "2023-03-09T15:25:49+00:00",
                    "value": 0,
                },
                {
                    "created_at": "2023-03-09T15:25:49+00:00",
                    "group": "Default",
                    "is_required": False,
                    "name": "Certified Payroll Required?",
                    "type": "checkbox",
                    "updated_at": "2023-03-09T15:25:49+00:00",
                    "value": False,
                },
                {
                    "created_at": "2023-03-09T15:25:49+00:00",
                    "group": "Default",
                    "is_required": False,
                    "name": "IVR PIN #",
                    "type": "text",
                    "updated_at": "2023-03-09T15:25:49+00:00",
                    "value": None,
                },
                {
                    "created_at": "2023-03-09T15:25:49+00:00",
                    "group": "Default",
                    "is_required": False,
                    "name": "IVR ID #",
                    "type": "text",
                    "updated_at": "2023-03-09T15:25:49+00:00",
                    "value": None,
                },
                {
                    "created_at": "2023-03-09T15:25:49+00:00",
                    "group": "Default",
                    "is_required": False,
                    "name": "Route Day",
                    "type": "select",
                    "updated_at": "2023-03-09T15:25:49+00:00",
                    "value": None,
                },
                {
                    "created_at": "2023-03-09T15:25:49+00:00",
                    "group": "Default",
                    "is_required": False,
                    "name": "Estimated Completion Date",
                    "type": "date",
                    "updated_at": "2023-03-09T15:25:49+00:00",
                    "value": None,
                },
                {
                    "created_at": "2023-03-09T15:25:49+00:00",
                    "group": "Default",
                    "is_required": False,
                    "name": "ZohoSyncDate",
                    "type": "date",
                    "updated_at": "2023-03-09T15:25:49+00:00",
                    "value": None,
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
                        "name": "item_custom_fields_item",
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


def test_convert_json_to_avro_schema() -> None:
    expected = {
        "type": "record",
        "name": "jobs",
        "fields": [
            {"name": "id", "type": "long"},
            {"name": "number", "type": "string"},
            {"name": "check_number", "type": "null", "default": None},
            {"name": "priority", "type": "string"},
        ],
    }
    actual = MergeableAvroSchema.convert_json_to_avro_schema(
        json_data={"id": 1, "number": "123", "check_number": None, "priority": "high"},
        name="jobs",
    )

    assert actual == expected


def test_convert_json_to_avro_schema_with_list() -> None:
    expected = {
        "fields": [
            {"name": "id", "type": "long"},
            {"name": "number", "type": "string"},
            {"default": None, "name": "check_number", "type": "null"},
            {"name": "priority", "type": "string"},
            {
                "name": "printable_work_order",
                "type": {
                    "items": {
                        "fields": [
                            {"name": "name", "type": "string"},
                            {"name": "url", "type": "string"},
                        ],
                        "name": "jobs_printable_work_order_item",
                        "type": "record",
                    },
                    "type": "array",
                },
            },
        ],
        "name": "jobs",
        "type": "record",
    }
    actual = MergeableAvroSchema.convert_json_to_avro_schema(
        json_data={
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
        name="jobs",
    )

    assert actual == expected


def test_convert_json_to_avro_schema_with_empty_list() -> None:
    expected = {
        "fields": [
            {"name": "id", "type": "long"},
            {"name": "number", "type": "string"},
            {"default": None, "name": "check_number", "type": "null"},
            {"name": "priority", "type": "string"},
            {
                "name": "printable_work_order",
                "type": {"default": [], "items": "null", "type": "array"},
            },
        ],
        "name": "jobs",
        "type": "record",
    }
    actual = MergeableAvroSchema.convert_json_to_avro_schema(
        json_data={
            "id": 1,
            "number": "123",
            "check_number": None,
            "priority": "high",
            "printable_work_order": [],
        },
        name="jobs",
    )

    assert actual == expected


def test_convert_json_to_avro_schema_with_dict() -> None:
    expected = {
        "fields": [
            {"name": "id", "type": "long"},
            {"name": "number", "type": "string"},
            {"default": None, "name": "check_number", "type": "null"},
            {"name": "priority", "type": "string"},
            {
                "name": "fake_data",
                "type": {
                    "fields": [
                        {"name": "name", "type": "string"},
                        {"name": "url", "type": "string"},
                    ],
                    "name": "jobs_fake_data",
                    "type": "record",
                },
            },
        ],
        "name": "jobs",
        "type": "record",
    }
    actual = MergeableAvroSchema.convert_json_to_avro_schema(
        json_data={
            "id": 1,
            "number": "123",
            "check_number": None,
            "priority": "high",
            "fake_data": {
                "name": "Print With Rates",
                "url": "https://admin.servicefusion.com/printJobWithRates?jobId=Sz6bz0GNuv5gIaLQ1mJeXeLorFxh_Uo0TYghWKC5mas",
            },
        },
        name="jobs",
    )

    assert actual == expected


def test_schema_candidate_from_airbyte_message() -> None:
    expected = AvroSchemaCandidate(
        MergeableAvroSchema(
            {
                "type": "record",
                "name": "jobs",
                "fields": [
                    {"name": "id", "type": "long"},
                    {"name": "number", "type": "string"},
                    {"name": "check_number", "type": "null", "default": None},
                    {"name": "priority", "type": "string"},
                ],
            }
        )
    )
    actual = AvroSchemaCandidate.from_avroable_data(
        AirbyteMessage(
            _airbyte_ab_id="abc123",
            _airbyte_stream="jobs",
            _airbyte_data={
                "id": 1,
                "number": "123",
                "check_number": None,
                "priority": "high",
            },
            _airbyte_emitted_at=1234567890,
        )
    )

    assert actual == expected


def test_mergeable_avro_schema_full_jobs_record():
    event = {
        "_airbyte_ab_id": "f0d68fda-8f5c-46ee-8aab-2698544172ca",
        "_airbyte_stream": "jobs",
        "_airbyte_emitted_at": 1678546500085,
        "_airbyte_data": {
            "id": 653944,
            "number": "653944",
            "check_number": None,
            "priority": "Normal",
            "description": "Big Box cleaning package w/ Gas Station",
            "tech_notes": "1.\tWork to be performed between 9 PM and 5:30 AM.\r\n2.\tNo chemicals are to be used in cleaning process.\r\n3.\tOnly pressure wash an entrance way when it is out of service and the other entrance way has been opened up, coordinate with active manager.\r\n\r\nIF PROBLEMS WITH IVR - CALL SERVICE CHANNEL HELPLINE: 516-240-6810\r\nIF FAIL TO IVR, PUT NOTE ON DAILY REPORT.\n",
            "completion_notes": None,
            "payment_status": "Unpaid",
            "taxes_fees_total": 2000,
            "drive_labor_total": 0,
            "billable_expenses_total": 0,
            "total": 2000,
            "payments_deposits_total": 0,
            "due_total": 2000,
            "cost_total": 1922,
            "duration": 28800,
            "time_frame_promised_start": "21:00",
            "time_frame_promised_end": "21:30",
            "start_date": None,
            "end_date": None,
            "created_at": "2015-11-23T17:02:34+00:00",
            "updated_at": "2015-11-23T17:02:34+00:00",
            "closed_at": None,
            "customer_id": None,
            "customer_name": None,
            "parent_customer": None,
            "status": "Dispatched",
            "sub_status": None,
            "contact_first_name": None,
            "contact_last_name": None,
            "street_1": None,
            "street_2": None,
            "city": None,
            "state_prov": None,
            "postal_code": None,
            "location_name": None,
            "is_gated": False,
            "gate_instructions": None,
            "category": "Big Box",
            "source": None,
            "payment_type": "Direct Bill",
            "customer_payment_terms": "NET 30",
            "project": None,
            "phase": None,
            "po_number": "Enter IVR # here",
            "contract": "Monthly Service",
            "note_to_customer": "Thank you for your business!",
            "called_in_by": None,
            "is_requires_follow_up": False,
            "agents": [
                {"id": 980193119, "first_name": "Andrew", "last_name": "Hayes"},
                {"id": 980193780, "first_name": "Jeremiah", "last_name": "Morgan"},
            ],
            "custom_fields": [],
            "pictures": [],
            "documents": [],
            "equipment": [],
            "techs_assigned": [],
            "tasks": [
                {
                    "type": "Misc",
                    "description": "Call IVR# 877-563-0589 to check in with Service Channel. Use PIN 343981 and PO# above",
                    "start_time": None,
                    "start_date": None,
                    "end_date": None,
                    "is_completed": False,
                    "created_at": "2015-11-23T17:02:34+00:00",
                    "updated_at": "2015-11-23T17:02:34+00:00",
                },
                {
                    "type": "Misc",
                    "description": "Check in with manager and request Gas Station lights to be left on",
                    "start_time": None,
                    "start_date": None,
                    "end_date": None,
                    "is_completed": False,
                    "created_at": "2015-11-23T17:02:34+00:00",
                    "updated_at": "2015-11-23T17:02:34+00:00",
                },
                {
                    "type": "Misc",
                    "description": "Place safety cones around work area",
                    "start_time": None,
                    "start_date": None,
                    "end_date": None,
                    "is_completed": False,
                    "created_at": "2015-11-23T17:02:34+00:00",
                    "updated_at": "2015-11-23T17:02:34+00:00",
                },
                {
                    "type": "Misc",
                    "description": "If there are air curtain units at entryways, be sure to turn these off before cleaning that area. Leave off to dry, and be sure manager is aware that they need to be turned back on.",
                    "start_time": None,
                    "start_date": None,
                    "end_date": None,
                    "is_completed": False,
                    "created_at": "2015-11-23T17:02:34+00:00",
                    "updated_at": "2015-11-23T17:02:34+00:00",
                },
                {
                    "type": "Misc",
                    "description": "Once job is complete, walk the job with the active manager to insure satisfaction - get work order signed.",
                    "start_time": None,
                    "start_date": None,
                    "end_date": None,
                    "is_completed": False,
                    "created_at": "2015-11-23T17:02:34+00:00",
                    "updated_at": "2015-11-23T17:02:34+00:00",
                },
                {
                    "type": "Misc",
                    "description": "Check out via IVR: 877-563-0589 - Use PIN 343981 and PO# above",
                    "start_time": None,
                    "start_date": None,
                    "end_date": None,
                    "is_completed": False,
                    "created_at": "2015-11-23T17:02:34+00:00",
                    "updated_at": "2015-11-23T17:02:34+00:00",
                },
                {
                    "type": "Misc",
                    "description": "Submit Job Report",
                    "start_time": None,
                    "start_date": None,
                    "end_date": None,
                    "is_completed": False,
                    "created_at": "2015-11-23T17:02:34+00:00",
                    "updated_at": "2015-11-23T17:02:34+00:00",
                },
            ],
            "notes": [],
            "products": [],
            "services": [
                {
                    "name": "Pressure Wash Entranceways",
                    "description": "Pressure/Steam Wash Entrances, to include:\r\n* 1st story storefront wall\r\n* All concrete surfaces in customer entrances and 20' surrounding. \r\n* Includes: chewing gum, wasp nest, and cob web removal in above areas.",
                    "multiplier": 1,
                    "rate": 950,
                    "total": 950,
                    "cost": 696,
                    "actual_cost": 361,
                    "item_index": 2,
                    "parent_index": 1,
                    "created_at": "2015-11-23T17:02:34+00:00",
                    "updated_at": "2015-11-23T17:02:34+00:00",
                    "is_show_rate_items": False,
                    "tax": None,
                    "service": "Pressure Wash Entranceways",
                    "service_list_id": 253532,
                    "service_rate_id": 93862,
                    "pattern_row_id": None,
                    "qbo_class_id": None,
                    "qbd_class_id": None,
                },
                {
                    "name": "Pressure Wash Sidewalks",
                    "description": "Pressure/Steam wash front sidewalks, to include: \r\n* 1st story storefront wall\r\n* All concrete sidewalks across storefront and 20' down both sides of store\r\n* Include chewing gum and wasp nest removal.",
                    "multiplier": 1,
                    "rate": 300,
                    "total": 300,
                    "cost": 120,
                    "actual_cost": 0,
                    "item_index": 3,
                    "parent_index": 1,
                    "created_at": "2015-11-23T17:02:34+00:00",
                    "updated_at": "2015-11-23T17:02:34+00:00",
                    "is_show_rate_items": False,
                    "tax": None,
                    "service": "Pressure Wash Sidewalks",
                    "service_list_id": 253533,
                    "service_rate_id": 93868,
                    "pattern_row_id": None,
                    "qbo_class_id": None,
                    "qbd_class_id": None,
                },
                {
                    "name": "Clean Emergency Exits",
                    "description": "Pressure Wash Emergency Exit Pads",
                    "multiplier": 1,
                    "rate": 50,
                    "total": 50,
                    "cost": 20,
                    "actual_cost": 20,
                    "item_index": 4,
                    "parent_index": 1,
                    "created_at": "2015-11-23T17:02:34+00:00",
                    "updated_at": "2015-11-23T17:02:34+00:00",
                    "is_show_rate_items": False,
                    "tax": None,
                    "service": "Clean Emergency Exits",
                    "service_list_id": 253534,
                    "service_rate_id": 93819,
                    "pattern_row_id": None,
                    "qbo_class_id": None,
                    "qbd_class_id": None,
                },
                {
                    "name": "Clean Signs",
                    "description": "Pressure/Steam Wash 1st Story Signs",
                    "multiplier": 1,
                    "rate": 100,
                    "total": 100,
                    "cost": 40,
                    "actual_cost": 0,
                    "item_index": 5,
                    "parent_index": 1,
                    "created_at": "2015-11-23T17:02:34+00:00",
                    "updated_at": "2015-11-23T17:02:34+00:00",
                    "is_show_rate_items": False,
                    "tax": None,
                    "service": "Clean Signs",
                    "service_list_id": 253535,
                    "service_rate_id": 93826,
                    "pattern_row_id": None,
                    "qbo_class_id": None,
                    "qbd_class_id": None,
                },
                {
                    "name": "Clean Trash Areas",
                    "description": "Pressure Wash Trash Compactor and Organic Waste Areas\r\n* CUT POWER TO COMPACTOR PRIOR TO CLEANING \r\n* Avoid getting power box wet\r\n* Clean both sides of enclosure walls\r\n* Clean enclosure floors\r\n* If there is a compactor leak, let a member of management know so they can address the issue.",
                    "multiplier": 1,
                    "rate": 350,
                    "total": 350,
                    "cost": 250,
                    "actual_cost": 0,
                    "item_index": 6,
                    "parent_index": 1,
                    "created_at": "2015-11-23T17:02:34+00:00",
                    "updated_at": "2015-11-23T17:02:34+00:00",
                    "is_show_rate_items": False,
                    "tax": None,
                    "service": "Clean Trash Areas",
                    "service_list_id": 253536,
                    "service_rate_id": 93830,
                    "pattern_row_id": None,
                    "qbo_class_id": None,
                    "qbd_class_id": None,
                },
                {
                    "name": "Clean Tire Center: Exterior",
                    "description": "Pressure/Steam wash exterior of Tire Center.To include gum and wasp nest removal.",
                    "multiplier": 1,
                    "rate": 250,
                    "total": 250,
                    "cost": 100,
                    "actual_cost": 0,
                    "item_index": 7,
                    "parent_index": 1,
                    "created_at": "2015-11-23T17:02:34+00:00",
                    "updated_at": "2015-11-23T17:02:34+00:00",
                    "is_show_rate_items": False,
                    "tax": None,
                    "service": "Clean Tire Center - Exterior",
                    "service_list_id": 253537,
                    "service_rate_id": 93828,
                    "pattern_row_id": None,
                    "qbo_class_id": None,
                    "qbd_class_id": None,
                },
                {
                    "name": "Water Reclamation",
                    "description": "Capture and process waste water.",
                    "multiplier": 1,
                    "rate": 0,
                    "total": 0,
                    "cost": 0,
                    "actual_cost": 0,
                    "item_index": 8,
                    "parent_index": 1,
                    "created_at": "2015-11-23T17:02:34+00:00",
                    "updated_at": "2015-11-23T17:02:34+00:00",
                    "is_show_rate_items": False,
                    "tax": None,
                    "service": "Water Reclamation - Included",
                    "service_list_id": 253538,
                    "service_rate_id": 93879,
                    "pattern_row_id": None,
                    "qbo_class_id": None,
                    "qbd_class_id": None,
                },
                {
                    "name": "Gas Station",
                    "description": "Pressure/Steam wash gas station canopy, islands, flat surfaces and building.To include gum and wasp nest removal.",
                    "multiplier": 1,
                    "rate": 950,
                    "total": 950,
                    "cost": 696,
                    "actual_cost": 0,
                    "item_index": 9,
                    "parent_index": 1,
                    "created_at": "2015-11-23T17:02:35+00:00",
                    "updated_at": "2015-11-23T17:02:35+00:00",
                    "is_show_rate_items": False,
                    "tax": None,
                    "service": "Pressure/Steam Clean Gas Station",
                    "service_list_id": 253539,
                    "service_rate_id": 143429,
                    "pattern_row_id": None,
                    "qbo_class_id": None,
                    "qbd_class_id": None,
                },
            ],
            "other_charges": [
                {
                    "name": "Package Discount",
                    "rate": 950,
                    "total": 950,
                    "charge_index": 1,
                    "parent_index": 1,
                    "is_percentage": False,
                    "is_discount": True,
                    "created_at": "2015-11-23T17:02:35+00:00",
                    "updated_at": "2015-11-23T17:02:35+00:00",
                    "other_charge": "Package Discount",
                    "applies_to": None,
                    "service_list_id": None,
                    "other_charge_id": 2245,
                    "pattern_row_id": None,
                    "qbo_class_id": None,
                    "qbd_class_id": None,
                }
            ],
            "labor_charges": [],
            "expenses": [],
            "payments": [],
            "invoices": [],
            "signatures": [],
            "printable_work_order": [
                {
                    "name": "Print With Rates",
                    "url": "https://admin.servicefusion.com/printJobWithRates?jobId=-ZqGZ54VYae9qjWip2WXBYKxJ8YlStDPrp8C6eG1PwE",
                },
                {
                    "name": "Print Without Rates",
                    "url": "https://admin.servicefusion.com/printJobWithoutRates?jobId=-ZqGZ54VYae9qjWip2WXBYKxJ8YlStDPrp8C6eG1PwE",
                },
                {
                    "name": "Download As Pdf",
                    "url": "https://admin.servicefusion.com/downloadJobAsPdf?jobId=-ZqGZ54VYae9qjWip2WXBYKxJ8YlStDPrp8C6eG1PwE",
                },
                {
                    "name": "Download As Excel",
                    "url": "https://admin.servicefusion.com/downloadJobAsExcel?jobId=-ZqGZ54VYae9qjWip2WXBYKxJ8YlStDPrp8C6eG1PwE",
                },
            ],
            "visits": [
                {
                    "notes_for_techs": None,
                    "time_frame_promised_start": "07:00",
                    "time_frame_promised_end": None,
                    "duration": 28800,
                    "is_text_notified": False,
                    "is_voice_notified": False,
                    "start_date": "2023-03-06",
                    "status": None,
                    "techs_assigned": [
                        {"id": 980414916, "first_name": "Danny", "last_name": "Teston"}
                    ],
                },
                {
                    "notes_for_techs": None,
                    "time_frame_promised_start": "08:00",
                    "time_frame_promised_end": None,
                    "duration": 28800,
                    "is_text_notified": False,
                    "is_voice_notified": False,
                    "start_date": "2023-03-09",
                    "status": None,
                    "techs_assigned": [
                        {"id": 980391383, "first_name": "Jovan", "last_name": "Maddox"}
                    ],
                },
                {
                    "notes_for_techs": None,
                    "time_frame_promised_start": "12:00",
                    "time_frame_promised_end": "14:00",
                    "duration": 7200,
                    "is_text_notified": False,
                    "is_voice_notified": False,
                    "start_date": "2023-03-21",
                    "status": None,
                    "techs_assigned": [
                        {
                            "id": 980367272,
                            "first_name": "Sebastian",
                            "last_name": "Lewis",
                        }
                    ],
                },
            ],
        },
    }

    actual = MergeableAvroSchema.from_avroable_data(event)
    expected = MergeableAvroSchema(
        schema_dict={
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
                        "items": {
                            "fields": [
                                {"name": "first_name", "type": "string"},
                                {"name": "id", "type": "long"},
                                {"name": "last_name", "type": "string"},
                            ],
                            "name": "jobs_agents_item",
                            "type": "record",
                        },
                        "type": "array",
                    },
                },
                {
                    "name": "custom_fields",
                    "type": {"default": [], "items": "null", "type": "array"},
                },
                {
                    "name": "pictures",
                    "type": {"default": [], "items": "null", "type": "array"},
                },
                {
                    "name": "documents",
                    "type": {"default": [], "items": "null", "type": "array"},
                },
                {
                    "name": "equipment",
                    "type": {"default": [], "items": "null", "type": "array"},
                },
                {
                    "name": "techs_assigned",
                    "type": {"default": [], "items": "null", "type": "array"},
                },
                {
                    "name": "tasks",
                    "type": {
                        "items": {
                            "fields": [
                                {"name": "created_at", "type": "string"},
                                {"name": "description", "type": "string"},
                                {"default": None, "name": "end_date", "type": "null"},
                                {"name": "is_completed", "type": "boolean"},
                                {"default": None, "name": "start_date", "type": "null"},
                                {"default": None, "name": "start_time", "type": "null"},
                                {"name": "type", "type": "string"},
                                {"name": "updated_at", "type": "string"},
                            ],
                            "name": "jobs_tasks_item",
                            "type": "record",
                        },
                        "type": "array",
                    },
                },
                {
                    "name": "notes",
                    "type": {"default": [], "items": "null", "type": "array"},
                },
                {
                    "name": "products",
                    "type": {"default": [], "items": "null", "type": "array"},
                },
                {
                    "name": "services",
                    "type": {
                        "items": {
                            "fields": [
                                {"name": "actual_cost", "type": "long"},
                                {"name": "cost", "type": "long"},
                                {"name": "created_at", "type": "string"},
                                {"name": "description", "type": "string"},
                                {"name": "is_show_rate_items", "type": "boolean"},
                                {"name": "item_index", "type": "long"},
                                {"name": "multiplier", "type": "long"},
                                {"name": "name", "type": "string"},
                                {"name": "parent_index", "type": "long"},
                                {
                                    "default": None,
                                    "name": "pattern_row_id",
                                    "type": "null",
                                },
                                {
                                    "default": None,
                                    "name": "qbd_class_id",
                                    "type": "null",
                                },
                                {
                                    "default": None,
                                    "name": "qbo_class_id",
                                    "type": "null",
                                },
                                {"name": "rate", "type": "long"},
                                {"name": "service", "type": "string"},
                                {"name": "service_list_id", "type": "long"},
                                {"name": "service_rate_id", "type": "long"},
                                {"default": None, "name": "tax", "type": "null"},
                                {"name": "total", "type": "long"},
                                {"name": "updated_at", "type": "string"},
                            ],
                            "name": "jobs_services_item",
                            "type": "record",
                        },
                        "type": "array",
                    },
                },
                {
                    "name": "other_charges",
                    "type": {
                        "items": {
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
                                {
                                    "default": None,
                                    "name": "pattern_row_id",
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
                            "name": "jobs_other_charges_item",
                            "type": "record",
                        },
                        "type": "array",
                    },
                },
                {
                    "name": "labor_charges",
                    "type": {"default": [], "items": "null", "type": "array"},
                },
                {
                    "name": "expenses",
                    "type": {"default": [], "items": "null", "type": "array"},
                },
                {
                    "name": "payments",
                    "type": {"default": [], "items": "null", "type": "array"},
                },
                {
                    "name": "invoices",
                    "type": {"default": [], "items": "null", "type": "array"},
                },
                {
                    "name": "signatures",
                    "type": {"default": [], "items": "null", "type": "array"},
                },
                {
                    "name": "printable_work_order",
                    "type": {
                        "items": {
                            "fields": [
                                {"name": "name", "type": "string"},
                                {"name": "url", "type": "string"},
                            ],
                            "name": "jobs_printable_work_order_item",
                            "type": "record",
                        },
                        "type": "array",
                    },
                },
                {
                    "name": "visits",
                    "type": {
                        "items": {
                            "fields": [
                                {"name": "duration", "type": "long"},
                                {"name": "is_text_notified", "type": "boolean"},
                                {"name": "is_voice_notified", "type": "boolean"},
                                {
                                    "default": None,
                                    "name": "notes_for_techs",
                                    "type": "null",
                                },
                                {"name": "start_date", "type": "string"},
                                {"default": None, "name": "status", "type": "null"},
                                {
                                    "name": "techs_assigned",
                                    "type": {
                                        "items": {
                                            "fields": [
                                                {"name": "id", "type": "long"},
                                                {
                                                    "name": "first_name",
                                                    "type": "string",
                                                },
                                                {"name": "last_name", "type": "string"},
                                            ],
                                            "name": "jobs_visits_item_techs_assigned_item",
                                            "type": "record",
                                        },
                                        "type": "array",
                                    },
                                },
                                {
                                    "default": None,
                                    "name": "time_frame_promised_end",
                                    "type": ["null", "string"],
                                },
                                {"name": "time_frame_promised_start", "type": "string"},
                            ],
                            "name": "jobs_visits_item",
                            "type": "record",
                        },
                        "type": "array",
                    },
                },
            ],
            "name": "jobs",
            "type": "record",
        }
    )

    assert actual == expected


def test_registered_schema_from_schema_version(schema_version: SchemaVersion) -> None:
    actual = RegisteredAvroSchema.from_schema_version(schema_version)
    expected = RegisteredAvroSchema(
        MergeableAvroSchema(schema_version.schema.flat_schema),
        RegisteredAvroSchemaId(6),
    )

    assert actual == expected


def test_default_always_created_when_null_exists():
    data = {
        "_airbyte_ab_id": "7da9fb2f-9602-4c6b-bf06-14716704840d",
        "_airbyte_stream": "jobs",
        "_airbyte_emitted_at": 1679275725411,
        "_airbyte_data": {
            "other_charges": [
                {
                    "charge_index": 1,
                    "other_charge": None,
                    "applies_to": None,
                },
                {
                    "charge_index": 2,
                    "other_charge": "Discount",
                    "applies_to": None,
                },
            ],
        },
    }

    actual = MergeableAvroSchema.from_avroable_data(data)
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
