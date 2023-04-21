"""
Schema mismatch: must be string on field street_2
existing schema:  MergeableAvroSchema(schema_dict={'type': 'record', 'name': 'jobs', 'fields': [{'name': 'id', 'type': 'long'}, {'name': 'number', 'type': 'string'}, {'name': 'check_number', 'type': 'null', 'default': None}, {'name': 'priority', 'type': 'string'}, {'name': 'description', 'type': 'string'}, {'name': 'tech_notes', 'type': 'string'}, {'name': 'completion_notes', 'type': 'null', 'default': None}, {'name': 'payment_status', 'type': 'string'}, {'name': 'taxes_fees_total', 'type': 'double'}, {'name': 'drive_labor_total', 'type': 'long'}, {'name': 'billable_expenses_total', 'type': 'long'}, {'name': 'total', 'type': 'double'}, {'name': 'payments_deposits_total', 'type': 'long'}, {'name': 'due_total', 'type': 'double'}, {'name': 'cost_total', 'type': 'long'}, {'name': 'duration', 'type': 'long'}, {'name': 'time_frame_promised_start', 'type': 'string'}, {'name': 'time_frame_promised_end', 'type': 'null', 'default': None}, {'name': 'start_date', 'type': 'string'}, {'name': 'end_date', 'type': 'null', 'default': None}, {'name': 'created_at', 'type': 'string'}, {'name': 'updated_at', 'type': 'string'}, {'name': 'closed_at', 'type': 'null', 'default': None}, {'name': 'customer_id', 'type': 'long'}, {'name': 'customer_name', 'type': 'string'}, {'name': 'parent_customer', 'type': 'null', 'default': None}, {'name': 'status', 'type': 'string'}, {'name': 'sub_status', 'type': 'null', 'default': None}, {'name': 'contact_first_name', 'type': 'string'}, {'name': 'contact_last_name', 'type': 'string'}, {'name': 'street_1', 'type': 'string'}, {'name': 'street_2', 'type': 'string'}, {'name': 'city', 'type': 'string'}, {'name': 'state_prov', 'type': 'string'}, {'name': 'postal_code', 'type': 'string'}, {'name': 'location_name', 'type': 'null', 'default': None}, {'name': 'is_gated', 'type': 'boolean'}, {'name': 'gate_instructions', 'type': 'null', 'default': None}, {'name': 'category', 'type': 'string'}, {'name': 'source', 'type': 'string'}, {'name': 'payment_type', 'type': 'string'}, {'name': 'customer_payment_terms', 'type': 'string'}, {'name': 'project', 'type': 'null', 'default': None}, {'name': 'phase', 'type': 'null', 'default': None}, {'name': 'po_number', 'type': 'null', 'default': None}, {'name': 'contract', 'type': 'null', 'default': None}, {'name': 'note_to_customer', 'type': 'null', 'default': None}, {'name': 'called_in_by', 'type': 'null', 'default': None}, {'name': 'is_requires_follow_up', 'type': 'boolean'}, {'name': 'agents', 'type': {'type': 'array', 'items': {'type': 'record', 'name': 'agents_item', 'fields': [{'name': 'id', 'type': 'long'}, {'name': 'first_name', 'type': 'string'}, {'name': 'last_name', 'type': 'string'}]}}}, {'name': 'custom_fields', 'type': {'type': 'array', 'items': {'name': 'custom_fields_item', 'type': 'record', 'fields': [{'name': 'created_at', 'type': 'string'}, {'name': 'group', 'type': 'string'}, {'name': 'is_required', 'type': 'boolean'}, {'name': 'name', 'type': 'string'}, {'name': 'type', 'type': 'string'}, {'name': 'updated_at', 'type': 'string'}, {'name': 'value', 'type': ['null', 'boolean', 'string'], 'default': None}]}}}, {'name': 'pictures', 'type': {'type': 'array', 'items': 'null', 'default': []}}, {'name': 'documents', 'type': {'type': 'array', 'items': 'null', 'default': []}}, {'name': 'equipment', 'type': {'type': 'array', 'items': 'null', 'default': []}}, {'name': 'techs_assigned', 'type': {'type': 'array', 'items': {'type': 'record', 'name': 'techs_assigned_item', 'fields': [{'name': 'id', 'type': 'long'}, {'name': 'first_name', 'type': 'string'}, {'name': 'last_name', 'type': 'string'}]}}}, {'name': 'tasks', 'type': {'type': 'array', 'items': 'null', 'default': []}}, {'name': 'notes', 'type': {'type': 'array', 'items': 'null', 'default': []}}, {'name': 'products', 'type': {'type': 'array', 'items': 'null', 'default': []}}, {'name': 'services', 'type': {'type': 'array', 'items': {'type': 'record', 'name': 'services_item', 'fields': [{'name': 'name', 'type': 'string'}, {'name': 'description', 'type': 'string'}, {'name': 'multiplier', 'type': 'long'}, {'name': 'rate', 'type': 'long'}, {'name': 'total', 'type': 'long'}, {'name': 'cost', 'type': 'long'}, {'name': 'actual_cost', 'type': 'long'}, {'name': 'item_index', 'type': 'long'}, {'name': 'parent_index', 'type': 'long'}, {'name': 'created_at', 'type': 'string'}, {'name': 'updated_at', 'type': 'string'}, {'name': 'is_show_rate_items', 'type': 'boolean'}, {'name': 'tax', 'type': 'string'}, {'name': 'service', 'type': 'string'}, {'name': 'service_list_id', 'type': 'long'}, {'name': 'service_rate_id', 'type': 'long'}, {'name': 'pattern_row_id', 'type': 'long'}, {'name': 'qbo_class_id', 'type': 'null', 'default': None}, {'name': 'qbd_class_id', 'type': 'null', 'default': None}]}}}, {'name': 'other_charges', 'type': {'type': 'array', 'items': {'type': 'record', 'name': 'other_charges_item', 'fields': [{'name': 'name', 'type': 'string'}, {'name': 'rate', 'type': 'double'}, {'name': 'total', 'type': 'double'}, {'name': 'charge_index', 'type': 'long'}, {'name': 'parent_index', 'type': 'long'}, {'name': 'is_percentage', 'type': 'boolean'}, {'name': 'is_discount', 'type': 'boolean'}, {'name': 'created_at', 'type': 'string'}, {'name': 'updated_at', 'type': 'string'}, {'name': 'other_charge', 'type': 'string'}, {'name': 'applies_to', 'type': 'null', 'default': None}, {'name': 'service_list_id', 'type': 'null', 'default': None}, {'name': 'other_charge_id', 'type': 'long'}, {'name': 'pattern_row_id', 'type': 'long'}, {'name': 'qbo_class_id', 'type': 'null', 'default': None}, {'name': 'qbd_class_id', 'type': 'null', 'default': None}]}}}, {'name': 'labor_charges', 'type': {'type': 'array', 'items': 'null', 'default': []}}, {'name': 'expenses', 'type': {'type': 'array', 'items': 'null', 'default': []}}, {'name': 'payments', 'type': {'type': 'array', 'items': 'null', 'default': []}}, {'name': 'invoices', 'type': {'type': 'array', 'items': 'null', 'default': []}}, {'name': 'signatures', 'type': {'type': 'array', 'items': 'null', 'default': []}}, {'name': 'printable_work_order', 'type': {'type': 'array', 'items': {'name': 'printable_work_order_item', 'type': 'record', 'fields': [{'name': 'name', 'type': 'string'}, {'name': 'url', 'type': 'string'}]}}}, {'name': 'visits', 'type': {'type': 'array', 'items': 'null', 'default': []}}]})
Message data:  {'id': 1015712267, 'number': '1015712267', 'check_number': None, 'priority': 'Normal', 'description': 'Window Cleaning In and out - wipe window sills to remove water and debris', 'tech_notes': 'in and out \n', 'completion_notes': None, 'payment_status': 'Unpaid', 'taxes_fees_total': 50.23, 'drive_labor_total': 0, 'billable_expenses_total': 0, 'total': 50.23, 'payments_deposits_total': 0, 'due_total': 50.23, 'cost_total': 0, 'duration': 900, 'time_frame_promised_start': '07:30', 'time_frame_promised_end': None, 'start_date': '2024-03-11', 'end_date': None, 'created_at': '2023-03-12T00:18:48+00:00', 'updated_at': '2023-03-12T00:18:48+00:00', 'closed_at': None, 'customer_id': 1027587, 'customer_name': 'Matthews Restaurant', 'parent_customer': None, 'status': 'Cancelled', 'sub_status': None, 'contact_first_name': 'Matthew', 'contact_last_name': 'Medure', 'street_1': '2107 Hendricks Avenue', 'street_2': None, 'city': 'Jacksonville', 'state_prov': 'FL', 'postal_code': '32207', 'location_name': None, 'is_gated': False, 'gate_instructions': None, 'category': 'Route', 'source': 'Word of Mouth', 'payment_type': 'Direct Bill', 'customer_payment_terms': 'NET 30', 'project': None, 'phase': None, 'po_number': None, 'contract': None, 'note_to_customer': None, 'called_in_by': None, 'is_requires_follow_up': False, 'agents': [{'id': 980193782, 'first_name': 'Benjamin', 'last_name': 'Steffens'}], 'custom_fields': [{'name': 'IVR PIN #', 'value': None, 'type': 'text', 'group': 'Default', 'created_at': '2023-03-12T00:18:48+00:00', 'updated_at': '2023-03-12T00:18:48+00:00', 'is_required': False}, {'name': 'IVR ID #', 'value': None, 'type': 'text', 'group': 'Default', 'created_at': '2023-03-12T00:18:48+00:00', 'updated_at': '2023-03-12T00:18:48+00:00', 'is_required': False}, {'name': 'Certified Payroll Required?', 'value': False, 'type': 'checkbox', 'group': 'Default', 'created_at': '2023-03-12T00:18:48+00:00', 'updated_at': '2023-03-12T00:18:48+00:00', 'is_required': False}, {'name': 'Route Day', 'value': 'Mon', 'type': 'select', 'group': 'Default', 'created_at': '2023-03-12T00:18:48+00:00', 'updated_at': '2023-03-12T00:18:48+00:00', 'is_required': False}, {'name': 'Store #', 'value': None, 'type': 'text', 'group': 'Default', 'created_at': '2023-03-12T00:18:48+00:00', 'updated_at': '2023-03-12T00:18:48+00:00', 'is_required': False}, {'name': 'Estimated Completion Date', 'value': None, 'type': 'date', 'group': 'Default', 'created_at': '2023-03-12T00:18:48+00:00', 'updated_at': '2023-03-12T00:18:48+00:00', 'is_required': False}, {'name': 'Man Hours Budgeted', 'value': 0, 'type': 'numericinput', 'group': 'Default', 'created_at': '2023-03-12T00:18:48+00:00', 'updated_at': '2023-03-12T00:18:48+00:00', 'is_required': False}, {'name': 'Total Man Hours Recorded', 'value': 0, 'type': 'numericinput', 'group': 'Default', 'created_at': '2023-03-12T00:18:48+00:00', 'updated_at': '2023-03-12T00:18:48+00:00', 'is_required': False}, {'name': 'Man Hours Spent To Date', 'value': 0, 'type': 'numericinput', 'group': 'Default', 'created_at': '2023-03-12T00:18:48+00:00', 'updated_at': '2023-03-12T00:18:48+00:00', 'is_required': False}], 'pictures': [], 'documents': [], 'equipment': [], 'techs_assigned': [], 'tasks': [], 'notes': [], 'products': [], 'services': [{'name': 'Window Cleaning Interior/Exterior', 'description': '* Clean Windows Inside and Out\r\n* Removal of paint, adhesive, calcium deposits, or construction debris is an additional service and will require a Scratched Glass Waiver giving Krystal Klean permission to use plastic scrapers or razors to remove debris.\r\n* Krystal Klean upholds the highest industry standards for glass cleaning tools & methods but must inform and educate its customers about the inherent risk of scratches when cleaning glass. Given the facts below, Krystal Klean cannot be held liable for glass scratches. Minuscule glass particles (“glass fines”) may exist on the pane surface. This flaw is common for tempered or hurricane-proof glass often installed in FL. During a normal cleaning process, the glass fines can break off and cause hairline scratches. Removal of paint, adhesives, calcium deposits, or construction debris may require the use of scrubbing pads or scrapers, which increases the risk of scratched glass, and is a separate service from standard window cleaning. When cleaning glass to remove calcium deposits, some brands of tinted or soft glass may be micro-scratched with vinyl buffing pads. Pre-existing scratches may be visible or apparent after the glass is cleaned.', 'multiplier': 1, 'rate': 46.73, 'total': 46.73, 'cost': 0, 'actual_cost': 0, 'item_index': 4, 'parent_index': 0, 'created_at': '2023-03-12T00:18:48+00:00', 'updated_at': '2023-03-12T00:18:48+00:00', 'is_show_rate_items': False, 'tax': 'Duval', 'service': 'Window Cleaning Interior/Exterior', 'service_list_id': 106930183, 'service_rate_id': 93882, 'pattern_row_id': 28871914, 'qbo_class_id': None, 'qbd_class_id': None}], 'other_charges': [{'name': 'Duval', 'rate': 7.5, 'total': 3.50475, 'charge_index': 0, 'parent_index': 1, 'is_percentage': True, 'is_discount': False, 'created_at': '2023-03-12T00:18:48+00:00', 'updated_at': '2023-03-12T00:18:48+00:00', 'other_charge': 'Duval', 'applies_to': None, 'service_list_id': None, 'other_charge_id': 1545, 'pattern_row_id': 1941978, 'qbo_class_id': None, 'qbd_class_id': None}], 'labor_charges': [], 'expenses': [], 'payments': [], 'invoices': [], 'signatures': [], 'printable_work_order': [{'name': 'Print With Rates', 'url': 'https://admin.servicefusion.com/printJobWithRates?jobId=6K7hoVWLC37VuXsT-hrDulIZsVwqJqpCRCNlwTQJlio'}, {'name': 'Print Without Rates', 'url': 'https://admin.servicefusion.com/printJobWithoutRates?jobId=6K7hoVWLC37VuXsT-hrDulIZsVwqJqpCRCNlwTQJlio'}, {'name': 'Download As Pdf', 'url': 'https://admin.servicefusion.com/downloadJobAsPdf?jobId=6K7hoVWLC37VuXsT-hrDulIZsVwqJqpCRCNlwTQJlio'}, {'name': 'Download As Excel', 'url': 'https://admin.servicefusion.com/downloadJobAsExcel?jobId=6K7hoVWLC37VuXsT-hrDulIZsVwqJqpCRCNlwTQJlio'}], 'visits': []}
"""
from schema_registry.client.schema import AvroSchema


from schema_registry.serializers import AvroMessageSerializer
from json_to_avro.json_to_avro import (
    SchemaProvider,
)

from json_to_avro.avro_schema import RegisteredAvroSchema
from json_to_avro import JsonToAvro, AvroableData


def test_things(
    schema_version_jobs_test, schema_registry_client_with_jobs_cached_schema
):
    def _register(subject: str, schema, *_, self=None, **__):
        schema_registry_client_with_jobs_cached_schema._cache_schema(
            schema,
            schema_version_jobs_test.version + 1,
            subject,
            schema_version_jobs_test.version + 1,
        )
        return schema_version_jobs_test.schema_id + 1

    subject = "jobs-test-value"
    registered_schema = RegisteredAvroSchema.from_schema_version(
        schema_version_jobs_test
    )
    provider = SchemaProvider(
        {subject: registered_schema}, schema_registry_client_with_jobs_cached_schema
    )
    schema_registry_client_with_jobs_cached_schema.register = _register
    schema_registry_client_with_jobs_cached_schema.get_by_id = (
        lambda *_, **__: AvroSchema(provider[subject].schema.schema_dict)
    )

    ser = AvroMessageSerializer(schema_registry_client_with_jobs_cached_schema)
    airbyte_msg = dict(
        _airbyte_ab_id="1",
        _airbyte_stream="jobs",
        _airbyte_emitted_at=0,
        _airbyte_data={
            "id": 1015712267,
            "number": "1015712267",
            "check_number": None,
            "priority": "Normal",
            "description": "Window Cleaning In and out - wipe window sills to remove water and debris",
            "tech_notes": "in and out \n",
            "completion_notes": None,
            "payment_status": "Unpaid",
            "taxes_fees_total": 50.23,
            "drive_labor_total": 0,
            "billable_expenses_total": 0,
            "total": 50.23,
            "payments_deposits_total": 0,
            "due_total": 50.23,
            "cost_total": 0,
            "duration": 900,
            "time_frame_promised_start": "07:30",
            "time_frame_promised_end": None,
            "start_date": "2024-03-11",
            "end_date": None,
            "created_at": "2023-03-12T00:18:48+00:00",
            "updated_at": "2023-03-12T00:18:48+00:00",
            "closed_at": None,
            "customer_id": 1027587,
            "customer_name": "Matthews Restaurant",
            "parent_customer": None,
            "status": "Cancelled",
            "sub_status": None,
            "contact_first_name": "Matthew",
            "contact_last_name": "Medure",
            "street_1": "2107 Hendricks Avenue",
            "street_2": None,
            "city": "Jacksonville",
            "state_prov": "FL",
            "postal_code": "32207",
            "location_name": None,
            "is_gated": False,
            "gate_instructions": None,
            "category": "Route",
            "source": "Word of Mouth",
            "payment_type": "Direct Bill",
            "customer_payment_terms": "NET 30",
            "project": None,
            "phase": None,
            "po_number": None,
            "contract": None,
            "note_to_customer": None,
            "called_in_by": None,
            "is_requires_follow_up": False,
            "agents": [
                {"id": 980193782, "first_name": "Benjamin", "last_name": "Steffens"}
            ],
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
                    "name": "Route Day",
                    "value": "Mon",
                    "type": "select",
                    "group": "Default",
                    "created_at": "2023-03-12T00:18:48+00:00",
                    "updated_at": "2023-03-12T00:18:48+00:00",
                    "is_required": False,
                },
                {
                    "name": "Store #",
                    "value": None,
                    "type": "text",
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
                {
                    "name": "Man Hours Budgeted",
                    "value": 0,
                    "type": "numericinput",
                    "group": "Default",
                    "created_at": "2023-03-12T00:18:48+00:00",
                    "updated_at": "2023-03-12T00:18:48+00:00",
                    "is_required": False,
                },
                {
                    "name": "Total Man Hours Recorded",
                    "value": 0,
                    "type": "numericinput",
                    "group": "Default",
                    "created_at": "2023-03-12T00:18:48+00:00",
                    "updated_at": "2023-03-12T00:18:48+00:00",
                    "is_required": False,
                },
                {
                    "name": "Man Hours Spent To Date",
                    "value": 0,
                    "type": "numericinput",
                    "group": "Default",
                    "created_at": "2023-03-12T00:18:48+00:00",
                    "updated_at": "2023-03-12T00:18:48+00:00",
                    "is_required": False,
                },
            ],
            "pictures": [],
            "documents": [],
            "equipment": [],
            "techs_assigned": [],
            "tasks": [],
            "notes": [],
            "products": [],
            "services": [
                {
                    "name": "Window Cleaning Interior/Exterior",
                    "description": "* Clean Windows Inside and Out\r\n* Removal of paint, adhesive, calcium deposits, or construction debris is an additional service and will require a Scratched Glass Waiver giving Krystal Klean permission to use plastic scrapers or razors to remove debris.\r\n* Krystal Klean upholds the highest industry standards for glass cleaning tools & methods but must inform and educate its customers about the inherent risk of scratches when cleaning glass. Given the facts below, Krystal Klean cannot be held liable for glass scratches. Minuscule glass particles (“glass fines”) may exist on the pane surface. This flaw is common for tempered or hurricane-proof glass often installed in FL. During a normal cleaning process, the glass fines can break off and cause hairline scratches. Removal of paint, adhesives, calcium deposits, or construction debris may require the use of scrubbing pads or scrapers, which increases the risk of scratched glass, and is a separate service from standard window cleaning. When cleaning glass to remove calcium deposits, some brands of tinted or soft glass may be micro-scratched with vinyl buffing pads. Pre-existing scratches may be visible or apparent after the glass is cleaned.",
                    "multiplier": 1,
                    "rate": 46.73,
                    "total": 46.73,
                    "cost": 0,
                    "actual_cost": 0,
                    "item_index": 4,
                    "parent_index": 0,
                    "created_at": "2023-03-12T00:18:48+00:00",
                    "updated_at": "2023-03-12T00:18:48+00:00",
                    "is_show_rate_items": False,
                    "tax": "Duval",
                    "service": "Window Cleaning Interior/Exterior",
                    "service_list_id": 106930183,
                    "service_rate_id": 93882,
                    "pattern_row_id": 28871914,
                    "qbo_class_id": None,
                    "qbd_class_id": None,
                }
            ],
            "other_charges": [
                {
                    "name": "Duval",
                    "rate": 7.5,
                    "total": 3.50475,
                    "charge_index": 0,
                    "parent_index": 1,
                    "is_percentage": True,
                    "is_discount": False,
                    "created_at": "2023-03-12T00:18:48+00:00",
                    "updated_at": "2023-03-12T00:18:48+00:00",
                    "other_charge": "Duval",
                    "applies_to": None,
                    "service_list_id": None,
                    "other_charge_id": 1545,
                    "pattern_row_id": 1941978,
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
                    "url": "https://admin.servicefusion.com/printJobWithRates?jobId=6K7hoVWLC37VuXsT-hrDulIZsVwqJqpCRCNlwTQJlio",
                },
                {
                    "name": "Print Without Rates",
                    "url": "https://admin.servicefusion.com/printJobWithoutRates?jobId=6K7hoVWLC37VuXsT-hrDulIZsVwqJqpCRCNlwTQJlio",
                },
                {
                    "name": "Download As Pdf",
                    "url": "https://admin.servicefusion.com/downloadJobAsPdf?jobId=6K7hoVWLC37VuXsT-hrDulIZsVwqJqpCRCNlwTQJlio",
                },
                {
                    "name": "Download As Excel",
                    "url": "https://admin.servicefusion.com/downloadJobAsExcel?jobId=6K7hoVWLC37VuXsT-hrDulIZsVwqJqpCRCNlwTQJlio",
                },
            ],
            "visits": [],
        },
    )

    actual = JsonToAvro(SchemaProvider({}, schema_registry_client_with_jobs_cached_schema), ser).serialize_as_avro(AvroableData(record_name="jobs", subject_name="jobs-value", data=airbyte_msg))
    expected = b'\x00\x00\x00\x00\x08\x02\x10Benjamin\xec\xc7\xe4\xa6\x07\x10Steffens\x00\x00\nRoute\x18Jacksonville\x0eMatthew\x0cMedure\x0022023-03-12T00:18:48+00:00\x1222023-03-12T00:18:48+00:00\x0eDefault\x00\x12IVR PIN #\x08text22023-03-12T00:18:48+00:00\x0022023-03-12T00:18:48+00:00\x0eDefault\x00\x10IVR ID #\x08text22023-03-12T00:18:48+00:00\x0022023-03-12T00:18:48+00:00\x0eDefault\x006Certified Payroll Required?\x10checkbox22023-03-12T00:18:48+00:00\x02\x0022023-03-12T00:18:48+00:00\x0eDefault\x00\x12Route Day\x0cselect22023-03-12T00:18:48+00:00\x06\x06Mon22023-03-12T00:18:48+00:00\x0eDefault\x00\x0eStore #\x08text22023-03-12T00:18:48+00:00\x0022023-03-12T00:18:48+00:00\x0eDefault\x002Estimated Completion Date\x08date22023-03-12T00:18:48+00:00\x0022023-03-12T00:18:48+00:00\x0eDefault\x00$Man Hours Budgeted\x18numericinput22023-03-12T00:18:48+00:00\x04\x0022023-03-12T00:18:48+00:00\x0eDefault\x000Total Man Hours Recorded\x18numericinput22023-03-12T00:18:48+00:00\x04\x0022023-03-12T00:18:48+00:00\x0eDefault\x00.Man Hours Spent To Date\x18numericinput22023-03-12T00:18:48+00:00\x04\x00\x00\x86\xb8}&Matthews Restaurant\x0cNET 30\x92\x01Window Cleaning In and out - wipe window sills to remove water and debris\x00\x00=\n\xd7\xa3p\x1dI@\x88\x0e\x00\x00\x96\xa8\xd4\xc8\x07\x00\x00\x00\x00\x00\x141015712267\x02\x0022023-03-12T00:18:48+00:00\x00\x01\nDuval\nDuval\x92\x18\x02\xb4\x87\xed\x01\x00\x00\x00\x00\x00\x00\x1e@}?5^\xba\t\x0c@22023-03-12T00:18:48+00:00\x00\x0cUnpaid\x16Direct Bill\x00\x00\x00\n32207\x08 Print With Rates\xc6\x01https://admin.servicefusion.com/printJobWithRates?jobId=6K7hoVWLC37VuXsT-hrDulIZsVwqJqpCRCNlwTQJlio&Print Without Rates\xcc\x01https://admin.servicefusion.com/printJobWithoutRates?jobId=6K7hoVWLC37VuXsT-hrDulIZsVwqJqpCRCNlwTQJlio\x1eDownload As Pdf\xc4\x01https://admin.servicefusion.com/downloadJobAsPdf?jobId=6K7hoVWLC37VuXsT-hrDulIZsVwqJqpCRCNlwTQJlio"Download As Excel\xc8\x01https://admin.servicefusion.com/downloadJobAsExcel?jobId=6K7hoVWLC37VuXsT-hrDulIZsVwqJqpCRCNlwTQJlio\x00\x0cNormal\x00\x02\x00\x0022023-03-12T00:18:48+00:00\xf2\x12* Clean Windows Inside and Out\r\n* Removal of paint, adhesive, calcium deposits, or construction debris is an additional service and will require a Scratched Glass Waiver giving Krystal Klean permission to use plastic scrapers or razors to remove debris.\r\n* Krystal Klean upholds the highest industry standards for glass cleaning tools & methods but must inform and educate its customers about the inherent risk of scratches when cleaning glass. Given the facts below, Krystal Klean cannot be held liable for glass scratches. Minuscule glass particles (\xe2\x80\x9cglass fines\xe2\x80\x9d) may exist on the pane surface. This flaw is common for tempered or hurricane-proof glass often installed in FL. During a normal cleaning process, the glass fines can break off and cause hairline scratches. Removal of paint, adhesives, calcium deposits, or construction debris may require the use of scrubbing pads or scrapers, which increases the risk of scratched glass, and is a separate service from standard window cleaning. When cleaning glass to remove calcium deposits, some brands of tinted or soft glass may be micro-scratched with vinyl buffing pads. Pre-existing scratches may be visible or apparent after the glass is cleaned.\x00\x08\x02BWindow Cleaning Interior/Exterior\x00\xd4\xb3\xc4\x1b\x02=\n\xd7\xa3p]G@BWindow Cleaning Interior/Exterior\x8e\x80\xfde\xf4\xba\x0b\nDuval\x02=\n\xd7\xa3p]G@22023-03-12T00:18:48+00:00\x00\x00\x1aWord of Mouth\x142024-03-11\x04FL\x12Cancelled*2107 Hendricks Avenue\x00\x00=\n\xd7\xa3p\x1dI@\x18in and out \n\x00\n07:30=\n\xd7\xa3p\x1dI@22023-03-12T00:18:48+00:00\x00'

    assert actual == expected
