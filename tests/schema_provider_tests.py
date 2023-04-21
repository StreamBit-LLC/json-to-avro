from typing import MutableMapping
from unittest.mock import Mock

import pytest
from schema_registry.client import AsyncSchemaRegistryClient
from schema_registry.client.utils import SchemaVersion

from conftest import SchemaMetadata
from airbyte_json_to_avro.airbyte_json_to_avro import (
    SchemaProvider,
    RegisteredAvroSchema,
    AvroSchemaCandidate,
    AirbyteMessage,
    RegisteredAvroSchemaId,
)


@pytest.mark.asyncio
async def test_schema_provider_get_from_cache(
    current_schema_table_with_cached_schema: MutableMapping[str, RegisteredAvroSchema],
    schema_metadata: SchemaMetadata,
) -> None:
    sr_mock = Mock()
    provider = SchemaProvider(current_schema_table_with_cached_schema, sr_mock)
    actual = await provider.get(schema_metadata["subject"])
    expected = current_schema_table_with_cached_schema[schema_metadata["subject"]]
    assert actual == expected
    sr_mock.get_schema.assert_not_called()


@pytest.mark.asyncio
async def test_schema_provider_get_no_cache(
    schema_registry_client_with_patched_request_method_for_get_schema: AsyncSchemaRegistryClient,
    schema_metadata: SchemaMetadata,
    schema_version: SchemaVersion,
) -> None:
    current_schema_table: MutableMapping[str, RegisteredAvroSchema] = dict()
    provider = SchemaProvider(
        current_schema_table,
        schema_registry_client_with_patched_request_method_for_get_schema,
    )
    actual = await provider.get(schema_metadata["subject"])
    expected = RegisteredAvroSchema.from_schema_version(schema_version)
    assert actual == expected
    assert schema_metadata["subject"] in current_schema_table


@pytest.mark.asyncio
async def test_schema_provider_get_schema_doesnt_exist(
    schema_registry_client_with_patched_request_method_for_get_schema_not_found: AsyncSchemaRegistryClient,
    schema_metadata: SchemaMetadata,
    schema_version: SchemaVersion,
) -> None:
    current_schema_table: MutableMapping[str, RegisteredAvroSchema] = dict()
    provider = SchemaProvider(
        current_schema_table,
        schema_registry_client_with_patched_request_method_for_get_schema_not_found,
    )
    actual = await provider.get(schema_metadata["subject"])
    assert actual is None
    assert schema_metadata["subject"] not in current_schema_table


@pytest.mark.asyncio
def test_schema_provider_getitem_key_error() -> None:
    sr_mock = Mock()
    current_schema_table: MutableMapping[str, RegisteredAvroSchema] = dict()
    provider = SchemaProvider(current_schema_table, sr_mock)
    with pytest.raises(KeyError):
        provider["subject"]  # noqa


@pytest.mark.asyncio
def test_schema_provider_getitem_returns_subject(
    current_schema_table_with_cached_schema: MutableMapping[str, RegisteredAvroSchema],
    schema_metadata: SchemaMetadata,
    schema_version: SchemaVersion,
) -> None:
    sr_mock = Mock()
    provider = SchemaProvider(current_schema_table_with_cached_schema, sr_mock)
    actual = provider[schema_metadata["subject"]]
    expected = RegisteredAvroSchema.from_schema_version(schema_version)
    assert actual == expected


@pytest.mark.asyncio
async def test_schema_provider_register_and_set_returns_schema_id(
    schema_registry_client_with_patched_request_method_register: AsyncSchemaRegistryClient,
    schema_metadata: SchemaMetadata,
    schema_version: SchemaVersion,
    airbyte_msg: AirbyteMessage,
) -> None:
    current_schema_table: MutableMapping[str, RegisteredAvroSchema] = dict()
    provider = SchemaProvider(
        current_schema_table,
        schema_registry_client_with_patched_request_method_register,
    )
    schema_candidate = AvroSchemaCandidate.from_avroable_data(airbyte_msg)
    actual_schema_id = await provider.register_and_set(
        schema_metadata["subject"], AvroSchemaCandidate.from_avroable_data(airbyte_msg)
    )
    expected_schema_id = schema_version.schema_id
    assert actual_schema_id == expected_schema_id
    assert current_schema_table[schema_metadata["subject"]] == RegisteredAvroSchema(
        schema=schema_candidate.schema,
        schema_id=RegisteredAvroSchemaId(expected_schema_id),
    )
