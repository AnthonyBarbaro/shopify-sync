import unittest
from copy import deepcopy
from types import SimpleNamespace
from typing import Any
from unittest import mock

from app.db import ShopRecord
from app.inventory import InventorySyncService
from app.models import CustomerSyncRequest
from app.shopify import ShopifyClient
from app.utils import ShopifyAPIError


class AppendOnlyCustomerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.client = mock.Mock(spec=ShopifyClient)
        self.client.get_customer_by_identifier.return_value = None
        self.client.customer_create.return_value = {
            'id': 'gid://shopify/Customer/2', 'displayName': 'New Customer',
        }
        self.service = InventorySyncService(self.client, SimpleNamespace(), None)
        self.service.logger = mock.Mock()
        self.shop = ShopRecord(shop_domain='one.myshopify.com', access_token='test-token')
        self.payload = CustomerSyncRequest(
            source='Customer.dbf', pos_customer_number='123', firstName='POS name',
            email='sample@example.test', phone='+15551234567',
            tags=['POS import'], note='POS note', taxExempt=False,
            addresses=[{'address1': '123 Sample Street'}],
            metafields=[{'namespace': 'pos', 'key': 'customer_class', 'value': 'retail'}],
        )
        self.existing: dict[str, Any] = {
            'id': 'gid://shopify/Customer/1', 'displayName': 'Shopify name',
            'email': 'sample@example.test', 'phone': '+15551234567',
            'tags': ['VIP'], 'note': 'Merchant note', 'taxExempt': True,
            'addresses': [{'id': 'address1', 'address1': '456 Existing Street'}],
            'emailMarketingConsent': {'marketingState': 'UNSUBSCRIBED'},
            'metafields': [{'namespace': 'custom', 'key': 'loyalty', 'value': 'gold'}],
        }

    def assert_no_customer_updates(self) -> None:
        self.client.customer_set.assert_not_called()
        self.client.set_customer_metafields.assert_not_called()

    def test_existing_customers_are_preserved_by_pos_id_email_or_phone(self) -> None:
        for match_index, expected_key in enumerate(('customId', 'emailAddress', 'phoneNumber')):
            with self.subTest(matched_by=expected_key):
                self.client.reset_mock()
                self.client.get_customer_by_identifier.side_effect = [None] * match_index + [self.existing]
                before = deepcopy(self.existing)
                result = self.service.sync_customer(self.payload, self.shop)
                self.assertTrue(result.success)
                self.assertTrue(result.details['skipped_existing'])
                self.assertFalse(result.details['created'])
                self.assertEqual(result.customer_id, self.existing['id'])
                self.assertIn(expected_key, self.client.get_customer_by_identifier.call_args.kwargs['identifier'])
                self.client.customer_create.assert_not_called()
                self.assert_no_customer_updates()
                self.assertEqual(self.existing, before)

    def test_new_customer_is_created_with_identity_and_metafields_in_one_request(self) -> None:
        result = self.service.sync_customer(self.payload, self.shop)
        self.assertTrue(result.success)
        self.assertTrue(result.details['created'])
        self.assertTrue(result.details['append_only'])
        self.client.customer_create.assert_called_once()
        customer_input = self.client.customer_create.call_args.kwargs['input_data']
        self.assertEqual(customer_input['firstName'], 'POS name')
        self.assertEqual(customer_input['tags'], ['POS import'])
        self.assertEqual(customer_input['note'], 'POS note')
        self.assertEqual(customer_input['addresses'][0]['address1'], '123 Sample Street')
        self.assertIn({
            'namespace': 'pos', 'key': 'legacy_customer_id', 'type': 'id', 'value': 'Customer.dbf:123',
        }, customer_input['metafields'])
        self.assertTrue(all('ownerId' not in metafield for metafield in customer_input['metafields']))
        self.assertNotIn('id', customer_input)
        self.assertNotIn('emailMarketingConsent', customer_input)
        self.assert_no_customer_updates()

    def test_identity_lookup_failure_blocks_creation(self) -> None:
        self.client.get_customer_by_identifier.side_effect = ShopifyAPIError('Lookup unavailable')
        result = self.service.sync_customer(self.payload, self.shop)
        self.assertFalse(result.success)
        self.client.customer_create.assert_not_called()
        self.assert_no_customer_updates()

    def test_creation_conflict_rechecks_without_updating_the_concurrent_customer(self) -> None:
        self.client.get_customer_by_identifier.side_effect = [None, None, None, self.existing]
        self.client.customer_create.side_effect = ShopifyAPIError('Email already taken')
        result = self.service.sync_customer(self.payload, self.shop)
        self.assertTrue(result.success)
        self.assertTrue(result.details['skipped_existing'])
        self.assertEqual(result.details['metafield_count'], 0)
        self.client.customer_create.assert_called_once()
        self.assert_no_customer_updates()

    def test_creation_error_without_a_confirmed_match_remains_retryable(self) -> None:
        self.client.customer_create.side_effect = ShopifyAPIError('Create unavailable')
        result = self.service.sync_customer(self.payload, self.shop)
        self.assertFalse(result.success)
        self.assert_no_customer_updates()

    def test_failure_reason_describes_customer_data_denial_without_contact_details(self) -> None:
        self.client.get_customer_by_identifier.side_effect = ShopifyAPIError("Lookup failed", {
            "errors": [{"message": "Access denied to sample@example.test", "extensions": {"code": "ACCESS_DENIED"}}],
        })
        result = self.service.sync_customer(self.payload, self.shop)
        self.assertFalse(result.success)
        self.assertIn("protected customer data", result.failure_reason)
        self.assertNotIn("sample@example.test", result.failure_reason)
        self.client.customer_create.assert_not_called()

    def test_address_failure_points_to_the_exact_submitted_address(self) -> None:
        payload = CustomerSyncRequest.model_validate({**self.payload.model_dump(), "addresses": [
            {}, {"address1": "123 First Street", "zip": "10001"},
            {"address1": "456 Second Street", "zip": "99999", "phone": "+15551234567"},
        ]})
        self.client.customer_create.side_effect = ShopifyAPIError("Shopify rejected this customer", {
            "user_errors": [{"field": ["input", "addresses", "1", "zip"], "message": "ZIP code does not match the state"}],
        })
        result = self.service.sync_customer(payload, self.shop)
        self.assertFalse(result.success)
        self.assertEqual(result.address_review["issues"][0]["address_number"], 2)
        self.assertEqual(result.address_review["issues"][0]["field"], "input.addresses.1.zip")
        self.assertEqual(result.address_review["addresses"], [
            {"number": 2, "address1": "456 Second Street", "zip": "99999", "countryCode": "US"},
        ])
        self.assertNotIn("address_review", result.model_dump())
        self.assertNotIn("456 Second Street", result.model_dump_json())

    def test_invalid_identity_cannot_create_a_name_only_duplicate(self) -> None:
        result = self.service.sync_customer(CustomerSyncRequest(firstName='Name only'), self.shop)
        self.assertFalse(result.success)
        self.assertEqual(result.details, {'source': None})
        self.client.get_customer_by_identifier.assert_not_called()
        self.client.customer_create.assert_not_called()

    def test_payload_cannot_override_the_stable_pos_identity_or_target_an_existing_id(self) -> None:
        payload = self.payload.model_copy(update={
            'metafields': [
                self.payload.metafields[0].model_copy(update={
                    'key': 'legacy_customer_id', 'value': 'another-pos-id',
                }),
            ],
        })
        payload = CustomerSyncRequest.model_validate({
            **payload.model_dump(), 'id': self.existing['id'],
            'emailMarketingConsent': {'marketingState': 'SUBSCRIBED'},
        })
        self.assertTrue(self.service.sync_customer(payload, self.shop).success)
        customer_input = self.client.customer_create.call_args.kwargs['input_data']
        self.assertEqual(customer_input['metafields'], [{
            'namespace': 'pos', 'key': 'legacy_customer_id', 'type': 'id', 'value': 'Customer.dbf:123',
        }])
        self.assertNotIn('id', customer_input)
        self.assertNotIn('emailMarketingConsent', customer_input)

    def test_replaying_a_pos_id_only_customer_skips_the_created_record(self) -> None:
        payload = CustomerSyncRequest(pos_customer_number='123', firstName='New Customer')
        self.client.get_customer_by_identifier.side_effect = [None, self.existing]
        first = self.service.sync_customer(payload, self.shop)
        second = self.service.sync_customer(payload, self.shop)
        self.assertTrue(first.details['created'])
        self.assertTrue(second.details['skipped_existing'])
        self.client.customer_create.assert_called_once()
        self.assert_no_customer_updates()

    def test_bulk_counts_preserved_customers_as_processed(self) -> None:
        self.client.get_customer_by_identifier.return_value = self.existing
        result = self.service.sync_customers_bulk([self.payload], self.shop)
        self.assertEqual((result.succeeded, result.failed), (1, 0))
        self.assertTrue(result.results[0].details['skipped_existing'])
        self.client.customer_create.assert_not_called()
        self.assert_no_customer_updates()


class AppendOnlyCustomerClientTests(unittest.TestCase):
    def setUp(self) -> None:
        self.client = ShopifyClient(SimpleNamespace())

    def test_lookup_uses_read_only_identifier_query_with_versioned_input_names(self) -> None:
        for identifier in ({'emailAddress': 'sample@example.test'}, {'phoneNumber': '+15551234567'}):
            with self.subTest(identifier=identifier):
                with mock.patch.object(self.client, 'graphql', return_value={
                    'data': {'customerByIdentifier': None},
                }) as graphql:
                    self.assertIsNone(self.client.get_customer_by_identifier('shop', 'token', identifier=identifier))
                self.assertIn('query CustomerByIdentifier', graphql.call_args.args[2])
                self.assertEqual(graphql.call_args.args[3], {'identifier': identifier})

    def test_creation_uses_customer_create_without_an_update_identifier(self) -> None:
        customer = {'id': 'gid://shopify/Customer/1'}
        with mock.patch.object(self.client, 'graphql', return_value={
            'data': {'customerCreate': {'customer': customer, 'userErrors': []}},
        }) as graphql:
            result = self.client.customer_create('shop', 'token', input_data={'firstName': 'Sample'})
        self.assertEqual(result, customer)
        self.assertEqual(graphql.call_args.kwargs['operation_name'], 'CustomerCreate')
        self.assertNotIn('customerSet', graphql.call_args.args[2])
        self.assertEqual(graphql.call_args.args[3], {'input': {'firstName': 'Sample'}})

    def test_malformed_lookup_and_failed_creations_are_not_treated_as_success(self) -> None:
        with mock.patch.object(self.client, 'graphql', return_value={'data': {}}):
            with self.assertRaises(KeyError):
                self.client.get_customer_by_identifier('shop', 'token', identifier={'emailAddress': 'sample@example.test'})
        for response in (
            {'customer': None, 'userErrors': [{'field': ['email'], 'message': 'Already taken'}]},
            {'customer': None, 'userErrors': []},
        ):
            with self.subTest(response=response):
                with mock.patch.object(self.client, 'graphql', return_value={'data': {'customerCreate': response}}):
                    with self.assertRaises(ShopifyAPIError):
                        self.client.customer_create('shop', 'token', input_data={'firstName': 'Sample'})


if __name__ == '__main__':
    unittest.main()
