import io
import os
import sqlite3
import subprocess
import sys
import tempfile
import textwrap
import unittest
import zipfile
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from pathlib import Path

from app.db import DatabaseStore
from app.installer import (
    RUNTIME_FILES,
    build_personalized_installer_bat,
    build_runtime_archive,
    setup_script_bytes,
    sha256_hex,
)
from app.utils import AuthorizationError, SyncProcessingError
from windows_connector.connector import bind_state_identity


SHOP = "first-shop.myshopify.com"
OTHER_SHOP = "other-shop.myshopify.com"


def install_shop(store: DatabaseStore, shop_domain: str = SHOP) -> None:
    store.upsert_shop(
        shop_domain=shop_domain,
        access_token=f"token-for-{shop_domain}",
        scope="read_products,write_products",
        refresh_token=None,
        access_token_expires_at=None,
        refresh_token_expires_at=None,
        shop_name=shop_domain.split(".", 1)[0],
        myshopify_domain=shop_domain,
    )


class ConnectorEnrollmentTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.store = DatabaseStore(
            str(Path(self.temporary_directory.name) / "sync.sqlite3"),
            "test-encryption-secret",
        )
        install_shop(self.store)

    def tearDown(self) -> None:
        self.temporary_directory.cleanup()

    def test_existing_unique_installation_schema_is_migrated(self):
        legacy_path = Path(self.temporary_directory.name) / "legacy.sqlite3"
        with sqlite3.connect(legacy_path) as connection:
            connection.execute(
                """
                CREATE TABLE connector_devices (
                    device_id TEXT PRIMARY KEY,
                    shop_domain TEXT NOT NULL,
                    installation_id TEXT UNIQUE NOT NULL,
                    api_key TEXT UNIQUE NOT NULL,
                    secret_salt TEXT NOT NULL,
                    secret_hash TEXT NOT NULL,
                    status TEXT NOT NULL,
                    connector_version TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    activated_at TEXT,
                    last_used_at TEXT,
                    revoked_at TEXT
                )
                """
            )
            connection.commit()

        DatabaseStore(str(legacy_path), "test-encryption-secret")

        with sqlite3.connect(legacy_path) as connection:
            unique_indexes = [
                row[1]
                for row in connection.execute("PRAGMA index_list(connector_devices)")
                if row[2]
            ]
            unique_column_sets = [
                [column[2] for column in connection.execute(f'PRAGMA index_info("{name}")')]
                for name in unique_indexes
            ]
            columns = {
                row[1]
                for row in connection.execute("PRAGMA table_info(connector_devices)")
            }
        self.assertNotIn(["installation_id"], unique_column_sets)
        self.assertIn("activation_expires_at", columns)

    def test_enrollment_stores_only_a_digest_and_is_idempotent_for_same_device(self):
        enrollment = self.store.create_connector_enrollment(
            SHOP,
            artifact_version="1.2.0",
        )
        with sqlite3.connect(self.store.database_path) as connection:
            stored = connection.execute(
                "SELECT token_digest FROM connector_enrollments WHERE enrollment_id = ?",
                (enrollment.enrollment_id,),
            ).fetchone()[0]
        self.assertNotEqual(stored, enrollment.token)
        self.assertEqual(len(stored), 64)

        first = self.store.redeem_connector_enrollment(
            token=enrollment.token,
            installation_id="win_11111111111111111111111111111111",
            device_secret="sec_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            connector_version="1.2.0",
        )
        retry = self.store.redeem_connector_enrollment(
            token=enrollment.token,
            installation_id="win_11111111111111111111111111111111",
            device_secret="sec_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            connector_version="1.2.0",
        )
        self.assertEqual(retry, first)
        self.assertEqual(first.status, "pending")

    def test_redeemed_token_rejects_a_different_device_or_secret(self):
        enrollment = self.store.create_connector_enrollment(SHOP, artifact_version="1.2.0")
        self.store.redeem_connector_enrollment(
            token=enrollment.token,
            installation_id="win_11111111111111111111111111111111",
            device_secret="sec_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            connector_version="1.2.0",
        )
        with self.assertRaises(AuthorizationError):
            self.store.redeem_connector_enrollment(
                token=enrollment.token,
                installation_id="win_22222222222222222222222222222222",
                device_secret="sec_bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                connector_version="1.2.0",
            )

    def test_expired_token_is_rejected(self):
        enrollment = self.store.create_connector_enrollment(SHOP, artifact_version="1.2.0")
        with sqlite3.connect(self.store.database_path) as connection:
            connection.execute(
                "UPDATE connector_enrollments SET expires_at = '2000-01-01T00:00:00+00:00'"
            )
            connection.commit()
        with self.assertRaises(AuthorizationError):
            self.store.redeem_connector_enrollment(
                token=enrollment.token,
                installation_id="win_11111111111111111111111111111111",
                device_secret="sec_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                connector_version="1.2.0",
            )

    def test_pending_activation_expires_thirty_minutes_after_redemption(self):
        enrollment = self.store.create_connector_enrollment(SHOP, artifact_version="1.2.0")
        secret = "sec_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        device = self.store.redeem_connector_enrollment(
            token=enrollment.token,
            installation_id="win_11111111111111111111111111111111",
            device_secret=secret,
            connector_version="1.2.0",
        )
        with sqlite3.connect(self.store.database_path) as connection:
            redeemed_at, activation_expires_at = connection.execute(
                """
                SELECT connector_enrollments.redeemed_at,
                       connector_devices.activation_expires_at
                FROM connector_enrollments
                JOIN connector_devices
                    ON connector_devices.device_id = connector_enrollments.device_id
                WHERE connector_enrollments.enrollment_id = ?
                """,
                (enrollment.enrollment_id,),
            ).fetchone()
            self.assertEqual(
                datetime.fromisoformat(activation_expires_at)
                - datetime.fromisoformat(redeemed_at),
                timedelta(minutes=30),
            )
            connection.execute(
                """
                UPDATE connector_devices
                SET activation_expires_at = '2000-01-01T00:00:00+00:00'
                WHERE device_id = ?
                """,
                (device.device_id,),
            )
            connection.commit()

        with self.assertRaises(AuthorizationError):
            self.store.activate_connector_device(api_key=device.api_key, device_secret=secret)
        self.assertEqual(
            self.store.get_connector_device_for_api_key(device.api_key).status,
            "revoked",
        )

    def test_active_device_activation_remains_idempotent_after_setup_window(self):
        enrollment = self.store.create_connector_enrollment(SHOP, artifact_version="1.2.0")
        secret = "sec_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        device = self.store.redeem_connector_enrollment(
            token=enrollment.token,
            installation_id="win_11111111111111111111111111111111",
            device_secret=secret,
            connector_version="1.2.0",
        )
        activated = self.store.activate_connector_device(
            api_key=device.api_key,
            device_secret=secret,
        )
        with sqlite3.connect(self.store.database_path) as connection:
            connection.execute(
                """
                UPDATE connector_devices
                SET activation_expires_at = '2000-01-01T00:00:00+00:00'
                WHERE device_id = ?
                """,
                (device.device_id,),
            )
            connection.commit()

        retry = self.store.activate_connector_device(
            api_key=device.api_key,
            device_secret=secret,
        )
        self.assertEqual(retry.device_id, activated.device_id)
        self.assertEqual(retry.status, "active")

    def test_new_installer_revokes_an_older_pending_device(self):
        first_enrollment = self.store.create_connector_enrollment(SHOP, artifact_version="1.2.0")
        first_secret = "sec_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        first = self.store.redeem_connector_enrollment(
            token=first_enrollment.token,
            installation_id="win_11111111111111111111111111111111",
            device_secret=first_secret,
            connector_version="1.2.0",
        )

        self.store.create_connector_enrollment(SHOP, artifact_version="1.2.0")

        self.assertEqual(
            self.store.get_connector_device_for_api_key(first.api_key).status,
            "revoked",
        )
        with self.assertRaises(AuthorizationError):
            self.store.activate_connector_device(
                api_key=first.api_key,
                device_secret=first_secret,
            )

    def test_only_one_concurrent_redemption_can_claim_a_token(self):
        enrollment = self.store.create_connector_enrollment(SHOP, artifact_version="1.2.0")

        def redeem(index: int) -> str:
            try:
                self.store.redeem_connector_enrollment(
                    token=enrollment.token,
                    installation_id=f"win_{index:032d}",
                    device_secret=f"sec_{index:043d}",
                    connector_version="1.2.0",
                )
                return "ok"
            except AuthorizationError:
                return "rejected"

        with ThreadPoolExecutor(max_workers=2) as executor:
            results = list(executor.map(redeem, (1, 2)))
        self.assertCountEqual(results, ["ok", "rejected"])

    def test_activation_replaces_the_previous_device_without_a_gap(self):
        first_enrollment = self.store.create_connector_enrollment(SHOP, artifact_version="1.2.0")
        first_secret = "sec_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        first = self.store.redeem_connector_enrollment(
            token=first_enrollment.token,
            installation_id="win_11111111111111111111111111111111",
            device_secret=first_secret,
            connector_version="1.2.0",
        )
        self.store.activate_connector_device(api_key=first.api_key, device_secret=first_secret)

        second_enrollment = self.store.create_connector_enrollment(SHOP, artifact_version="1.2.0")
        second_secret = "sec_bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
        second = self.store.redeem_connector_enrollment(
            token=second_enrollment.token,
            installation_id="win_11111111111111111111111111111111",
            device_secret=second_secret,
            connector_version="1.2.0",
        )
        self.assertEqual(self.store.get_active_connector_device(SHOP).device_id, first.device_id)
        self.store.record_connector_heartbeat(shop_domain=SHOP, channel="inventory")
        self.assertIsNotNone(self.store.get_connector_heartbeat(shop_domain=SHOP)["last_seen_at"])

        active = self.store.activate_connector_device(
            api_key=second.api_key,
            device_secret=second_secret,
        )
        self.assertEqual(active.status, "active")
        self.assertEqual(self.store.get_active_connector_device(SHOP).device_id, second.device_id)
        self.assertIsNone(self.store.get_connector_heartbeat(shop_domain=SHOP)["last_seen_at"])
        with self.assertRaises(AuthorizationError):
            self.store.verify_connector_device_credentials(first.api_key, first_secret)

    def test_managed_device_blocks_reissuing_legacy_shared_credentials(self):
        self.store.ensure_pos_credentials(SHOP)
        enrollment = self.store.create_connector_enrollment(SHOP, artifact_version="1.2.0")
        secret = "sec_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        device = self.store.redeem_connector_enrollment(
            token=enrollment.token,
            installation_id="win_11111111111111111111111111111111",
            device_secret=secret,
            connector_version="1.2.0",
        )
        self.store.activate_connector_device(api_key=device.api_key, device_secret=secret)

        self.assertIsNone(self.store.get_pos_credentials_for_shop(SHOP))
        with self.assertRaises(SyncProcessingError):
            self.store.ensure_pos_credentials(SHOP)
        with self.assertRaises(SyncProcessingError):
            self.store.rotate_pos_credentials(SHOP)

    def test_revoke_device_is_shop_scoped_and_clears_heartbeat(self):
        install_shop(self.store, OTHER_SHOP)
        enrollment = self.store.create_connector_enrollment(SHOP, artifact_version="1.2.0")
        secret = "sec_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        device = self.store.redeem_connector_enrollment(
            token=enrollment.token,
            installation_id="win_11111111111111111111111111111111",
            device_secret=secret,
            connector_version="1.2.0",
        )
        self.store.activate_connector_device(api_key=device.api_key, device_secret=secret)
        self.store.record_connector_heartbeat(shop_domain=SHOP, channel="inventory")

        self.assertFalse(
            self.store.revoke_connector_device(
                shop_domain=OTHER_SHOP,
                device_id=device.device_id,
            )
        )
        self.assertEqual(self.store.get_active_connector_device(SHOP).device_id, device.device_id)
        self.assertTrue(
            self.store.revoke_connector_device(
                shop_domain=SHOP,
                device_id=device.device_id,
            )
        )
        self.assertIsNone(self.store.get_active_connector_device(SHOP))
        self.assertIsNone(self.store.get_connector_heartbeat(shop_domain=SHOP)["last_seen_at"])
        with self.assertRaises(AuthorizationError):
            self.store.verify_connector_device_credentials(device.api_key, secret)

    def test_uninstall_revokes_devices_and_pending_enrollments(self):
        enrollment = self.store.create_connector_enrollment(SHOP, artifact_version="1.2.0")
        secret = "sec_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        device = self.store.redeem_connector_enrollment(
            token=enrollment.token,
            installation_id="win_11111111111111111111111111111111",
            device_secret=secret,
            connector_version="1.2.0",
        )
        self.store.activate_connector_device(api_key=device.api_key, device_secret=secret)
        pending = self.store.create_connector_enrollment(SHOP, artifact_version="1.2.0")

        self.store.mark_shop_uninstalled(SHOP)

        self.assertIsNone(self.store.get_active_connector_device(SHOP))
        with self.assertRaises(AuthorizationError):
            self.store.redeem_connector_enrollment(
                token=pending.token,
                installation_id="win_22222222222222222222222222222222",
                device_secret="sec_bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                connector_version="1.2.0",
            )

    def test_shop_request_logs_do_not_include_unattributed_other_requests(self):
        for shop_domain in (SHOP, None):
            self.store.record_request_log(
                shop_domain=shop_domain,
                api_key_preview=None,
                method="POST",
                path="/sync/test",
                query_string=None,
                status_code=200,
                route_path="/sync/test",
                request_body="{}",
                user_agent="test",
                source_ip="127.0.0.1",
                duration_ms=1,
            )
        rows = self.store.list_request_logs(shop_domain=SHOP)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].shop_domain, SHOP)
        self.assertEqual(self.store.request_log_count(shop_domain=SHOP), 1)

    def test_legacy_location_override_is_attached_only_to_one_existing_shop(self):
        self.assertTrue(self.store.seed_single_shop_inventory_location("gid://shopify/Location/1"))
        self.assertEqual(
            self.store.get_shop(SHOP).inventory_location_id,
            "gid://shopify/Location/1",
        )
        install_shop(self.store, OTHER_SHOP)
        self.assertFalse(self.store.seed_single_shop_inventory_location("gid://shopify/Location/2"))
        self.assertIsNone(self.store.get_shop(OTHER_SHOP).inventory_location_id)

    def test_inventory_webhook_enqueue_deduplicates_shopify_delivery_id(self):
        payload = b'{"inventory_item_id":1,"location_id":2,"available":3}'
        first, first_created = self.store.enqueue_inventory_webhook_event(
            shop_domain=SHOP,
            webhook_id="webhook-delivery-1",
            raw_payload=payload,
        )
        retry, retry_created = self.store.enqueue_inventory_webhook_event(
            shop_domain=SHOP,
            webhook_id="webhook-delivery-1",
            raw_payload=payload,
        )

        self.assertTrue(first_created)
        self.assertFalse(retry_created)
        self.assertEqual(retry.id, first.id)
        self.assertEqual(first.raw_payload, payload)
        self.assertEqual(
            self.store.inventory_webhook_event_count(shop_domain=SHOP),
            1,
        )

    def test_inventory_webhook_queue_survives_restart_and_retries_durably(self):
        payload = b'{"inventory_item_id":1,"location_id":2,"available":3}'
        queued, created = self.store.enqueue_inventory_webhook_event(
            shop_domain=SHOP,
            webhook_id="webhook-delivery-2",
            raw_payload=payload,
        )
        self.assertTrue(created)

        reopened = DatabaseStore(str(self.store.database_path), "test-encryption-secret")
        first_claim = reopened.claim_inventory_webhook_events(shop_domain=SHOP)
        self.assertEqual([event.id for event in first_claim], [queued.id])
        self.assertEqual(first_claim[0].raw_payload, payload)
        self.assertEqual(first_claim[0].attempts, 1)
        self.assertTrue(
            reopened.retry_inventory_webhook_event(
                event_id=first_claim[0].id,
                lease_token=first_claim[0].lease_token,
                error="temporary Shopify failure",
                retry_delay_seconds=0,
            )
        )

        reopened_again = DatabaseStore(str(self.store.database_path), "test-encryption-secret")
        second_claim = reopened_again.claim_inventory_webhook_events(shop_domain=SHOP)
        self.assertEqual([event.id for event in second_claim], [queued.id])
        self.assertEqual(second_claim[0].attempts, 2)
        self.assertEqual(second_claim[0].last_error, "temporary Shopify failure")
        self.assertTrue(
            reopened_again.complete_inventory_webhook_event(
                event_id=second_claim[0].id,
                lease_token=second_claim[0].lease_token,
                result="queued",
            )
        )
        self.assertEqual(
            reopened_again.inventory_webhook_event_count(
                shop_domain=SHOP,
                status="completed",
            ),
            1,
        )
        self.assertEqual(
            reopened_again.claim_inventory_webhook_events(shop_domain=SHOP),
            [],
        )

    def test_older_webhook_retry_cannot_regress_latest_quantity_observation(self):
        item_id = "gid://shopify/InventoryItem/1"
        self.assertTrue(
            self.store.upsert_inventory_item_quantity(
                shop_domain=SHOP,
                inventory_item_id=item_id,
                sku="ABC",
                quantity=8,
                source_updated_at="2026-09-04T12:02:00+00:00",
            )
        )
        self.assertFalse(
            self.store.upsert_inventory_item_quantity(
                shop_domain=SHOP,
                inventory_item_id=item_id,
                sku="ABC",
                quantity=0,
                source_updated_at="2026-09-04T12:01:00+00:00",
            )
        )
        self.assertEqual(
            self.store.get_inventory_item_quantity_observation(
                shop_domain=SHOP,
                inventory_item_id=item_id,
            ),
            (8, "2026-09-04T12:02:00+00:00"),
        )

    def test_timestamp_less_authoritative_write_preserves_quantity_watermark(self):
        item_id = "gid://shopify/InventoryItem/1"
        common = {
            "shop_domain": SHOP,
            "inventory_item_id": item_id,
            "sku": "ABC",
        }
        self.assertTrue(
            self.store.upsert_inventory_item_quantity(
                **common,
                quantity=8,
                source_updated_at="2026-09-04T12:02:00+00:00",
            )
        )
        self.assertTrue(
            self.store.upsert_inventory_item_quantity(
                **common,
                quantity=9,
            )
        )
        self.assertEqual(
            self.store.get_inventory_item_quantity_observation(
                shop_domain=SHOP,
                inventory_item_id=item_id,
            ),
            (9, "2026-09-04T12:02:00+00:00"),
        )
        self.assertFalse(
            self.store.upsert_inventory_item_quantity(
                **common,
                quantity=0,
                source_updated_at="2026-09-04T12:01:00+00:00",
            )
        )
        self.assertEqual(
            self.store.get_inventory_item_quantity_observation(
                shop_domain=SHOP,
                inventory_item_id=item_id,
            ),
            (9, "2026-09-04T12:02:00+00:00"),
        )

    def test_timestamp_less_queued_change_cannot_replace_versioned_change(self):
        common = {
            "shop_domain": SHOP,
            "inventory_item_id": "gid://shopify/InventoryItem/1",
            "location_id": "gid://shopify/Location/2",
            "sku": "ABC",
        }
        self.store.upsert_inventory_change(
            **common,
            quantity=8,
            source_updated_at="2026-09-04T12:02:00+00:00",
        )
        self.store.upsert_inventory_change(**common, quantity=9)
        self.store.upsert_inventory_change(
            **common,
            quantity=0,
            source_updated_at="2026-09-04T12:01:00+00:00",
        )

        queued = self.store.list_inventory_changes(shop_domain=SHOP)[0]
        self.assertEqual(queued.quantity, 8)
        self.assertEqual(queued.version, 1)
        self.assertEqual(
            queued.source_updated_at,
            "2026-09-04T12:02:00+00:00",
        )

    def test_matrix_repair_persists_normalized_inventory_timestamp(self):
        database_path = Path(self.temporary_directory.name) / "matrix-repair.sqlite3"
        script = textwrap.dedent(
            """
            import asyncio
            import json

            from starlette.requests import Request

            import app.main as main

            async def inline_to_thread(function, *args, **kwargs):
                return function(*args, **kwargs)

            main.asyncio.to_thread = inline_to_thread

            shop_domain = "first-shop.myshopify.com"
            item_id = "gid://shopify/InventoryItem/1"
            main.db.upsert_shop(
                shop_domain=shop_domain,
                access_token="token",
                scope="read_inventory,write_inventory",
                refresh_token=None,
                access_token_expires_at=None,
                refresh_token_expires_at=None,
                shop_name="First Shop",
                myshopify_domain=shop_domain,
            )
            shop = main.db.get_shop(shop_domain)
            assert shop is not None

            class MatrixRepair:
                def repair_matrix_structure(self, payload, active_shop):
                    assert active_shop.shop_domain == shop_domain
                    assert payload.sku == "BASE"
                    return {
                        "base_sku": "BASE",
                        "status": "already_correct",
                        "message": "Already correct.",
                        "product_id": "gid://shopify/Product/1",
                        "variants": [
                            {
                                "sku": "CHILD",
                                "inventory_item_id": item_id,
                                "quantity": 9,
                                "inventory_level_updated_at": "2026-09-04T05:02:00-07:00",
                            }
                        ],
                    }

            main.inventory_service = MatrixRepair()
            body = json.dumps(
                {
                    "items": [
                        {
                            "sku": "BASE",
                            "variants": [
                                {"sku": "CHILD", "barcode": "CHILD", "quantity": 9}
                            ],
                        }
                    ]
                }
            ).encode("utf-8")

            async def invoke():
                sent = False

                async def receive():
                    nonlocal sent
                    if sent:
                        return {"type": "http.disconnect"}
                    sent = True
                    return {"type": "http.request", "body": body, "more_body": False}

                request = Request(
                    {
                        "type": "http",
                        "method": "POST",
                        "path": "/sync/catalog/matrix-structure/repair",
                        "query_string": b"",
                        "headers": [(b"content-type", b"application/json")],
                    },
                    receive,
                )
                return await main.repair_catalog_matrix_structure(request, shop)

            response = asyncio.run(invoke())
            assert response.status_code == 200
            assert main.db.get_inventory_item_quantity_observation(
                shop_domain=shop_domain,
                inventory_item_id=item_id,
            ) == (9, "2026-09-04T12:02:00+00:00")

            assert main.db.upsert_inventory_item_quantity(
                shop_domain=shop_domain,
                inventory_item_id=item_id,
                sku="CHILD",
                quantity=10,
            )
            assert main.db.get_inventory_item_quantity_observation(
                shop_domain=shop_domain,
                inventory_item_id=item_id,
            ) == (10, "2026-09-04T12:02:00+00:00")
            assert not main.db.upsert_inventory_item_quantity(
                shop_domain=shop_domain,
                inventory_item_id=item_id,
                sku="CHILD",
                quantity=0,
                source_updated_at="2026-09-04T12:01:00+00:00",
            )
            assert main.db.get_inventory_item_quantity_observation(
                shop_domain=shop_domain,
                inventory_item_id=item_id,
            ) == (10, "2026-09-04T12:02:00+00:00")
            """
        )
        environment = os.environ.copy()
        environment.update(
            {
                "SHOPIFY_CLIENT_ID": "test-client",
                "SHOPIFY_CLIENT_SECRET": "test-secret",
                "APP_BASE_URL": "https://sync.example.test",
                "APP_SESSION_SECRET": "test-session-secret",
                "POS_SECRET_ENCRYPTION_SECRET": "test-encryption-secret",
                "DATABASE_PATH": str(database_path),
            }
        )
        completed = subprocess.run(
            [sys.executable, "-c", script],
            cwd=Path(__file__).resolve().parents[1],
            env=environment,
            capture_output=True,
            text=True,
            check=False,
            timeout=10,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr or completed.stdout)

    def test_inventory_webhook_route_only_enqueues_before_durable_processing(self):
        database_path = Path(self.temporary_directory.name) / "webhook-route.sqlite3"
        script = textwrap.dedent(
            """
            import asyncio
            import base64
            import hashlib
            import hmac
            import json
            import sys

            from fastapi import BackgroundTasks
            from starlette.requests import Request

            import app.main as main

            shop = "first-shop.myshopify.com"
            main.db.upsert_shop(
                shop_domain=shop,
                access_token="token",
                scope="read_inventory,write_inventory",
                refresh_token=None,
                access_token_expires_at=None,
                refresh_token_expires_at=None,
                shop_name="First Shop",
                myshopify_domain=shop,
            )
            main.db.set_shop_inventory_location(
                shop_domain=shop,
                location_id="gid://shopify/Location/2",
            )
            main.db.upsert_inventory_item_quantity(
                shop_domain=shop,
                inventory_item_id="gid://shopify/InventoryItem/1",
                sku="ABC",
                quantity=2,
                source_updated_at="2026-09-04T12:00:00+00:00",
            )

            class NoNetworkCalls:
                def __getattr__(self, name):
                    raise AssertionError(f"Unexpected external call: {name}")

            main.shopify_client = NoNetworkCalls()
            main.inventory_service = NoNetworkCalls()
            body = json.dumps(
                {
                    "inventory_item_id": 1,
                    "location_id": 2,
                    "available": 3,
                    "updated_at": "2026-09-04T12:01:00Z",
                },
                separators=(",", ":"),
            ).encode("utf-8")
            signature = base64.b64encode(
                hmac.new(b"test-secret", body, hashlib.sha256).digest()
            ).decode("ascii")

            async def invoke():
                sent = False

                async def receive():
                    nonlocal sent
                    if sent:
                        return {"type": "http.disconnect"}
                    sent = True
                    return {"type": "http.request", "body": body, "more_body": False}

                request = Request(
                    {
                        "type": "http",
                        "method": "POST",
                        "path": "/webhooks/inventory-levels-update",
                        "query_string": b"",
                        "headers": [
                            (b"x-shopify-hmac-sha256", signature.encode("ascii")),
                            (b"x-shopify-shop-domain", shop.encode("ascii")),
                            (b"x-shopify-webhook-id", b"delivery-1"),
                        ],
                    },
                    receive,
                )
                return await main.inventory_levels_update_webhook(
                    request,
                    BackgroundTasks(),
                )

            first = asyncio.run(invoke())
            second = asyncio.run(invoke())
            assert first.status_code == 200
            assert json.loads(first.body)["status"] == "queued"
            assert json.loads(second.body)["status"] == "duplicate"
            assert main.db.inventory_webhook_event_count(shop_domain=shop) == 1
            assert main.db.list_inventory_changes(shop_domain=shop) == []

            result = main.drain_inventory_webhook_events(shop_domain=shop)
            assert result == {"claimed": 1, "completed": 1, "retried": 0}
            changes = main.db.list_inventory_changes(shop_domain=shop)
            assert len(changes) == 1 and changes[0].quantity == 3
            observation = main.db.get_inventory_item_quantity_observation(
                shop_domain=shop,
                inventory_item_id="gid://shopify/InventoryItem/1",
            )
            assert observation == (3, "2026-09-04T12:01:00+00:00"), observation

            main.db.upsert_inventory_item_quantity(
                shop_domain=shop,
                inventory_item_id="gid://shopify/InventoryItem/1",
                sku="ABC",
                quantity=0,
                source_updated_at="2026-09-04T12:03:00+00:00",
            )
            stale_body = json.dumps(
                {
                    "inventory_item_id": 1,
                    "location_id": 2,
                    "available": 4,
                    "updated_at": "2026-09-04T12:02:00Z",
                },
                separators=(",", ":"),
            ).encode("utf-8")
            main.db.enqueue_inventory_webhook_event(
                shop_domain=shop,
                webhook_id="delivery-stale-positive",
                raw_payload=stale_body,
            )
            stale_result = main.drain_inventory_webhook_events(shop_domain=shop)
            assert stale_result == {"claimed": 1, "completed": 1, "retried": 0}
            stale_observation = main.db.get_inventory_item_quantity_observation(
                shop_domain=shop,
                inventory_item_id="gid://shopify/InventoryItem/1",
            )
            assert stale_observation == (0, "2026-09-04T12:03:00+00:00"), stale_observation

            unversioned_body = json.dumps(
                {
                    "inventory_item_id": 1,
                    "location_id": 2,
                    "available": 5,
                },
                separators=(",", ":"),
            ).encode("utf-8")
            main.db.enqueue_inventory_webhook_event(
                shop_domain=shop,
                webhook_id="delivery-unversioned-positive",
                raw_payload=unversioned_body,
            )
            unversioned_result = main.drain_inventory_webhook_events(shop_domain=shop)
            assert unversioned_result == {"claimed": 1, "completed": 1, "retried": 0}
            assert main.db.get_inventory_item_quantity_observation(
                shop_domain=shop,
                inventory_item_id="gid://shopify/InventoryItem/1",
            ) == (0, "2026-09-04T12:03:00+00:00")
            assert main.db.list_inventory_changes(shop_domain=shop)[0].quantity == 3
            """
        )
        environment = os.environ.copy()
        environment.update(
            {
                "SHOPIFY_CLIENT_ID": "test-client",
                "SHOPIFY_CLIENT_SECRET": "test-secret",
                "APP_BASE_URL": "https://sync.example.test",
                "APP_SESSION_SECRET": "test-session-secret",
                "POS_SECRET_ENCRYPTION_SECRET": "test-encryption-secret",
                "DATABASE_PATH": str(database_path),
            }
        )
        completed = subprocess.run(
            [sys.executable, "-c", script],
            cwd=Path(__file__).resolve().parents[1],
            env=environment,
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr or completed.stdout)

    def test_concurrent_inventory_drains_preserve_causal_order(self):
        database_path = Path(self.temporary_directory.name) / "concurrent-webhooks.sqlite3"
        script = textwrap.dedent(
            """
            import json
            import sqlite3
            import threading

            import app.main as main

            shop = "first-shop.myshopify.com"
            item_id = "gid://shopify/InventoryItem/1"
            main.db.upsert_shop(
                shop_domain=shop,
                access_token="token",
                scope="read_inventory,write_inventory",
                refresh_token=None,
                access_token_expires_at=None,
                refresh_token_expires_at=None,
                shop_name="First Shop",
                myshopify_domain=shop,
            )
            main.db.set_shop_inventory_location(
                shop_domain=shop,
                location_id="gid://shopify/Location/2",
            )
            main.db.upsert_inventory_item_quantity(
                shop_domain=shop,
                inventory_item_id=item_id,
                sku="ABC",
                quantity=0,
                source_updated_at="2026-09-04T12:00:00+00:00",
            )

            class NoNetworkCalls:
                def __getattr__(self, name):
                    raise AssertionError(f"Unexpected external call: {name}")

            class BlockingRepair:
                def __init__(self):
                    self.calls = 0
                    self.entered = threading.Event()
                    self.release = threading.Event()

                def restore_auto_archived_product_by_sku(self, **kwargs):
                    self.calls += 1
                    if self.calls != 1:
                        raise AssertionError("A stale positive retry attempted status repair")
                    self.entered.set()
                    assert self.release.wait(5), "Timed out waiting to release repair"
                    raise RuntimeError("temporary Shopify failure")

            main.shopify_client = NoNetworkCalls()
            repair = BlockingRepair()
            main.inventory_service = repair

            def payload(available, updated_at):
                return json.dumps(
                    {
                        "inventory_item_id": 1,
                        "location_id": 2,
                        "available": available,
                        "updated_at": updated_at,
                    },
                    separators=(",", ":"),
                ).encode("utf-8")

            older, _ = main.db.enqueue_inventory_webhook_event(
                shop_domain=shop,
                webhook_id="older-positive",
                raw_payload=payload(4, "2026-09-04T12:01:00Z"),
            )
            main.db.enqueue_inventory_webhook_event(
                shop_domain=shop,
                webhook_id="newer-zero",
                raw_payload=payload(0, "2026-09-04T12:02:00Z"),
            )

            first_result = {}

            def first_drain():
                first_result.update(
                    main.drain_inventory_webhook_events(shop_domain=shop, limit=1)
                )

            worker = threading.Thread(target=first_drain)
            worker.start()
            assert repair.entered.wait(5), "The first drain did not reach status repair"
            overlapping = main.drain_inventory_webhook_events(shop_domain=shop, limit=1)
            assert overlapping == {"claimed": 0, "completed": 0, "retried": 0}
            repair.release.set()
            worker.join(5)
            assert not worker.is_alive(), "The first drain did not finish"
            assert first_result == {"claimed": 1, "completed": 0, "retried": 1}

            with sqlite3.connect(main.db.database_path) as connection:
                connection.execute(
                    "UPDATE inventory_webhook_events "
                    "SET next_attempt_at = '2999-01-01T00:00:00+00:00' "
                    "WHERE id = ?",
                    (older.id,),
                )
                connection.commit()
            newer_result = main.drain_inventory_webhook_events(shop_domain=shop, limit=1)
            assert newer_result == {"claimed": 1, "completed": 1, "retried": 0}

            with sqlite3.connect(main.db.database_path) as connection:
                connection.execute(
                    "UPDATE inventory_webhook_events "
                    "SET next_attempt_at = '2000-01-01T00:00:00+00:00' "
                    "WHERE id = ?",
                    (older.id,),
                )
                connection.commit()
            retry_result = main.drain_inventory_webhook_events(shop_domain=shop, limit=1)
            assert retry_result == {"claimed": 1, "completed": 1, "retried": 0}
            assert repair.calls == 1
            assert main.db.get_inventory_item_quantity_observation(
                shop_domain=shop,
                inventory_item_id=item_id,
            ) == (0, "2026-09-04T12:02:00+00:00")
            """
        )
        environment = os.environ.copy()
        environment.update(
            {
                "SHOPIFY_CLIENT_ID": "test-client",
                "SHOPIFY_CLIENT_SECRET": "test-secret",
                "APP_BASE_URL": "https://sync.example.test",
                "APP_SESSION_SECRET": "test-session-secret",
                "POS_SECRET_ENCRYPTION_SECRET": "test-encryption-secret",
                "DATABASE_PATH": str(database_path),
            }
        )
        completed = subprocess.run(
            [sys.executable, "-c", script],
            cwd=Path(__file__).resolve().parents[1],
            env=environment,
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr or completed.stdout)

    def test_shop_inventory_location_is_persisted_per_tenant(self):
        install_shop(self.store, OTHER_SHOP)
        self.assertTrue(
            self.store.set_shop_inventory_location(
                shop_domain=SHOP,
                location_id="gid://shopify/Location/11",
            )
        )
        self.assertTrue(
            self.store.set_shop_inventory_location(
                shop_domain=OTHER_SHOP,
                location_id="gid://shopify/Location/22",
            )
        )
        self.assertEqual(
            self.store.get_shop(SHOP).inventory_location_id,
            "gid://shopify/Location/11",
        )
        self.assertEqual(
            self.store.get_shop(OTHER_SHOP).inventory_location_id,
            "gid://shopify/Location/22",
        )


class ConnectorInstallerArtifactTests(unittest.TestCase):
    def test_runtime_archive_is_deterministic_and_contains_only_the_manifest(self):
        first = build_runtime_archive()
        second = build_runtime_archive()
        self.assertEqual(first, second)
        with zipfile.ZipFile(io.BytesIO(first)) as archive:
            names = archive.namelist()
        self.assertEqual(
            set(names),
            {*RUNTIME_FILES, "windows_connector/requirements.txt"},
        )
        self.assertFalse(any(".." in Path(name).parts for name in names))
        self.assertFalse(any(name.lower().endswith(".dbf") for name in names))

    def test_personalized_bat_contains_one_time_token_but_no_permanent_secret(self):
        token = "pair_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
        bat = build_personalized_installer_bat(
            base_url="https://sync.example.com",
            shop_domain=SHOP,
            enrollment_token=token,
            setup_sha256=sha256_hex(setup_script_bytes()),
            runtime_sha256=sha256_hex(build_runtime_archive()),
        ).decode("utf-8")
        self.assertEqual(bat.count(token), 1)
        self.assertNotIn("SHOPIFY_SYNC_API_SECRET", bat)
        self.assertIn("DisableDelayedExpansion", bat)
        self.assertIn("Get-FileHash -Algorithm SHA256", bat)
        self.assertTrue(bat.endswith("\r\n"))

    def test_remote_installer_base_url_must_use_https(self):
        with self.assertRaises(ValueError):
            build_personalized_installer_bat(
                base_url="http://sync.example.com",
                shop_domain=SHOP,
                enrollment_token="pair_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
                setup_sha256="A" * 64,
                runtime_sha256="B" * 64,
            )

    def test_ambiguous_activation_never_restores_potentially_revoked_credentials(self):
        script = setup_script_bytes().decode("utf-8")
        activation = script.index('-Uri "$baseUrl/connector/v1/activate"')
        confirmation = script.index('-Uri "$baseUrl/api/connector/verify"', activation)
        guarded_rollback = script.index(
            "if ($installationMutationStarted -and -not $deviceActivated -and -not $activationMayHaveCommitted)"
        )
        self.assertLess(activation, confirmation)
        self.assertLess(confirmation, guarded_rollback)
        self.assertIn(
            "The verified new installation was retained to avoid restoring credentials that may have been revoked.",
            script,
        )

    def test_custom_legacy_data_directory_is_rejected_before_runtime_replacement(self):
        script = setup_script_bytes().decode("utf-8")
        compatibility_check = script.index(
            "Assert-CompatibleDataDirectory $existingConfig $dataRoot"
        )
        mutation_start = script.index("$installationMutationStarted = $true")
        runtime_replacement = script.index(
            "Move-Item -LiteralPath $runtimeExtracted -Destination $runtimeRoot"
        )
        self.assertLess(compatibility_check, mutation_start)
        self.assertLess(compatibility_check, runtime_replacement)
        self.assertIn(
            "Contact support to migrate its inventory state before installing.",
            script,
        )

    def test_verified_pending_device_is_reused_and_reactivated_without_reenrollment(self):
        script = setup_script_bytes().decode("utf-8")
        pending_check = script.index('$existingCredentialStatus -eq "pending"')
        pending_reuse = script.index("if ($reusePendingDevice)", pending_check)
        enrollment_guard = script.index(
            "if (-not $reuseActiveDevice -and -not $reusePendingDevice)",
            pending_reuse,
        )
        activation = script.index('-Uri "$baseUrl/connector/v1/activate"', enrollment_guard)
        self.assertLess(pending_check, pending_reuse)
        self.assertLess(pending_reuse, enrollment_guard)
        self.assertLess(enrollment_guard, activation)
        self.assertIn(
            "The pending connector pairing was rejected or expired.",
            script,
        )

    def test_unverifiable_bound_install_can_repair_with_safe_fresh_pairing(self):
        script = setup_script_bytes().decode("utf-8")
        recovery_warning = script.index(
            "Setup will use a fresh shop-bound pairing and safe write-back mode."
        )
        mutation_start = script.index("$installationMutationStarted = $true")
        enrollment_guard = script.index(
            "if (-not $reuseActiveDevice -and -not $reusePendingDevice)"
        )
        self.assertLess(recovery_warning, mutation_start)
        self.assertLess(mutation_start, enrollment_guard)
        self.assertIn('"POS_WRITEBACK_MODE" = "dry-run"', script)


class ConnectorStateIdentityTests(unittest.TestCase):
    def test_new_state_is_bound_to_shop_and_installation(self):
        state = {"version": 1, "quantities": {}}
        changed = bind_state_identity(
            state,
            shop_domain=SHOP,
            installation_id="win_11111111111111111111111111111111",
        )
        self.assertTrue(changed)
        self.assertEqual(state["shop_domain"], SHOP)
        self.assertEqual(state["installation_id"], "win_11111111111111111111111111111111")

    def test_state_from_another_shop_or_computer_is_rejected(self):
        with self.assertRaises(RuntimeError):
            bind_state_identity(
                {"shop_domain": OTHER_SHOP},
                shop_domain=SHOP,
                installation_id="win_11111111111111111111111111111111",
            )
        with self.assertRaises(RuntimeError):
            bind_state_identity(
                {"shop_domain": SHOP, "installation_id": "win_old"},
                shop_domain=SHOP,
                installation_id="win_new",
            )

    def test_unbound_inventory_history_requires_explicit_verified_adoption(self):
        state = {"version": 1, "quantities": {"SKU-1": {"pos": 2}}}
        with self.assertRaisesRegex(RuntimeError, "has no shop identity"):
            bind_state_identity(
                state,
                shop_domain=SHOP,
                installation_id="win_11111111111111111111111111111111",
            )
        self.assertNotIn("shop_domain", state)

        changed = bind_state_identity(
            state,
            shop_domain=SHOP,
            installation_id="win_11111111111111111111111111111111",
            allow_unbound_history=True,
        )
        self.assertTrue(changed)
        self.assertEqual(state["shop_domain"], SHOP)


if __name__ == "__main__":
    unittest.main()
