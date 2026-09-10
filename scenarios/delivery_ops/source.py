"""Source schema lifecycle and deterministic dirty-data generator."""

from __future__ import annotations

import json
import random
from collections.abc import Iterable
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import psycopg
from psycopg.types.json import Jsonb

from scenarios.delivery_ops.config import Settings

SQL_DIR = Path(__file__).with_name("sql")
START = datetime(2026, 1, 1, tzinfo=timezone.utc)

TRUNCATE_ORDER = (
    "refunds",
    "charges",
    "route_stops",
    "routes",
    "delivery_attempts",
    "shipment_events",
    "shipment_items",
    "shipments",
    "courier_assignments",
    "couriers",
    "addresses",
    "customers",
    "merchant_contracts",
    "merchants",
    "service_levels",
    "hubs",
)


def apply_schema(settings: Settings, reset: bool = False) -> None:
    with psycopg.connect(**settings.pg_kwargs(), autocommit=True) as connection:
        if reset:
            connection.execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public")
        connection.execute((SQL_DIR / "schema.sql").read_text())
    print(f"DeliveryOps source schema ready (reset={reset})")


def release_replication_slot(settings: Settings, slot_name: str) -> None:
    """Drop an inactive lab slot so a stopped consumer cannot retain WAL."""
    with psycopg.connect(**settings.pg_kwargs(), autocommit=True) as connection:
        active = connection.execute(
            "SELECT active FROM pg_replication_slots WHERE slot_name = %s",
            (slot_name,),
        ).fetchone()
        if active is None:
            print(f"Replication slot already absent: {slot_name}")
            return
        if active[0]:
            raise RuntimeError(f"Replication slot is still active: {slot_name}")
        connection.execute("SELECT pg_drop_replication_slot(%s)", (slot_name,))
    print(f"Replication slot released: {slot_name}")


def copy_rows(
    connection: psycopg.Connection,
    table: str,
    columns: tuple[str, ...],
    rows: Iterable[tuple[object, ...]],
) -> int:
    count = 0
    statement = f"COPY {table} ({', '.join(columns)}) FROM STDIN"
    with connection.cursor().copy(statement) as copy:
        for row in rows:
            copy.write_row(row)
            count += 1
    return count


def dirty_phone(index: int) -> str:
    number = f"09{index % 100_000_000:08d}"
    variants = (
        number,
        f"+84 {number[1:4]} {number[4:7]} {number[7:]}",
        f"84{number[1:]}",
        f" {number[:4]} {number[4:7]} {number[7:]} ",
    )
    return variants[index % len(variants)]


def dirty_amount(value: int, index: int) -> str:
    variants = (str(value), f"{value:,}", f"{value} VND", f" {value}.00 ")
    return variants[index % len(variants)]


def seed(settings: Settings) -> dict[str, int]:
    rng = random.Random(settings.random_seed)
    shipment_count = settings.shipment_count
    merchant_count = max(20, min(500, shipment_count // 100))
    customer_count = max(100, min(50_000, shipment_count // 3))
    courier_count = max(30, min(300, shipment_count // 50))
    hub_count = 8
    provinces = (
        "Hồ Chí Minh",
        "TP HCM",
        "Ho Chi Minh ",
        "Hà Nội",
        "Ha Noi",
        "Đà Nẵng",
        "Da Nang",
        "Bình Dương",
    )
    service_levels = (
        ("STANDARD", "Standard", 72, Decimal("22000"), True),
        ("EXPRESS", "Express", 24, Decimal("42000"), True),
        ("SAME_DAY", "Same day", 10, Decimal("65000"), True),
    )

    counts: dict[str, int] = {}
    with psycopg.connect(**settings.pg_kwargs()) as connection:
        connection.execute(
            "TRUNCATE " + ", ".join(TRUNCATE_ORDER) + " RESTART IDENTITY CASCADE"
        )

        hubs = [
            (
                f"H{i:02d}",
                f"VN-HUB-{i:02d}",
                f"Delivery hub {i:02d}",
                provinces[i - 1],
                "Asia/Ho_Chi_Minh",
                5000 + i * 750,
                START - timedelta(days=900 - i * 30),
                None,
            )
            for i in range(1, hub_count + 1)
        ]
        counts["hubs"] = copy_rows(
            connection,
            "hubs",
            (
                "hub_id",
                "hub_code",
                "hub_name",
                "province_raw",
                "timezone_name",
                "capacity_shipments_per_day",
                "opened_at",
                "closed_at",
            ),
            hubs,
        )
        counts["service_levels"] = copy_rows(
            connection,
            "service_levels",
            (
                "service_level_code",
                "service_name",
                "promised_hours",
                "base_fee_vnd",
                "active",
            ),
            service_levels,
        )

        merchants = []
        contracts = []
        for i in range(1, merchant_count + 1):
            merchant_id = f"M{i:05d}"
            created = START - timedelta(days=600 - i % 300)
            merchants.append(
                (
                    merchant_id,
                    f"Merchant {i:05d}",
                    None if i % 23 == 0 else f"TAX{i:08d}",
                    f"ops+{i}@merchant.example",
                    dirty_phone(i),
                    f"H{1 + i % hub_count:02d}",
                    created,
                    START + timedelta(days=i % 150),
                    START + timedelta(days=170) if i % 97 == 0 else None,
                )
            )
            split = START + timedelta(days=90)
            if i % 3 == 0:
                contracts.append(
                    (
                        f"MC{i:05d}-1",
                        merchant_id,
                        created,
                        split,
                        "standard",
                        Decimal("0.0200"),
                        Decimal("50000000"),
                    )
                )
            contracts.append(
                (
                    f"MC{i:05d}-2",
                    merchant_id,
                    split if i % 3 == 0 else created,
                    None,
                    ("standard", "growth", "enterprise")[i % 3],
                    Decimal(str((i % 5) / 100)),
                    Decimal(str(50_000_000 + (i % 5) * 25_000_000)),
                )
            )
        counts["merchants"] = copy_rows(
            connection,
            "merchants",
            (
                "merchant_id",
                "merchant_name",
                "tax_code",
                "contact_email",
                "phone_raw",
                "home_hub_id",
                "created_at",
                "updated_at",
                "deleted_at",
            ),
            merchants,
        )
        counts["merchant_contracts"] = copy_rows(
            connection,
            "merchant_contracts",
            (
                "contract_id",
                "merchant_id",
                "valid_from",
                "valid_to",
                "merchant_tier",
                "discount_rate",
                "credit_limit_vnd",
            ),
            contracts,
        )

        customers = []
        addresses = []
        for i in range(1, customer_count + 1):
            customer_id = f"C{i:07d}"
            created = START - timedelta(days=i % 400)
            customers.append(
                (
                    customer_id,
                    f"Customer {i:07d}",
                    dirty_phone(i + 10_000),
                    None if i % 71 == 0 else f"Customer.{i}@Example.COM ",
                    created,
                    START + timedelta(days=i % 180),
                    START + timedelta(days=175) if i % 997 == 0 else None,
                )
            )
            addresses.append(
                (
                    f"A{i:07d}",
                    customer_id,
                    f"  {1 + i % 999}  Đường số {1 + i % 50} ",
                    f"P.{1 + i % 20}" if i % 5 else None,
                    f"Quận {1 + i % 12}",
                    provinces[i % len(provinces)],
                    "" if i % 13 else None,
                    10.70 + (i % 100) / 1000,
                    106.60 + (i % 100) / 1000,
                    created,
                )
            )
        counts["customers"] = copy_rows(
            connection,
            "customers",
            (
                "customer_id",
                "full_name",
                "phone_raw",
                "email_raw",
                "created_at",
                "updated_at",
                "deleted_at",
            ),
            customers,
        )
        counts["addresses"] = copy_rows(
            connection,
            "addresses",
            (
                "address_id",
                "customer_id",
                "line1_raw",
                "ward_raw",
                "district_raw",
                "province_raw",
                "postal_code_raw",
                "latitude",
                "longitude",
                "created_at",
            ),
            addresses,
        )

        couriers = []
        assignments = []
        for i in range(1, courier_count + 1):
            courier_id = f"D{i:05d}"
            hired = START - timedelta(days=400 - i % 200)
            couriers.append(
                (
                    courier_id,
                    f"Courier {i:05d}",
                    dirty_phone(i + 20_000),
                    hired,
                    "inactive" if i % 89 == 0 else "active",
                    START + timedelta(days=i % 180),
                    None,
                )
            )
            if i % 5 == 0:
                assignments.append(
                    (
                        f"CA{i:05d}-1",
                        courier_id,
                        f"H{1 + i % hub_count:02d}",
                        "motorbike",
                        hired,
                        START + timedelta(days=100),
                    )
                )
            assignments.append(
                (
                    f"CA{i:05d}-2",
                    courier_id,
                    f"H{1 + (i + 1) % hub_count:02d}",
                    ("motorbike", "van", "electric_bike")[i % 3],
                    START + timedelta(days=100) if i % 5 == 0 else hired,
                    None,
                )
            )
        counts["couriers"] = copy_rows(
            connection,
            "couriers",
            (
                "courier_id",
                "courier_name",
                "phone_raw",
                "hired_at",
                "employment_status",
                "updated_at",
                "deleted_at",
            ),
            couriers,
        )
        counts["courier_assignments"] = copy_rows(
            connection,
            "courier_assignments",
            (
                "assignment_id",
                "courier_id",
                "hub_id",
                "vehicle_type",
                "valid_from",
                "valid_to",
            ),
            assignments,
        )

        shipments = []
        items = []
        events = []
        attempts = []
        charges = []
        refunds = []
        route_groups: dict[tuple[object, ...], list[tuple[str, datetime]]] = {}
        status_weights = (
            ("delivered", 72),
            ("delivery_failed", 8),
            ("returned", 5),
            ("cancelled", 5),
            ("out_for_delivery", 4),
            ("in_transit", 6),
        )
        for i in range(1, shipment_count + 1):
            shipment_id = f"S{i:09d}"
            merchant_id = f"M{1 + i % merchant_count:05d}"
            customer_id = f"C{1 + (i * 7) % customer_count:07d}"
            origin_hub = f"H{1 + i % hub_count:02d}"
            destination_hub = f"H{1 + (i * 3) % hub_count:02d}"
            courier_id = f"D{1 + i % courier_count:05d}"
            service_code, promised_hours = rng.choice(
                (("STANDARD", 72), ("EXPRESS", 24), ("SAME_DAY", 10))
            )
            created = START + timedelta(seconds=rng.randrange(180 * 86400))
            status = rng.choices(
                [item[0] for item in status_weights],
                weights=[item[1] for item in status_weights],
            )[0]
            base_value = 50_000 + (i * 7919) % 3_000_000
            currency = "VND"
            display_value = base_value
            if i % 97 == 0:
                currency, display_value = "USD", max(2, base_value // 25_000)
            elif i % 131 == 0:
                currency, display_value = "THB", max(50, base_value // 700)
            grams = 200 + (i * 37) % 20_000
            weight_unit = "kg" if i % 11 == 0 else "g"
            weight_value = (
                Decimal(str(round(grams / 1000, 3)))
                if weight_unit == "kg"
                else Decimal(grams)
            )
            promised = created + timedelta(hours=promised_hours)
            if i % 113 == 0:
                promised += timedelta(hours=12)

            sequence = ["created", "picked_up", "hub_received", "in_transit"]
            if status in {
                "out_for_delivery",
                "delivery_failed",
                "delivered",
                "returned",
            }:
                sequence.append("out_for_delivery")
            if status in {"delivery_failed", "delivered", "returned"}:
                sequence.append(
                    "delivered" if status == "delivered" else "delivery_failed"
                )
            if status == "returned":
                sequence.extend(("return_requested", "returned"))
            elif status == "cancelled":
                sequence = ["created", "cancelled"]
            elif status == "in_transit":
                sequence = sequence[:4]

            event_time = created
            last_recorded = created
            for version, event_type in enumerate(sequence, start=1):
                event_time += timedelta(minutes=20 + rng.randrange(300))
                recorded = event_time + timedelta(minutes=rng.randrange(20))
                if i % 101 == 0 and version == len(sequence):
                    recorded += timedelta(days=1 + i % 3)
                if i % 337 == 0 and version == 2:
                    recorded = event_time - timedelta(minutes=30)
                partner_event_id = f"PE-{i:09d}-{version:02d}"
                reason = None
                if event_type == "delivery_failed":
                    reason = (
                        "NO_ANSWER",
                        "khong nghe may",
                        "ADDRESS_ERR",
                        "addr-not-found",
                    )[i % 4]
                event = (
                    partner_event_id,
                    shipment_id,
                    event_type,
                    event_time,
                    recorded,
                    version,
                    destination_hub if version >= 3 else origin_hub,
                    courier_id if version >= 2 else None,
                    reason,
                    Jsonb(
                        {"partner": f"partner-{i % 4}", "scanner": f"device-{i % 80}"}
                    ),
                )
                events.append(event)
                last_recorded = max(last_recorded, recorded)
                if i % 199 == 0 and version == len(sequence):
                    events.append(event)

            final_status = sequence[-1]
            cancelled_at = event_time if final_status == "cancelled" else None
            shipments.append(
                (
                    shipment_id,
                    f"VN{i:011d}",
                    merchant_id,
                    customer_id,
                    f"A{1 + (i * 7) % customer_count:07d}",
                    origin_hub,
                    destination_hub,
                    service_code,
                    final_status,
                    weight_value,
                    weight_unit,
                    dirty_amount(display_value, i),
                    currency,
                    created,
                    promised,
                    last_recorded,
                    cancelled_at,
                    None,
                )
            )
            for item_number in range(1, 2 + i % 3):
                items.append(
                    (
                        f"SI{i:09d}-{item_number}",
                        shipment_id,
                        f"SKU-{(i * 13 + item_number) % 5000:05d}",
                        f"Item {item_number} for shipment {i}",
                        1 + (i + item_number) % 3,
                        dirty_amount(display_value // (1 + i % 3), i + item_number),
                        currency,
                        (i + item_number) % 17 == 0,
                    )
                )
            if final_status in {"delivered", "delivery_failed", "returned"}:
                failed = final_status != "delivered"
                attempts.append(
                    (
                        f"AT{i:09d}-1",
                        shipment_id,
                        1,
                        courier_id if i % 73 else None,
                        event_time,
                        "failed" if failed else "delivered",
                        ("NO_ANSWER", "khong nghe may", "ADDRESS_ERR")[i % 3]
                        if failed
                        else None,
                        f" Recipient {i} " if not failed else None,
                        f"s3://proof/{shipment_id}.jpg"
                        if not failed and i % 19
                        else None,
                        last_recorded,
                    )
                )
            charge_amount = 22_000 + (i % 5) * 5_000
            charges.append(
                (
                    f"CH{i:09d}",
                    shipment_id,
                    "delivery_fee",
                    dirty_amount(charge_amount, i),
                    "VND",
                    "voided" if final_status == "cancelled" else "captured",
                    created,
                    last_recorded,
                )
            )
            if final_status in {"returned", "cancelled"}:
                refunds.append(
                    (
                        f"RF{i:09d}",
                        f"CH{i:09d}",
                        shipment_id,
                        dirty_amount(charge_amount, i + 1),
                        "VND",
                        "RETURN" if final_status == "returned" else "customer_cancel",
                        last_recorded + timedelta(hours=12),
                    )
                )
            route_key = (created.date(), destination_hub, courier_id)
            route_groups.setdefault(route_key, []).append((shipment_id, event_time))

        # Eventual-integrity failures: partner events can arrive before a shipment.
        orphan_count = max(1, shipment_count // 500)
        for i in range(orphan_count):
            event_at = START + timedelta(days=i % 180)
            events.append(
                (
                    f"PE-ORPHAN-{i:06d}",
                    f"S-MISSING-{i:06d}",
                    "hub_received",
                    event_at,
                    event_at + timedelta(minutes=5),
                    1,
                    f"H{1 + i % hub_count:02d}",
                    None,
                    None,
                    Jsonb({"partner": "legacy-import"}),
                )
            )

        counts["shipments"] = copy_rows(
            connection,
            "shipments",
            (
                "shipment_id",
                "tracking_number",
                "merchant_id",
                "customer_id",
                "address_id",
                "origin_hub_id",
                "destination_hub_id",
                "service_level_code",
                "current_status",
                "weight_value",
                "weight_unit",
                "declared_value_text",
                "declared_currency",
                "created_at",
                "promised_at",
                "updated_at",
                "cancelled_at",
                "deleted_at",
            ),
            shipments,
        )
        counts["shipment_items"] = copy_rows(
            connection,
            "shipment_items",
            (
                "shipment_item_id",
                "shipment_id",
                "sku",
                "description",
                "quantity",
                "item_value_text",
                "currency",
                "fragile",
            ),
            items,
        )
        counts["shipment_events"] = copy_rows(
            connection,
            "shipment_events",
            (
                "partner_event_id",
                "shipment_id",
                "event_type",
                "event_at",
                "recorded_at",
                "source_version",
                "hub_id",
                "courier_id",
                "reason_code_raw",
                "payload",
            ),
            events,
        )
        counts["delivery_attempts"] = copy_rows(
            connection,
            "delivery_attempts",
            (
                "attempt_id",
                "shipment_id",
                "attempt_number",
                "courier_id",
                "attempted_at",
                "outcome",
                "reason_code_raw",
                "recipient_name_raw",
                "proof_url",
                "recorded_at",
            ),
            attempts,
        )
        counts["charges"] = copy_rows(
            connection,
            "charges",
            (
                "charge_id",
                "shipment_id",
                "charge_type",
                "amount_text",
                "currency",
                "charge_status",
                "charged_at",
                "updated_at",
            ),
            charges,
        )
        counts["refunds"] = copy_rows(
            connection,
            "refunds",
            (
                "refund_id",
                "charge_id",
                "shipment_id",
                "amount_text",
                "currency",
                "reason_code_raw",
                "refunded_at",
            ),
            refunds,
        )

        routes = []
        route_stops = []
        for route_number, ((route_date, hub_id, courier_id), members) in enumerate(
            sorted(route_groups.items()), start=1
        ):
            route_id = f"R{route_number:08d}"
            planned_start = datetime.combine(
                route_date, datetime.min.time(), tzinfo=timezone.utc
            ) + timedelta(hours=8)
            routes.append(
                (
                    route_id,
                    route_date,
                    hub_id,
                    courier_id,
                    planned_start,
                    planned_start + timedelta(minutes=route_number % 30),
                    planned_start + timedelta(hours=6, minutes=route_number % 90),
                    "completed",
                )
            )
            for sequence_number, (shipment_id, arrival) in enumerate(members, start=1):
                route_stops.append(
                    (
                        f"RS{route_number:08d}-{sequence_number:04d}",
                        route_id,
                        shipment_id,
                        sequence_number,
                        sequence_number
                        if sequence_number % 17
                        else sequence_number + 1,
                        planned_start + timedelta(minutes=sequence_number * 8),
                        arrival,
                        "visited",
                    )
                )
        counts["routes"] = copy_rows(
            connection,
            "routes",
            (
                "route_id",
                "route_date",
                "hub_id",
                "courier_id",
                "planned_start_at",
                "actual_start_at",
                "actual_end_at",
                "route_status",
            ),
            routes,
        )
        counts["route_stops"] = copy_rows(
            connection,
            "route_stops",
            (
                "route_stop_id",
                "route_id",
                "shipment_id",
                "planned_sequence",
                "actual_sequence",
                "planned_arrival_at",
                "actual_arrival_at",
                "stop_outcome",
            ),
            route_stops,
        )
        connection.commit()

    print(json.dumps(counts, indent=2, sort_keys=True))
    return counts


def emit_live_changes(settings: Settings, event_count: int = 20) -> dict[str, int]:
    """Generate isolated CDC activity without changing the scenario contract."""
    now = datetime.now(timezone.utc)
    inserted = 0
    updated = 0
    deleted = 0
    with psycopg.connect(**settings.pg_kwargs()) as connection:
        shipment_ids = [
            row[0]
            for row in connection.execute(
                "SELECT shipment_id FROM shipments ORDER BY shipment_id LIMIT %s",
                (event_count,),
            ).fetchall()
        ]
        for index, shipment_id in enumerate(shipment_ids, start=1):
            version = connection.execute(
                "SELECT coalesce(max(source_version), 0) + 1 FROM shipment_events WHERE shipment_id = %s",
                (shipment_id,),
            ).fetchone()[0]
            partner_event_id = f"PE-LIVE-{now:%Y%m%d%H%M%S}-{index:04d}"
            connection.execute(
                """
                INSERT INTO shipment_events (
                    partner_event_id, shipment_id, event_type, event_at, recorded_at,
                    source_version, hub_id, courier_id, payload
                )
                SELECT %s, s.shipment_id, 'out_for_delivery', %s, %s, %s,
                       s.destination_hub_id, NULL, %s
                FROM shipments s WHERE s.shipment_id = %s
                """,
                (
                    partner_event_id,
                    now - timedelta(minutes=index),
                    now,
                    version,
                    Jsonb({"generator": "delivery-live", "sequence": index}),
                    shipment_id,
                ),
            )
            inserted += 1
            connection.execute(
                "UPDATE shipments SET current_status = 'out_for_delivery', updated_at = %s WHERE shipment_id = %s",
                (now, shipment_id),
            )
            updated += 1
            if index == len(shipment_ids):
                connection.execute(
                    "DELETE FROM shipment_events WHERE partner_event_id = %s",
                    (partner_event_id,),
                )
                deleted += 1
        connection.commit()
    result = {
        "inserted_events": inserted,
        "updated_shipments": updated,
        "deleted_events": deleted,
    }
    print(json.dumps(result, indent=2, sort_keys=True))
    return result
