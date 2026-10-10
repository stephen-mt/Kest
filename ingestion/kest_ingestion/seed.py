from __future__ import annotations

from datetime import UTC, datetime, timedelta

from kest_ingestion.config import Settings
from kest_ingestion.db import business_connection


def seed_business_data(settings: Settings) -> None:
    base = datetime(2026, 1, 1, 9, tzinfo=UTC)
    customers = [
        ("cus_001", "an@example.test", "VN", base - timedelta(days=30), base, False),
        ("cus_002", "binh@example.test", "VN", base - timedelta(days=20), base, False),
        ("cus_003", "chris@example.test", None, base - timedelta(days=10), base, False),
    ]
    orders = [
        ("ord_001", "cus_001", "completed", "USD", base, base + timedelta(hours=1), False),
        (
            "ord_002",
            "cus_002",
            "completed",
            "USD",
            base + timedelta(days=1),
            base + timedelta(days=1, hours=2),
            False,
        ),
        (
            "ord_003",
            "cus_001",
            "cancelled",
            "USD",
            base + timedelta(days=2),
            base + timedelta(days=2, hours=1),
            False,
        ),
    ]
    items = [
        ("item_001", "ord_001", "prod_001", 2, 50, base + timedelta(hours=1), False),
        ("item_002", "ord_002", "prod_002", 1, 80, base + timedelta(days=1, hours=2), False),
        ("item_003", "ord_003", "prod_003", 1, 40, base + timedelta(days=2, hours=1), False),
    ]
    payments = [
        (
            "pay_001",
            "ord_001",
            "txn_001",
            "succeeded",
            100,
            base + timedelta(hours=2),
            base + timedelta(hours=2),
            False,
        ),
        (
            "pay_002",
            "ord_002",
            "txn_002",
            "succeeded",
            80,
            base + timedelta(days=1, hours=8),
            base + timedelta(days=1, hours=8),
            False,
        ),
        ("pay_003", "ord_003", "txn_003", "failed", 40, None, base + timedelta(days=2, hours=2), False),
    ]
    refunds = [
        ("ref_001", "pay_001", "succeeded", 20, base + timedelta(days=2), base + timedelta(days=2), False),
    ]
    campaigns = [
        (
            "cmp_001",
            "New Year Search",
            "search",
            base - timedelta(days=5),
            base + timedelta(days=10),
            base,
            False,
        ),
        ("cmp_002", "Retargeting", "social", base - timedelta(days=3), None, base, False),
    ]
    spend = [
        ("spend_001", "cmp_001", base.date(), 25, base + timedelta(hours=3), False),
        (
            "spend_002",
            "cmp_002",
            (base + timedelta(days=1)).date(),
            15,
            base + timedelta(days=1, hours=3),
            False,
        ),
    ]
    events = []
    for idx, (session, campaign, customer, day, completed) in enumerate(
        [
            ("sess_001", "cmp_001", "cus_001", 0, True),
            ("sess_002", "cmp_001", "cus_002", 0, False),
            ("sess_003", "cmp_002", "cus_002", 1, True),
        ]
    ):
        occurred = base + timedelta(days=day, minutes=idx)
        events.append(
            (
                f"evt_{idx}_view",
                "product_viewed",
                session,
                customer,
                "prod_001",
                campaign,
                occurred,
                occurred,
                False,
            )
        )
        events.append(
            (
                f"evt_{idx}_cart",
                "add_to_cart",
                session,
                customer,
                "prod_001",
                campaign,
                occurred + timedelta(minutes=2),
                occurred + timedelta(minutes=2),
                False,
            )
        )
        if completed:
            events.append(
                (
                    f"evt_{idx}_checkout",
                    "checkout_completed",
                    session,
                    customer,
                    "prod_001",
                    campaign,
                    occurred + timedelta(minutes=5),
                    occurred + timedelta(minutes=5),
                    False,
                )
            )

    with business_connection(settings) as conn, conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO customers VALUES (%s,%s,%s,%s,%s,%s)
            ON CONFLICT (customer_id) DO UPDATE SET email=excluded.email, country=excluded.country,
              updated_at=excluded.updated_at, is_deleted=excluded.is_deleted
        """,
            customers,
        )
        cur.executemany(
            """
            INSERT INTO orders VALUES (%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (order_id) DO UPDATE SET status=excluded.status, updated_at=excluded.updated_at,
              is_deleted=excluded.is_deleted
        """,
            orders,
        )
        cur.executemany(
            """
            INSERT INTO order_items VALUES (%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (order_item_id) DO UPDATE SET quantity=excluded.quantity,
              unit_price=excluded.unit_price, updated_at=excluded.updated_at, is_deleted=excluded.is_deleted
        """,
            items,
        )
        cur.executemany(
            """
            INSERT INTO payments VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (payment_id) DO UPDATE SET status=excluded.status, amount=excluded.amount,
              succeeded_at=excluded.succeeded_at, updated_at=excluded.updated_at, is_deleted=excluded.is_deleted
        """,
            payments,
        )
        cur.executemany(
            """
            INSERT INTO refunds VALUES (%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (refund_id) DO UPDATE SET status=excluded.status, amount=excluded.amount,
              succeeded_at=excluded.succeeded_at, updated_at=excluded.updated_at, is_deleted=excluded.is_deleted
        """,
            refunds,
        )
        cur.executemany(
            """
            INSERT INTO marketing_campaigns VALUES (%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (campaign_id) DO UPDATE SET name=excluded.name, channel=excluded.channel,
              updated_at=excluded.updated_at, is_deleted=excluded.is_deleted
        """,
            campaigns,
        )
        cur.executemany(
            """
            INSERT INTO campaign_spend VALUES (%s,%s,%s,%s,%s,%s)
            ON CONFLICT (spend_id) DO UPDATE SET amount=excluded.amount,
              updated_at=excluded.updated_at, is_deleted=excluded.is_deleted
        """,
            spend,
        )
        cur.executemany(
            """
            INSERT INTO events VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (event_id) DO UPDATE SET updated_at=excluded.updated_at,
              is_deleted=excluded.is_deleted
        """,
            events,
        )


def add_late_refund(settings: Settings) -> None:
    occurred = datetime(2026, 1, 4, 9, tzinfo=UTC)
    with business_connection(settings) as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO refunds VALUES ('ref_late_002', 'pay_002', 'succeeded', 10, %s, %s, false)
            ON CONFLICT (refund_id) DO UPDATE SET status=excluded.status, amount=excluded.amount,
              succeeded_at=excluded.succeeded_at, updated_at=excluded.updated_at
        """,
            (occurred, occurred),
        )
