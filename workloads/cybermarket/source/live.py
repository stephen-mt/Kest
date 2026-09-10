import random
import uuid
from collections import deque
from datetime import datetime, timedelta, timezone

import psycopg
from psycopg.types.json import Jsonb

from workloads.cybermarket.source.ids import (
    BUYER_COUNT,
    MARKET_COUNT,
    VENDOR_COUNT,
    buyer_id,
    live_id,
    market_id,
    vendor_id,
)


def utc_now():
    return datetime.now(timezone.utc).replace(tzinfo=None)


class LiveEventWriter:
    def __init__(self, settings):
        if settings.event_rate != 20:
            raise ValueError("CyberMarket's fixed workload rate must be 20 events/sec")
        self.settings = settings
        self.random = random.Random(settings.random_seed)
        self.connection = psycopg.connect(**settings.pg_kwargs(), autocommit=True)
        self.recent = deque(maxlen=5000)
        with self.connection.cursor() as cursor:
            cursor.execute("""
                SELECT t."EventCode", p."PPE_id"
                FROM transactions t
                JOIN "PaymentProcessingEvents" p ON p.transaction_ref = t."EventCode"
                ORDER BY t."EventTimestamp" DESC LIMIT 1000
            """)
            self.recent.extend(cursor.fetchall())

    def close(self):
        self.connection.close()

    def skewed_index(self, size):
        return 1 + int((self.random.random() ** 3) * size)

    def buyer_session(self):
        started = utc_now()
        duration = self.random.randint(8, 1800)
        pages = self.random.randint(1, 35)
        products = min(pages, self.random.randint(0, 14))
        additions = min(products, self.random.randint(0, 4))
        checkout = additions > 0 and self.random.random() < 0.55
        completed = checkout and self.random.random() < 0.68
        removals = self.random.randint(0, additions)
        bounced = pages == 1 and additions == 0
        with self.connection.transaction(), self.connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO "BuyerSessionAnalytics" VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
            """,
                (
                    live_id("BSA"),
                    buyer_id(self.skewed_index(BUYER_COUNT)),
                    started,
                    duration,
                    pages,
                    products,
                    additions,
                    removals,
                    self.random.randint(0, 8),
                    checkout,
                    completed,
                    bounced,
                    self.random.choice(("direct", "search", "affiliate", "social")),
                    self.random.choice(("mobile", "desktop", "tablet")),
                    self.random.choice(("NA", "EU", "APAC", "LATAM")),
                    duration / pages,
                    products / pages,
                    self.random.uniform(10, 100),
                    self.random.randint(0, 2),
                    round(products * self.random.uniform(8, 120), 2),
                ),
            )

    def purchase(self):
        now = utc_now()
        event_id = live_id("EVT")
        payment_id = live_id("PPE")
        vendor = vendor_id(self.skewed_index(VENDOR_COUNT))
        buyer = buyer_id(self.skewed_index(BUYER_COUNT))
        platform = market_id(self.random.randint(1, MARKET_COUNT))
        origin = self.random.choice(("NA", "EU", "APAC", "LATAM"))
        destination = self.random.choice(("NA", "EU", "APAC", "LATAM"))
        amount = round(self.random.lognormvariate(4.2, 0.9), 2)
        fraud_probability = min(0.99, self.random.betavariate(1.2, 8))
        financials = {
            "amount": amount,
            "currency": "USD",
            "status": "authorized",
            "updated_at": now.isoformat(timespec="milliseconds") + "Z",
        }
        with self.connection.transaction(), self.connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT "ProdCat", "Subcategory", "ListingAge", "SellerPointer"
                FROM products WHERE "SellerPointer" = %s
                ORDER BY "ListingAge"
            """,
                (vendor,),
            )
            products = cursor.fetchall()
            if len(products) != 4:
                raise RuntimeError(f"Seed products missing for {vendor}")
            products = self.random.sample(products, 2)
            cursor.execute(
                """
                INSERT INTO transactions VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
            """,
                (
                    event_id,
                    "purchase",
                    now,
                    platform,
                    vendor,
                    buyer,
                    origin,
                    destination,
                    int(origin != destination),
                    "multi-hop" if origin != destination else "direct",
                    self.random.choice(("normal", "elevated", "burst")),
                    "cross-border" if origin != destination else "domestic",
                    str(round(self.random.uniform(0, 100), 3)),
                    Jsonb(financials),
                ),
            )
            for line, product in enumerate(products, start=1):
                cursor.execute(
                    """
                    INSERT INTO transaction_products VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                    (event_id, *product, amount * (0.45 if line == 1 else 0.55), 1),
                )
            cursor.execute(
                """
                INSERT INTO "PaymentProcessingEvents" VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
            """,
                (
                    payment_id,
                    event_id,
                    now + timedelta(milliseconds=10),
                    self.random.choice(("card", "wallet", "bank_transfer")),
                    "authorized",
                    amount,
                    0,
                    "USD",
                    self.random.choice(("nova-pay", "orbit-pay")),
                    uuid.uuid4().hex[:12].upper(),
                    round(amount * 0.021, 2),
                    2.1,
                    fraud_probability < 0.8,
                    fraud_probability,
                    "Y",
                    True,
                    self.random.random() < 0.75,
                    None,
                    0,
                    self.random.randint(25, 900),
                ),
            )
            cursor.execute(
                """
                INSERT INTO risk_analytics VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
                (
                    event_id,
                    self.random.randint(0, 5),
                    fraud_probability,
                    "high"
                    if fraud_probability >= 0.7
                    else "medium"
                    if fraud_probability >= 0.3
                    else "low",
                    self.random.randint(0, 8),
                    self.random.randint(1, 5),
                    Jsonb(
                        {
                            "wallet_age_days": self.random.randint(1, 2500),
                            "score": fraud_probability,
                        }
                    ),
                ),
            )
            cursor.execute(
                """
                UPDATE buyers SET
                    "PurchaseCount" = "PurchaseCount" + 1,
                    buyer_risk_profile = buyer_risk_profile || %s
                WHERE "AcqCode" = %s
            """,
                (Jsonb({"last_purchase_at": now.isoformat() + "Z"}), buyer),
            )
            cursor.execute(
                """
                UPDATE vendors SET
                    "TotalTxns" = (COALESCE(NULLIF("TotalTxns", ''), '0')::bigint + 1)::text,
                    "LastActiveDt" = %s
                WHERE "SellerKey" = %s
            """,
                (now, vendor),
            )
        self.recent.append((event_id, payment_id))

    def choose_recent(self):
        if not self.recent:
            self.purchase()
        return self.random.choice(tuple(self.recent))

    def payment_update(self):
        now = utc_now()
        with self.connection.cursor() as cursor:
            cursor.execute("""
                SELECT p."PPE_id", p.transaction_ref, t."VendorLink", p.fraud_score,
                       p.amount_requested
                FROM "PaymentProcessingEvents" p
                JOIN transactions t ON t."EventCode" = p.transaction_ref
                WHERE p.processing_stage = 'authorized'
                ORDER BY p.event_timestamp
                LIMIT 100
            """)
            candidates = cursor.fetchall()
        if not candidates:
            self.purchase()
            return self.payment_update()
        payment_id, event_id, vendor, fraud_score, amount = self.random.choice(
            candidates
        )
        with self.connection.transaction(), self.connection.cursor() as cursor:
            failed = fraud_score >= 0.8 or self.random.random() < 0.05
            stage = "failed" if failed else "settled"
            cursor.execute(
                """
                UPDATE "PaymentProcessingEvents" SET
                    event_timestamp = %s,
                    processing_stage = %s,
                    amount_processed = %s,
                    decline_reason = %s,
                    processing_time_ms = processing_time_ms + %s,
                    retry_count = retry_count + %s
                WHERE "PPE_id" = %s
            """,
                (
                    now,
                    stage,
                    0 if failed else amount,
                    "risk_decline" if failed else None,
                    self.random.randint(5, 120),
                    int(failed),
                    payment_id,
                ),
            )
            status = "failed" if failed else "completed"
            cursor.execute(
                """
                UPDATE transactions
                SET transaction_financials = transaction_financials || %s
                WHERE "EventCode" = %s
                """,
                (
                    Jsonb({"status": status, "updated_at": now.isoformat() + "Z"}),
                    event_id,
                ),
            )
            if not failed:
                cursor.execute(
                    """
                    UPDATE vendors
                    SET "CompletedTxns" = "CompletedTxns" + 1
                    WHERE "SellerKey" = %s
                    """,
                    (vendor,),
                )

    def risk_prediction(self):
        event_id, _ = self.choose_recent()
        probability = min(0.99, self.random.betavariate(1.4, 7))
        category = (
            "high" if probability >= 0.7 else "medium" if probability >= 0.3 else "low"
        )
        with self.connection.transaction(), self.connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO "RiskModelPredictions" VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
            """,
                (
                    live_id("RMP"),
                    event_id,
                    utc_now(),
                    "cyber-risk-lite",
                    "1.0.0",
                    probability,
                    category,
                    self.random.uniform(0.65, 0.99),
                    self.random.choice(("velocity", "amount", "device", "behavior")),
                    self.random.randint(1, 8),
                    self.random.random(),
                    self.random.random(),
                    self.random.random(),
                    self.random.random(),
                    "manual_review" if category == "high" else "approve",
                    None,
                    self.random.randint(2, 95),
                    self.random.uniform(0.6, 1),
                    category == "high",
                    self.random.uniform(0, 0.2),
                ),
            )

    def transaction_status_update(self):
        event_id, _ = self.choose_recent()
        update = {"status": "fulfilled", "updated_at": utc_now().isoformat() + "Z"}
        with self.connection.transaction(), self.connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE transactions SET transaction_financials = transaction_financials || %s
                WHERE "EventCode" = %s
                  AND transaction_financials->>'status' = 'completed'
            """,
                (Jsonb(update), event_id),
            )

    def emit(self, event):
        getattr(self, event)()
