import re
import uuid

MARKET_COUNT = 10
VENDOR_COUNT = 1_000
BUYER_COUNT = 10_000
PRODUCT_COUNT = 4_000
PRODUCTS_PER_VENDOR = 4
MARKET_PATTERN = re.compile(r"PLAT-\d{3}\Z")
VENDOR_PATTERN = re.compile(r"SELLER-\d{6}\Z")
BUYER_PATTERN = re.compile(r"BUYER-\d{7}\Z")
UUID_PATTERN = r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"
LIVE_ID_PATTERNS = {
    prefix: re.compile(rf"{prefix}-LIVE-{UUID_PATTERN}\Z")
    for prefix in ("EVT", "BSA", "PPE", "RMP")
}


def market_id(index):
    return f"PLAT-{index:03d}"


def vendor_id(index):
    return f"SELLER-{index:06d}"


def buyer_id(index):
    return f"BUYER-{index:07d}"


def live_id(prefix):
    return f"{prefix}-LIVE-{uuid.uuid4()}"


def history_id(prefix, index):
    return f"{prefix}-HIST-{index:014d}"


def product_key(index):
    return (
        f"CAT-{index % 20:03d}",
        f"SUB-{index % 100:05d}",
        index % PRODUCTS_PER_VENDOR,
        vendor_id(1 + (index - 1) // PRODUCTS_PER_VENDOR),
    )
