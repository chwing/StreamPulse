import uuid
import random
import time
from datetime import datetime, timezone
from faker import Faker
from config import config

fake = Faker()
Faker.seed(0)   # reproducible names across runs


# ── Pre-generate the universe of users and products ──────────────────────────
# Done once at import time so every event references consistent entities.

USERS = [
    {
        "user_id":  str(uuid.uuid4()),
        "country":  fake.country_code(representation="alpha-2"),
        "device":   random.choice(["desktop", "mobile", "tablet"]),
    }
    for _ in range(config.NUM_USERS)
]

PRODUCTS = [
    {
        "product_id":   str(uuid.uuid4()),
        "product_name": fake.catch_phrase(),
        "category":     random.choice([
            "Electronics", "Clothing", "Home", "Sports",
            "Books", "Beauty", "Toys", "Automotive"
        ]),
        "price": round(random.uniform(4.99, 499.99), 2),
    }
    for _ in range(config.NUM_PRODUCTS)
]


def _now_ms() -> int:
    """Current UTC time as milliseconds since epoch."""
    return int(datetime.now(timezone.utc).timestamp() * 1000)


# ── Active sessions tracker ───────────────────────────────────────────────────
# Maps session_id → session metadata for sessions that are still open.
# A session closes when the user purchases or after random inactivity.

_active_sessions: dict[str, dict] = {}


def _get_or_create_session(user: dict) -> dict:
    """
    Return the user's existing open session, or create a new one.
    Each user has at most one active session at a time.
    """
    # Look for an existing session for this user
    for sid, session in _active_sessions.items():
        if session["user_id"] == user["user_id"]:
            return session

    # No existing session — start a new one
    session = {
        "session_id":      str(uuid.uuid4()),
        "user_id":         user["user_id"],
        "start_timestamp": _now_ms(),
        "pages_visited":   0,
        "device_type":     user["device"],
        "country":         user["country"],
        "converted":       False,
        "cart":            [],   # products added but not yet purchased
    }
    _active_sessions[session["session_id"]] = session
    return session


def _close_session(session: dict) -> dict:
    """
    Remove a session from the active tracker and return the
    SessionEvent record ready for Kafka serialization.
    """
    _active_sessions.pop(session["session_id"], None)
    return {
        "session_id":      session["session_id"],
        "user_id":         session["user_id"],
        "start_timestamp": session["start_timestamp"],
        "end_timestamp":   _now_ms(),
        "pages_visited":   session["pages_visited"],
        "device_type":     session["device_type"],
        "country":         session["country"],
        "converted":       session["converted"],
    }


# ── Event generation ──────────────────────────────────────────────────────────

def generate_clickstream_event(user: dict, session: dict, product: dict | None) -> dict:
    event_type = random.choice([
        "page_view", "product_click", "add_to_cart",
        "remove_from_cart", "search"
    ])
    return {
        "event_id":        str(uuid.uuid4()),
        "user_id":         user["user_id"],
        "session_id":      session["session_id"],
        "event_type":      event_type,
        "product_id":      product["product_id"] if product else None,
        "page_url":        fake.uri_path(),
        "referrer":        fake.uri() if random.random() < 0.3 else None,
        "device_type":     user["device"],
        "event_timestamp": _now_ms(),
    }


def generate_order_event(user: dict, session: dict) -> dict | None:
    """
    Generate an order from whatever is in the session's cart.
    Returns None if the cart is empty.
    """
    if not session["cart"]:
        return None

    items = []
    for product in session["cart"]:
        qty = random.randint(1, 3)
        items.append({
            "product_id":   product["product_id"],
            "product_name": product["product_name"],
            "category":     product["category"],
            "quantity":     qty,
            "unit_price":   product["price"],
            "subtotal":     round(product["price"] * qty, 2),
        })

    total = round(sum(i["subtotal"] for i in items), 2)

    return {
        "order_id":        str(uuid.uuid4()),
        "user_id":         user["user_id"],
        "session_id":      session["session_id"],
        "items":           items,
        "total_amount":    total,
        "currency":        "USD",
        "payment_method":  random.choice(
            ["credit_card", "debit_card", "paypal", "crypto"]
        ),
        "order_timestamp": _now_ms(),
    }


# ── Main simulation step ──────────────────────────────────────────────────────

def next_events() -> list[tuple[str, str, dict]]:
    """
    Simulate one user action. Returns a list of
    (topic, partition_key, event_dict) tuples.

    One call typically returns 1 event (a clickstream event),
    but may return 2–3 (clickstream + order + session close)
    when a user completes a purchase.
    """
    events = []

    # Pick a random user
    user    = random.choice(USERS)
    product = random.choice(PRODUCTS)
    session = _get_or_create_session(user)
    session["pages_visited"] += 1

    # Always emit a clickstream event
    click_event = generate_clickstream_event(user, session, product)
    events.append((config.TOPIC_CLICKSTREAM, user["user_id"], click_event))

    # 30% chance: add product to cart
    if random.random() < 0.30:
        session["cart"].append(product)

        # 10% chance: purchase if cart is non-empty
        if session["cart"] and random.random() < 0.10:
            order = generate_order_event(user, session)
            if order:
                events.append((config.TOPIC_ORDERS, user["user_id"], order))
                session["converted"] = True

                # Close the session after purchase
                session_event = _close_session(session)
                events.append(
                    (config.TOPIC_SESSIONS, user["user_id"], session_event)
                )

    # 5% chance: session times out without purchase
    elif random.random() < 0.05 and session["session_id"] in _active_sessions:
        session_event = _close_session(session)
        events.append(
            (config.TOPIC_SESSIONS, user["user_id"], session_event)
        )

    return events