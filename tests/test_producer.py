import json
import sys, os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'kafka'))

from producer import generate_event

def test_event_has_required_fields():
    event = generate_event()
    required = ["user_id", "event_type", "timestamp", "session_id", "device", "page", "country"]
    for field in required:
        assert field in event, f"Missing field: {field}"

def test_event_type_is_valid():
    valid_types = {"page_view", "search", "add_to_cart", "purchase", "logout"}
    for _ in range(20):
        event = generate_event()
        assert event["event_type"] in valid_types

def test_event_is_json_serializable():
    event = generate_event()
    serialized = json.dumps(event)
    deserialized = json.loads(serialized)
    assert deserialized["user_id"] == event["user_id"]

def test_amount_only_on_some_events():
    amounts = [generate_event().get("amount") for _ in range(50)]
    has_amount = [a for a in amounts if a is not None]
    has_none = [a for a in amounts if a is None]
    assert len(has_amount) > 0
    assert len(has_none) > 0