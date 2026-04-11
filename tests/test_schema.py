import sys, os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'spark'))
from schema import event_schema
from pyspark.sql.types import StructType

def test_schema_is_struct_type():
    assert isinstance(event_schema, StructType)

def test_schema_has_all_fields():
    field_names = [f.name for f in event_schema.fields]
    expected = ["user_id", "event_type", "timestamp", "session_id", "device", "page", "country", "amount"]
    for field in expected:
        assert field in field_names

def test_amount_is_nullable():
    amount_field = next(f for f in event_schema.fields if f.name == "amount")
    assert amount_field.nullable is True