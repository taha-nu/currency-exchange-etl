"""Unit tests for the transform logic (run with: pytest tests/)."""
from include.currency_utils import DOCUMENT_FIELDS, order_document, transform_rates

V2_PAYLOAD = [
    {"date": "2026-11-06", "base": "EUR", "quote": "USD", "rate": 1.1512},
    {"date": "2026-11-06", "base": "EUR", "quote": "GBP", "rate": 0.8731},
    {"date": "2026-11-06", "base": "EUR", "quote": "JPY", "rate": 176.42},
]


def test_v2_array_format():
    docs = transform_rates(V2_PAYLOAD, base="EUR", retrieved_at="2026-11-09T07:15:00")
    assert len(docs) == 3
    assert docs[0] == {
        "base_currency": "EUR",
        "target_currency": "USD",
        "exchange_rate": 1.1512,
        "rate_date": "2026-11-06",
        "source": "Frankfurter API",
        "retrieved_at": "2026-11-09T07:15:00",
    }


def test_dict_format_supported():
    payload = {"base": "EUR", "date": "2026-11-06", "rates": {"USD": 1.15, "PKR": 325.1}}
    docs = transform_rates(payload, base="EUR")
    assert {d["target_currency"] for d in docs} == {"USD", "PKR"}
    assert all(set(d) == {"base_currency", "target_currency", "exchange_rate",
                          "rate_date", "source", "retrieved_at"} for d in docs)


def test_field_order_matches_required_shape():
    docs = transform_rates(V2_PAYLOAD, base="EUR")
    assert all(list(d) == list(DOCUMENT_FIELDS) for d in docs)

    # simulate XCom scrambling the key order; order_document must restore it
    scrambled = dict(reversed(list(docs[0].items())))
    assert list(order_document(scrambled)) == list(DOCUMENT_FIELDS)
    assert order_document(scrambled) == docs[0]


def test_invalid_rates_skipped():
    payload = V2_PAYLOAD + [{"date": "2026-11-06", "base": "EUR", "quote": "XXX", "rate": None}]
    assert len(transform_rates(payload, base="EUR")) == 3
