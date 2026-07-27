import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import time
import pytest
from unittest.mock import MagicMock, patch
from marshmallow import ValidationError

from services.bracket_order_service import calculate_exit_prices, cancel_bracket_order
from restx_api.schemas import BracketOrderSchema
from utils.constants import BO_STATE_ACTIVE, BO_STATE_COMPLETED, BO_STATE_CRITICAL_UNPROTECTED, BO_STATE_FAILED


def test_calculate_exit_prices_buy_points():
    target, sl = calculate_exit_prices(100.0, "BUY", "points", 10.0, "points", 5.0)
    assert target == 110.0
    assert sl == 95.0


def test_calculate_exit_prices_sell_points():
    target, sl = calculate_exit_prices(100.0, "SELL", "points", 10.0, "points", 5.0)
    assert target == 90.0
    assert sl == 105.0


def test_calculate_exit_prices_buy_percentage():
    target, sl = calculate_exit_prices(100.0, "BUY", "percentage", 10.0, "percentage", 5.0)
    assert target == 110.0
    assert sl == 95.0


def test_calculate_exit_prices_sell_percentage():
    target, sl = calculate_exit_prices(100.0, "SELL", "percentage", 10.0, "percentage", 5.0)
    assert target == 90.0
    assert sl == 105.0


def test_calculate_exit_prices_buy_absolute():
    target, sl = calculate_exit_prices(100.0, "BUY", "absolute", 120.0, "absolute", 80.0)
    assert target == 120.0
    assert sl == 80.0


def test_calculate_exit_prices_sell_absolute():
    target, sl = calculate_exit_prices(100.0, "SELL", "absolute", 80.0, "absolute", 120.0)
    assert target == 80.0
    assert sl == 120.0


def test_schema_valid_payload():
    schema = BracketOrderSchema()
    data = {
        "apikey": "test_key",
        "strategy": "test_strat",
        "symbol": "SBIN",
        "exchange": "NSE",
        "action": "BUY",
        "quantity": 1,
        "target_type": "points",
        "target_value": 5.0,
        "sl_type": "points",
        "sl_value": 3.0
    }
    result = schema.load(data)
    assert result["price_type"] == "MARKET"
    assert result["product"] == "MIS"
    assert result["price"] == 0.0


def test_schema_invalid_target_type():
    schema = BracketOrderSchema()
    data = {
        "apikey": "test_key",
        "strategy": "test_strat",
        "symbol": "SBIN",
        "exchange": "NSE",
        "action": "BUY",
        "quantity": 1,
        "target_type": "invalid",
        "target_value": 5.0,
        "sl_type": "points",
        "sl_value": 3.0
    }
    with pytest.raises(ValidationError) as exc:
        schema.load(data)
    assert "target_type" in exc.value.messages


def test_schema_missing_required():
    schema = BracketOrderSchema()
    data = {
        "apikey": "test_key",
        "strategy": "test_strat",
        "symbol": "SBIN",
        "exchange": "NSE",
        "action": "BUY",
        "quantity": 1,
        "target_type": "points",
        "target_value": 5.0
    }
    with pytest.raises(ValidationError) as exc:
        schema.load(data)
    assert "sl_type" in exc.value.messages
    assert "sl_value" in exc.value.messages


# =============================================================================
# REDESIGNED BROKER-NEUTRAL ENGINE UNIT TESTS
# =============================================================================

@patch("services.bracket_order_manager.get_orders_by_status")
@patch("services.bracket_order_manager.get_order_status")
@patch("services.bracket_order_manager.get_quotes")
@patch("services.bracket_order_manager.place_order")
@patch("services.bracket_order_manager.update_bracket_order")
def test_bo_single_resting_order_only(mock_update, mock_place, mock_quotes, mock_order_status, mock_get_orders):
    """Test 1: Single Target LIMIT order placed after entry fill (no parallel SL-M order)."""
    from services.bracket_order_manager import _process_pending_entries

    mock_bo = {
        "bo_id": "bo_101",
        "entry_order_id": "entry_1",
        "api_key": "key1",
        "strategy": "strat1",
        "symbol": "SBIN",
        "exchange": "NSE",
        "action": "BUY",
        "quantity": 10,
        "product": "MIS",
        "price": 500.0,
        "target_type": "points",
        "target_value": 10.0,
        "sl_type": "points",
        "sl_value": 5.0,
    }
    mock_get_orders.return_value = [mock_bo]
    mock_order_status.side_effect = [
        (True, {"status": "success", "data": {"order_status": "complete", "price": 500.0}}, 200),
        (True, {"status": "success", "data": {"order_status": "open"}}, 200),
    ]
    mock_quotes.return_value = (True, {"status": "success", "data": {"ltp": 502.0}}, 200)
    mock_place.return_value = (True, {"status": "success", "orderid": "target_order_1"}, 200)

    _process_pending_entries()

    # Verify place_order was called ONLY ONCE for the Target LIMIT order
    assert mock_place.call_count == 1
    placed_payload = mock_place.call_args[0][0]
    assert placed_payload["pricetype"] == "LIMIT"
    assert placed_payload["price"] == "510.0"
    assert placed_payload["action"] == "SELL"

    # Verify BO updated to ACTIVE with target_order_id and sl_order_id=None
    update_calls = [call[0][1] for call in mock_update.call_args_list]
    assert any(u.get("status") == "ACTIVE" and u.get("target_order_id") == "target_order_1" and u.get("sl_order_id") is None for u in update_calls)


@patch("services.bracket_order_manager.get_orders_by_status")
@patch("services.bracket_order_manager.get_order_status")
@patch("services.bracket_order_manager.get_quotes")
@patch("services.bracket_order_manager.place_order")
@patch("services.bracket_order_manager.update_bracket_order")
def test_bo_market_entry_uses_average_price_for_target_sl_calc(mock_update, mock_place, mock_quotes, mock_order_status, mock_get_orders):
    """
    CRITICAL REGRESSION TEST:
    Simulates production scenario where MARKET entry has nominal limit/buffer price 582.85,
    but actual tradebook fill (average_price) is 529.90.
    Verifies target_price (539.90) and sl_price (514.90) are calculated from average_price (529.90),
    preventing false 'SL already crossed' triggers when LTP is 529.70.
    """
    from services.bracket_order_manager import _process_pending_entries

    mock_bo = {
        "bo_id": "bo_market_1",
        "entry_order_id": "entry_m1",
        "api_key": "key1",
        "strategy": "strat1",
        "symbol": "BANKNIFTY",
        "exchange": "NFO",
        "action": "BUY",
        "quantity": 15,
        "product": "MIS",
        "price": 582.85,  # Nominal buffer price on order
        "target_type": "points",
        "target_value": 10.0,
        "sl_type": "points",
        "sl_value": 15.0,
    }
    mock_get_orders.return_value = [mock_bo]
    # Broker order status returns nominal price 582.85 but executed average_price 529.90
    mock_order_status.side_effect = [
        (True, {"status": "success", "data": {"order_status": "complete", "price": 582.85, "average_price": 529.90}}, 200),
        (True, {"status": "success", "data": {"order_status": "open"}}, 200),
    ]
    # Current LTP is 529.70 (0.20 points below average fill, but 14.8 points ABOVE sl_price of 514.90)
    mock_quotes.return_value = (True, {"status": "success", "data": {"ltp": 529.70}}, 200)
    mock_place.return_value = (True, {"status": "success", "orderid": "target_order_m1"}, 200)

    _process_pending_entries()

    # 1. Target order MUST be placed (not skipped due to false SL trigger!)
    assert mock_place.call_count == 1
    placed_payload = mock_place.call_args[0][0]
    assert placed_payload["pricetype"] == "LIMIT"
    # Target price MUST be 529.90 + 10.0 = 539.90 (NOT 582.85 + 10.0 = 592.85!)
    assert float(placed_payload["price"]) == 539.9

    # 2. Check bracket order status update details
    update_calls = [call[0][1] for call in mock_update.call_args_list]
    placing_update = next(u for u in update_calls if u.get("status") == "EXIT_PLACING")
    assert placing_update["entry_price"] == 529.90
    assert placing_update["target_price"] == 539.90
    assert placing_update["sl_price"] == 514.90


@patch("services.bracket_order_manager.get_orders_by_status")
@patch("services.bracket_order_manager.get_quotes")
@patch("services.bracket_order_manager.get_auth_token_broker")
@patch("services.bracket_order_manager.cancel_order_with_auth")
@patch("services.bracket_order_manager.get_order_status")
@patch("services.bracket_order_manager.place_sl_market_exit_with_retries")
@patch("services.bracket_order_manager.verify_order_filled")
@patch("services.bracket_order_manager.update_bracket_order")
def test_bo_sl_breach_cancel_then_place_market(mock_update, mock_verify, mock_place_mkt, mock_order_status, mock_cancel, mock_auth, mock_quotes, mock_get_orders):
    """Test 2: SL breach triggers sequential target cancel -> confirmed poll -> SL MARKET exit."""
    from services.bracket_order_manager import _process_active_orders

    mock_bo = {
        "bo_id": "bo_102",
        "target_order_id": "target_order_2",
        "api_key": "key1",
        "strategy": "strat1",
        "symbol": "SBIN",
        "exchange": "NSE",
        "action": "BUY",
        "quantity": 10,
        "product": "MIS",
        "sl_price": 495.0,
        "target_price": 510.0,
        "status": "ACTIVE"
    }
    mock_get_orders.return_value = [mock_bo]
    mock_quotes.return_value = (True, {"status": "success", "data": {"ltp": 494.0}}, 200) # LTP crossed SL
    mock_auth.return_value = ("token123", "fyers")
    mock_cancel.return_value = (True, {"status": "success"}, 200)
    mock_order_status.return_value = (True, {"status": "success", "data": {"order_status": "cancelled"}}, 200) # Confirmed cancelled
    mock_place_mkt.return_value = (True, {"status": "success", "orderid": "sl_mkt_1"})
    mock_verify.return_value = (True, 493.5)

    _process_active_orders(cycle_count=1)

    assert mock_cancel.called
    assert mock_place_mkt.called
    update_calls = [call[0][1] for call in mock_update.call_args_list]
    assert any(u.get("status") == "COMPLETED" and u.get("exit_type") == "STOPLOSS" and u.get("exit_price") == 493.5 for u in update_calls)


@patch("services.bracket_order_manager.get_orders_by_status")
@patch("services.bracket_order_manager.get_quotes")
@patch("services.bracket_order_manager.get_auth_token_broker")
@patch("services.bracket_order_manager.cancel_order_with_auth")
@patch("services.bracket_order_manager.get_order_status")
@patch("services.bracket_order_manager.place_sl_market_exit_with_retries")
@patch("services.bracket_order_manager.update_bracket_order")
def test_bo_unconfirmed_cancel_aborts_market_order(mock_update, mock_place_mkt, mock_order_status, mock_cancel, mock_auth, mock_quotes, mock_get_orders):
    """Test 3: Unconfirmed target cancellation strictly aborts market order placement and sets CRITICAL_UNPROTECTED."""
    from services.bracket_order_manager import _process_active_orders

    mock_bo = {
        "bo_id": "bo_103",
        "target_order_id": "target_order_3",
        "api_key": "key1",
        "strategy": "strat1",
        "symbol": "SBIN",
        "exchange": "NSE",
        "action": "BUY",
        "quantity": 10,
        "product": "MIS",
        "sl_price": 495.0,
        "target_price": 510.0,
        "status": "ACTIVE"
    }
    mock_get_orders.return_value = [mock_bo]
    mock_quotes.return_value = (True, {"status": "success", "data": {"ltp": 494.0}}, 200) # SL breached
    mock_auth.return_value = ("token123", "fyers")
    mock_cancel.return_value = (True, {"status": "success"}, 200)
    # Target cancellation status remains 'open' (unconfirmed)
    mock_order_status.return_value = (True, {"status": "success", "data": {"order_status": "open"}}, 200)

    _process_active_orders(cycle_count=1)

    # Market exit MUST NOT be placed
    assert mock_place_mkt.call_count == 0
    update_calls = [call[0][1] for call in mock_update.call_args_list]
    assert any(u.get("status") == "CRITICAL_UNPROTECTED" for u in update_calls)


@patch("services.bracket_order_manager.get_orders_by_status")
@patch("services.bracket_order_manager.get_positionbook")
@patch("services.bracket_order_manager.get_order_status")
@patch("services.bracket_order_manager.place_sl_market_exit_with_retries")
@patch("services.bracket_order_manager.verify_order_filled")
@patch("services.bracket_order_manager.update_bracket_order")
def test_bo_critical_unprotected_continuous_polling_and_recovery(mock_update, mock_verify, mock_place_mkt, mock_order_status, mock_positionbook, mock_get_orders):
    """Test 4: CRITICAL_UNPROTECTED orders remain in poll loop and recover when target cancel completes."""
    from services.bracket_order_manager import _process_active_orders

    mock_bo = {
        "bo_id": "bo_104",
        "target_order_id": "target_order_4",
        "api_key": "key1",
        "strategy": "strat1",
        "symbol": "SBIN",
        "exchange": "NSE",
        "action": "BUY",
        "quantity": 10,
        "product": "MIS",
        "sl_price": 495.0,
        "target_price": 510.0,
        "status": "CRITICAL_UNPROTECTED",
        "error_message": "Target cancel unconfirmed"
    }
    mock_get_orders.return_value = [mock_bo]
    mock_positionbook.return_value = (True, {"status": "success", "data": [{"symbol": "SBIN", "exchange": "NSE", "product": "MIS", "netqty": 10}]}, 200)
    # Target order status now returns cancelled late!
    mock_order_status.return_value = (True, {"status": "success", "data": {"order_status": "cancelled"}}, 200)
    mock_place_mkt.return_value = (True, {"status": "success", "orderid": "sl_rec_1"})
    mock_verify.return_value = (True, 492.0)

    _process_active_orders(cycle_count=1)

    assert mock_place_mkt.called
    update_calls = [call[0][1] for call in mock_update.call_args_list]
    assert any(u.get("status") == "COMPLETED" and u.get("exit_type") == "STOPLOSS" for u in update_calls)


@patch("services.bracket_order_manager.get_orders_by_status")
@patch("services.bracket_order_manager.get_order_status")
@patch("services.bracket_order_manager.get_quotes")
@patch("services.bracket_order_manager.place_sl_market_exit_with_retries")
@patch("services.bracket_order_manager.verify_order_filled")
@patch("services.bracket_order_manager.update_bracket_order")
def test_bo_sl_already_crossed_on_fill(mock_update, mock_verify, mock_place_mkt, mock_quotes, mock_order_status, mock_get_orders):
    """Test 5: On entry fill, if LTP is already beyond SL price, skip target placement and execute market exit."""
    from services.bracket_order_manager import _process_pending_entries

    mock_bo = {
        "bo_id": "bo_105",
        "entry_order_id": "entry_5",
        "api_key": "key1",
        "strategy": "strat1",
        "symbol": "SBIN",
        "exchange": "NSE",
        "action": "BUY",
        "quantity": 10,
        "product": "MIS",
        "price": 500.0,
        "target_type": "points",
        "target_value": 10.0,
        "sl_type": "points",
        "sl_value": 5.0,
    }
    mock_get_orders.return_value = [mock_bo]
    mock_order_status.return_value = (True, {"status": "success", "data": {"order_status": "complete", "price": 500.0}}, 200)
    # Entry fill is 500, calculated SL is 495. Current LTP is 492 (already crossed SL!)
    mock_quotes.return_value = (True, {"status": "success", "data": {"ltp": 492.0}}, 200)
    mock_place_mkt.return_value = (True, {"status": "success", "orderid": "immediate_sl_1"})
    mock_verify.return_value = (True, 491.5)

    _process_pending_entries()

    # Immediate MARKET exit executed; target order NOT placed
    assert mock_place_mkt.called
    update_calls = [call[0][1] for call in mock_update.call_args_list]
    assert any(u.get("status") == "COMPLETED" and u.get("exit_type") == "STOPLOSS" for u in update_calls)


@patch("services.bracket_order_manager.get_orders_by_status")
@patch("services.bracket_order_manager.get_quotes")
@patch("services.bracket_order_manager.get_order_status")
@patch("services.bracket_order_manager.place_sl_market_exit_with_retries")
@patch("services.bracket_order_manager.update_bracket_order")
def test_bo_external_target_cancellation(mock_update, mock_place_mkt, mock_order_status, mock_quotes, mock_get_orders):
    """Test 6: External target order cancellation triggers emergency SL market exit."""
    from services.bracket_order_manager import _process_active_orders

    mock_bo = {
        "bo_id": "bo_106",
        "target_order_id": "target_order_6",
        "api_key": "key1",
        "strategy": "strat1",
        "symbol": "SBIN",
        "exchange": "NSE",
        "action": "BUY",
        "quantity": 10,
        "product": "MIS",
        "sl_price": 495.0,
        "target_price": 510.0,
        "status": "ACTIVE"
    }
    mock_get_orders.return_value = [mock_bo]
    mock_quotes.return_value = (True, {"status": "success", "data": {"ltp": 501.0}}, 200) # LTP safe
    mock_order_status.return_value = (True, {"status": "success", "data": {"order_status": "cancelled"}}, 200) # External cancel
    mock_place_mkt.return_value = (True, {"status": "success", "orderid": "ext_mkt_1", "price": 501.0})

    _process_active_orders(cycle_count=1)

    assert mock_place_mkt.called
    update_calls = [call[0][1] for call in mock_update.call_args_list]
    assert any(u.get("exit_type") == "EXTERNAL_CANCEL_SQUAREOFF" for u in update_calls)


@patch("services.bracket_order_manager.get_orders_by_status")
@patch("services.bracket_order_manager.get_quotes")
@patch("services.bracket_order_manager.update_bracket_order")
def test_bo_consecutive_quote_failures(mock_update, mock_quotes, mock_get_orders):
    """Test 7: 5 consecutive quote fetch failures escalate BO to CRITICAL_UNPROTECTED."""
    from services.bracket_order_manager import _process_active_orders

    mock_bo = {
        "bo_id": "bo_107",
        "target_order_id": "target_order_7",
        "api_key": "key1",
        "strategy": "strat1",
        "symbol": "SBIN",
        "exchange": "NSE",
        "action": "BUY",
        "quantity": 10,
        "product": "MIS",
        "sl_price": 495.0,
        "target_price": 510.0,
        "status": "ACTIVE"
    }
    mock_get_orders.return_value = [mock_bo]
    mock_quotes.return_value = (False, {"status": "error", "message": "Connection error"}, 500)

    for i in range(1, 6):
        _process_active_orders(cycle_count=i)

    update_calls = [call[0][1] for call in mock_update.call_args_list]
    assert any(u.get("status") == "CRITICAL_UNPROTECTED" and "consecutive quote fetch failures" in u.get("error_message", "") for u in update_calls)


@patch("services.bracket_order_service.get_bracket_order_by_bo_id")
@patch("services.bracket_order_service.cancel_order_with_auth")
@patch("services.orderstatus_service.get_order_status")
@patch("services.bracket_order_service.update_bracket_order")
def test_manual_cancel_bracket_order_strict_confirmation(mock_update, mock_order_status, mock_cancel_auth, mock_get_bo):
    """Test 8: Manual cancel_bracket_order(square_off=True) strictly confirms target cancellation before market square-off."""
    mock_bo = {
        "bo_id": "bo_108",
        "target_order_id": "target_order_8",
        "api_key": "key1",
        "strategy": "strat1",
        "symbol": "SBIN",
        "exchange": "NSE",
        "action": "BUY",
        "quantity": 10,
        "product": "MIS",
        "status": "ACTIVE"
    }
    mock_get_bo.return_value = mock_bo
    mock_cancel_auth.return_value = (True, {"status": "success"}, 200)
    mock_order_status.side_effect = [
        (True, {"status": "success", "data": {"order_status": "cancelled"}}, 200),
        (True, {"status": "success", "data": {"order_status": "complete", "price": 500.0}}, 200),
    ]

    with patch("services.place_order_service.place_order") as mock_place:
        mock_place.return_value = (True, {"status": "success", "orderid": "sq_1"}, 200)

        ok, resp, status_code = cancel_bracket_order(
            bo_id="bo_108", api_key="key1", auth_token="token", broker="fyers", square_off=True
        )

        assert ok is True
        assert status_code == 200
        assert mock_place.called
        sq_payload = mock_place.call_args[0][0]
        assert sq_payload["pricetype"] == "MARKET"
        assert sq_payload["action"] == "SELL"
