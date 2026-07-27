import os
import copy
import threading
import time
from datetime import datetime, timezone

from database.auth_db import get_auth_token_broker
from database.bracket_order_db import (
    get_orders_by_status,
    update_bracket_order,
)
from events.order_events import (
    BracketOrderAlertEvent,
    BracketOrderCompletedEvent,
    BracketOrderFailedEvent,
    BracketOrderFilledEvent,
)
from services.bracket_order_service import calculate_exit_prices
from services.cancel_order_service import cancel_order_with_auth
from services.orderstatus_service import get_order_status
from services.place_order_service import place_order
from services.positionbook_service import get_positionbook
from services.quotes_service import get_quotes
from utils.constants import (
    BO_DEFAULT_ACTIVE_POLL_INTERVAL_SECONDS,
    BO_DEFAULT_ENTRY_TIMEOUT_SECONDS,
    BO_DEFAULT_MAX_QUOTE_FAILURES,
    BO_DEFAULT_POLL_INTERVAL_SECONDS,
    BO_DEFAULT_RECON_INTERVAL_CYCLES,
    BO_DEFAULT_SL_RETRY_ATTEMPTS,
)
from utils.event_bus import EventBus
from utils.logging import get_logger

logger = get_logger(__name__)
bus = EventBus()

# Manager state
_running = False
_thread = None

# Polling intervals and defaults
POLL_INTERVAL = int(os.getenv("BO_POLL_INTERVAL_SECONDS", str(BO_DEFAULT_POLL_INTERVAL_SECONDS)))
ACTIVE_POLL_INTERVAL = int(os.getenv("BO_ACTIVE_POLL_INTERVAL_SECONDS", str(BO_DEFAULT_ACTIVE_POLL_INTERVAL_SECONDS)))
ENTRY_TIMEOUT = int(os.getenv("BO_ENTRY_TIMEOUT_SECONDS", str(BO_DEFAULT_ENTRY_TIMEOUT_SECONDS)))
SL_RETRY_ATTEMPTS = int(os.getenv("BO_SL_RETRY_ATTEMPTS", str(BO_DEFAULT_SL_RETRY_ATTEMPTS)))
RECON_INTERVAL_CYCLES = int(os.getenv("BO_RECON_INTERVAL_CYCLES", str(BO_DEFAULT_RECON_INTERVAL_CYCLES)))
MAX_QUOTE_FAILURES = int(os.getenv("BO_MAX_QUOTE_FAILURES", str(BO_DEFAULT_MAX_QUOTE_FAILURES)))

# Tracking consecutive quote failures per active BO
_quote_failure_counts = {}

def _get_time_elapsed(dt_obj):
    if not dt_obj:
        return 0
    now = datetime.now(timezone.utc)
    if dt_obj.tzinfo is None:
        dt_obj = dt_obj.replace(tzinfo=timezone.utc)
    return (now - dt_obj).total_seconds()


def _get_fill_price(order_resp_data: dict, fallback_price: float = 0.0) -> float:
    """
    Extract actual fill price from order status / tradebook response.
    
    CRITICAL FIX: Checks `average_price` FIRST because MARKET orders (or LIMIT orders
    with price protection) carry a nominal order/buffer limit price in `price` (e.g. 582.85),
    whereas the actual executed trade price is in `average_price` (e.g. 529.90).
    Only falls back to `price` if `average_price` is missing or 0.
    """
    if not isinstance(order_resp_data, dict):
        return float(fallback_price)
    avg_price = float(order_resp_data.get("average_price") or 0.0)
    if avg_price > 0:
        return avg_price
    nom_price = float(order_resp_data.get("price") or 0.0)
    if nom_price > 0:
        return nom_price
    return float(fallback_price)


def place_sl_market_exit_with_retries(bo: dict, exit_action: str, max_retries: int = SL_RETRY_ATTEMPTS) -> tuple[bool, dict]:
    """Place an SL MARKET exit order with bounded retry attempts and backoff."""
    sl_payload = {
        "apikey": bo["api_key"],
        "strategy": bo["strategy"],
        "symbol": bo["symbol"],
        "exchange": bo["exchange"],
        "action": exit_action,
        "quantity": str(bo["quantity"]),
        "pricetype": "MARKET",
        "product": bo["product"],
    }
    
    last_resp = {}
    for attempt in range(1, max_retries + 1):
        m_ok, m_resp, _ = place_order(sl_payload, bo["api_key"])
        last_resp = m_resp if isinstance(m_resp, dict) else {}
        if m_ok and last_resp.get("status") == "success":
            logger.info(f"SL Market Exit placed successfully for BO {bo['bo_id']} on attempt {attempt}: {last_resp.get('orderid')}")
            return True, last_resp
        
        logger.warning(f"Attempt {attempt}/{max_retries} to place SL Market Exit for BO {bo['bo_id']} failed: {last_resp}")
        time.sleep(0.5)
        
    return False, last_resp


def verify_order_filled(order_id: str, api_key: str, max_polls: int = 3) -> tuple[bool, float]:
    """Poll order status to explicitly verify order completion and get actual fill price."""
    for _ in range(max_polls):
        st_ok, st_resp, _ = get_order_status({"orderid": order_id}, api_key=api_key)
        if st_ok and st_resp.get("status") == "success":
            data = st_resp.get("data", {})
            if data.get("order_status", "").lower() == "complete":
                return True, _get_fill_price(data)
        time.sleep(0.3)
    return False, 0.0


def _process_pending_entries():
    """Phase A: Monitor entry orders & setup target order."""
    try:
        bos = get_orders_by_status(["ENTRY_PENDING"])
        for bo in bos:
            bo_id = bo["bo_id"]
            entry_order_id = bo["entry_order_id"]
            
            if not entry_order_id:
                logger.warning(f"BO {bo_id} is ENTRY_PENDING but has no entry_order_id")
                continue

            # Check entry order status
            status_data = {"orderid": entry_order_id, "strategy": bo["strategy"]}
            ok, resp, _ = get_order_status(status_data, api_key=bo["api_key"])
            
            if not ok or resp.get("status") != "success":
                continue
                
            data = resp.get("data", {})
            order_status = data.get("order_status", "").lower()

            if order_status == "complete":
                # Entry Filled!
                fill_price = _get_fill_price(data, bo["price"])
                logger.info(f"BO {bo_id} entry filled at {fill_price}")
                
                target_price, sl_price = calculate_exit_prices(
                    entry_price=fill_price,
                    action=bo["action"],
                    target_type=bo["target_type"],
                    target_value=bo["target_value"],
                    sl_type=bo["sl_type"],
                    sl_value=bo["sl_value"]
                )

                exit_action = "SELL" if bo["action"].upper() == "BUY" else "BUY"

                # PRE-PLACEMENT PRICE SANITY CHECK
                ok_quote, q_resp, _ = get_quotes(bo["symbol"], bo["exchange"], api_key=bo["api_key"])
                current_ltp = None
                if ok_quote and q_resp.get("status") == "success":
                    current_ltp = float(q_resp.get("data", {}).get("ltp", 0.0))

                sl_already_crossed = False
                if current_ltp and current_ltp > 0:
                    if bo["action"].upper() == "BUY":
                        sl_already_crossed = (current_ltp <= sl_price)
                    else:
                        sl_already_crossed = (current_ltp >= sl_price)

                if sl_already_crossed:
                    logger.warning(
                        f"BO {bo_id} SL price {sl_price} already crossed at entry fill (LTP={current_ltp}). "
                        f"Skipping target order; executing immediate MARKET exit."
                    )
                    m_ok, m_resp = place_sl_market_exit_with_retries(bo, exit_action)
                    if m_ok:
                        v_ok, m_fill_price = verify_order_filled(m_resp.get("orderid"), bo["api_key"])
                        if m_fill_price <= 0:
                            m_fill_price = sl_price
                        update_bracket_order(bo_id, {
                            "status": "COMPLETED",
                            "exit_type": "STOPLOSS",
                            "exit_price": m_fill_price,
                            "entry_price": fill_price,
                            "target_price": target_price,
                            "sl_price": sl_price,
                            "completed_at": datetime.now(timezone.utc)
                        })
                        bus.publish(BracketOrderCompletedEvent(bo_id=bo_id, exit_type="STOPLOSS", exit_price=m_fill_price))
                    else:
                        update_bracket_order(bo_id, {
                            "status": "CRITICAL_UNPROTECTED",
                            "error_message": "Immediate SL market exit failed after retries"
                        })
                        bus.publish(BracketOrderAlertEvent(bo_id=bo_id, error_message="Immediate SL market exit failed"))
                    continue

                # NORMAL PATH: Place ONLY the Target LIMIT order
                update_bracket_order(bo_id, {
                    "status": "EXIT_PLACING",
                    "entry_price": fill_price,
                    "target_price": target_price,
                    "sl_price": sl_price,
                    "filled_at": datetime.now(timezone.utc)
                })
                
                bus.publish(BracketOrderFilledEvent(
                    bo_id=bo_id, symbol=bo["symbol"], entry_price=fill_price
                ))

                target_payload = {
                    "apikey": bo["api_key"],
                    "strategy": bo["strategy"],
                    "symbol": bo["symbol"],
                    "exchange": bo["exchange"],
                    "action": exit_action,
                    "quantity": str(bo["quantity"]),
                    "pricetype": "LIMIT",
                    "price": str(target_price),
                    "product": bo["product"]
                }
                t_ok, t_resp, _ = place_order(target_payload, bo["api_key"])

                if t_ok and t_resp.get("status") == "success":
                    t_id = t_resp.get("orderid")
                    # NO BLIND TRUST: Explicitly verify target order status
                    st_ok, st_resp, _ = get_order_status({"orderid": t_id, "strategy": bo["strategy"]}, api_key=bo["api_key"])
                    v_status = st_resp.get("data", {}).get("order_status", "").lower() if st_ok else ""

                    if v_status in ["open", "trigger pending", "complete"]:
                        if v_status == "complete":
                            # Target hit immediately!
                            t_fill_price = _get_fill_price(st_resp.get("data", {}), target_price)
                            update_bracket_order(bo_id, {
                                "status": "COMPLETED",
                                "target_order_id": t_id,
                                "exit_type": "TARGET",
                                "exit_price": t_fill_price,
                                "completed_at": datetime.now(timezone.utc)
                            })
                            bus.publish(BracketOrderCompletedEvent(bo_id=bo_id, exit_type="TARGET", exit_price=t_fill_price))
                        else:
                            update_bracket_order(bo_id, {
                                "status": "ACTIVE",
                                "target_order_id": t_id,
                                "sl_order_id": None
                            })
                            logger.info(f"BO {bo_id} Target order placed & confirmed resting ({t_id}). SL ({sl_price}) monitored internally.")
                    else:
                        logger.error(f"BO {bo_id} Target order verification failed (status={v_status}). Emergency market exit.")
                        m_ok, m_resp = place_sl_market_exit_with_retries(bo, exit_action)
                        update_bracket_order(bo_id, {
                            "status": "COMPLETED" if m_ok else "CRITICAL_UNPROTECTED",
                            "error_message": "Target order verification failed"
                        })
                        if not m_ok:
                            bus.publish(BracketOrderAlertEvent(bo_id=bo_id, error_message="Target order verification failed; emergency exit failed"))
                else:
                    logger.error(f"BO {bo_id} Target placement rejected: {t_resp}. Emergency market exit.")
                    m_ok, m_resp = place_sl_market_exit_with_retries(bo, exit_action)
                    update_bracket_order(bo_id, {
                        "status": "COMPLETED" if m_ok else "CRITICAL_UNPROTECTED",
                        "error_message": t_resp.get("message", "Target order placement rejected")
                    })
                    if not m_ok:
                        bus.publish(BracketOrderAlertEvent(bo_id=bo_id, error_message="Target order placement rejected; emergency exit failed"))

            elif order_status in ["rejected", "cancelled"]:
                logger.info(f"BO {bo_id} entry order was {order_status}")
                update_bracket_order(bo_id, {
                    "status": "FAILED" if order_status == "rejected" else "CANCELLED",
                    "error_message": data.get("text", f"Entry {order_status}")
                })

            else:
                # Check timeout
                created_at = datetime.fromisoformat(bo["created_at"]) if bo.get("created_at") else None
                if _get_time_elapsed(created_at) > ENTRY_TIMEOUT:
                    logger.warning(f"BO {bo_id} entry timed out. Cancelling.")
                    auth_token, broker = get_auth_token_broker(bo["api_key"])
                    sd = copy.deepcopy(status_data)
                    sd["apikey"] = bo["api_key"]
                    cancel_order_with_auth(entry_order_id, auth_token, broker, sd)
                    update_bracket_order(bo_id, {"status": "CANCELLED"})

    except Exception as e:
        logger.error(f"Error in _process_pending_entries: {e}")


def _process_active_orders(cycle_count: int):
    """Phase B: Monitor ACTIVE & CRITICAL_UNPROTECTED bracket orders."""
    global _quote_failure_counts
    try:
        bos = get_orders_by_status(["ACTIVE", "CRITICAL_UNPROTECTED"])
        for bo in bos:
            bo_id = bo["bo_id"]
            exit_action = "SELL" if bo["action"].upper() == "BUY" else "BUY"
            is_unprotected = (bo["status"] == "CRITICAL_UNPROTECTED")

            # -------------------------------------------------------------
            # BRANCH FOR CRITICAL_UNPROTECTED ORDERS (RECOVERY & CONTINUOUS RE-ALERTING)
            # -------------------------------------------------------------
            if is_unprotected:
                logger.critical(f"RE-ALERTING: BO {bo_id} is in CRITICAL_UNPROTECTED state! ({bo.get('error_message')}). Attempting recovery...")
                bus.publish(BracketOrderAlertEvent(bo_id=bo_id, error_message=f"CRITICAL_UNPROTECTED re-alert: {bo.get('error_message')}"))

                # Check broker position to see if it's already flat
                pos_ok, pos_resp, _ = get_positionbook(api_key=bo["api_key"])
                if pos_ok and pos_resp.get("status") == "success":
                    pos_list = pos_resp.get("data", [])
                    matching_pos = None
                    if isinstance(pos_list, list):
                        for p in pos_list:
                            if p.get("symbol") == bo["symbol"] and p.get("exchange") == bo["exchange"] and p.get("product") == bo["product"]:
                                matching_pos = p
                                break
                    current_net_qty = int(matching_pos.get("netqty", matching_pos.get("quantity", 0))) if matching_pos else 0
                    if current_net_qty == 0:
                        logger.info(f"BO {bo_id} position is now flat at broker. Resolving CRITICAL_UNPROTECTED to COMPLETED.")
                        update_bracket_order(bo_id, {"status": "COMPLETED", "exit_type": "RESOLVED_FLAT", "completed_at": datetime.now(timezone.utc)})
                        continue

                # Check if target order status has resolved
                if bo.get("target_order_id"):
                    st_ok, st_resp, _ = get_order_status({"orderid": bo["target_order_id"], "strategy": bo["strategy"]}, api_key=bo["api_key"])
                    cur_status = st_resp.get("data", {}).get("order_status", "").lower() if st_ok else ""

                    if cur_status == "complete":
                        fill_p = _get_fill_price(st_resp.get("data", {}), bo.get("target_price", 0.0))
                        update_bracket_order(bo_id, {"status": "COMPLETED", "exit_type": "TARGET", "exit_price": fill_p, "completed_at": datetime.now(timezone.utc)})
                        logger.info(f"BO {bo_id} target completed late. Resolved CRITICAL_UNPROTECTED to COMPLETED.")
                        continue
                    elif cur_status in ["cancelled", "rejected"]:
                        # Target cancellation confirmed late! Attempt SL Market Exit now.
                        m_ok, m_resp = place_sl_market_exit_with_retries(bo, exit_action)
                        if m_ok:
                            v_ok, fill_p = verify_order_filled(m_resp.get("orderid"), bo["api_key"])
                            update_bracket_order(bo_id, {"status": "COMPLETED", "exit_type": "STOPLOSS", "exit_price": fill_p, "completed_at": datetime.now(timezone.utc)})
                            logger.info(f"BO {bo_id} CRITICAL_UNPROTECTED state successfully recovered via SL Market Exit.")
                            continue
                continue

            # -------------------------------------------------------------
            # STEP 1: ULTRA-LOW-LATENCY LTP CHECK (FAST PATH - FIRST)
            # -------------------------------------------------------------
            q_ok, q_resp, _ = get_quotes(bo["symbol"], bo["exchange"], api_key=bo["api_key"])
            if not q_ok or q_resp.get("status") != "success":
                _quote_failure_counts[bo_id] = _quote_failure_counts.get(bo_id, 0) + 1
                if _quote_failure_counts[bo_id] >= MAX_QUOTE_FAILURES:
                    logger.critical(f"CRITICAL ALERT: BO {bo_id} failed to fetch quote for {MAX_QUOTE_FAILURES} consecutive cycles! Transitioning to CRITICAL_UNPROTECTED!")
                    update_bracket_order(bo_id, {
                        "status": "CRITICAL_UNPROTECTED",
                        "error_message": f"{MAX_QUOTE_FAILURES} consecutive quote fetch failures"
                    })
                    bus.publish(BracketOrderAlertEvent(bo_id=bo_id, error_message=f"{MAX_QUOTE_FAILURES} consecutive quote fetch failures"))
                continue

            _quote_failure_counts[bo_id] = 0
            ltp = float(q_resp.get("data", {}).get("ltp", 0.0))

            sl_price = bo["sl_price"]
            sl_breached = False
            if ltp > 0 and sl_price and sl_price > 0:
                if bo["action"].upper() == "BUY":
                    sl_breached = (ltp <= sl_price)
                else:
                    sl_breached = (ltp >= sl_price)

            # -------------------------------------------------------------
            # STEP 2: SL BREACH HANDLING (STRICT CANCEL CONFIRMATION FIX)
            # -------------------------------------------------------------
            if sl_breached:
                breach_time = time.time()
                logger.warning(f"BO {bo_id} SL BREACH DETECTED! LTP={ltp}, SL={sl_price}. Initiating target cancellation.")

                auth_token, broker = get_auth_token_broker(bo["api_key"])
                t_id = bo["target_order_id"]

                # Step a: Cancel resting target order
                if t_id and auth_token:
                    sd = {"orderid": t_id, "strategy": bo["strategy"], "apikey": bo["api_key"]}
                    cancel_order_with_auth(t_id, auth_token, broker, sd)

                # Step b: Poll until target cancellation is STRICTLY CONFIRMED
                cancel_confirmed = False
                target_filled_during_cancel = False
                target_fill_price = 0.0

                for _ in range(5):  # 5 attempts x 200ms
                    time.sleep(0.2)
                    st_ok, st_resp, _ = get_order_status({"orderid": t_id, "strategy": bo["strategy"]}, api_key=bo["api_key"])
                    cur_status = st_resp.get("data", {}).get("order_status", "").lower() if st_ok else ""

                    if cur_status == "complete":
                        target_filled_during_cancel = True
                        target_fill_price = _get_fill_price(st_resp.get("data", {}), bo.get("target_price", 0.0))
                        logger.info(f"BO {bo_id} Target completed at {target_fill_price} during cancellation poll. Target exit takes precedence.")
                        update_bracket_order(bo_id, {
                            "status": "COMPLETED",
                            "exit_type": "TARGET",
                            "exit_price": target_fill_price,
                            "completed_at": datetime.now(timezone.utc)
                        })
                        bus.publish(BracketOrderCompletedEvent(bo_id=bo_id, exit_type="TARGET", exit_price=target_fill_price))
                        break
                    elif cur_status in ["cancelled", "rejected"]:
                        cancel_confirmed = True
                        break

                if target_filled_during_cancel:
                    continue

                if not cancel_confirmed:
                    time.sleep(0.5)
                    st_ok, st_resp, _ = get_order_status({"orderid": t_id, "strategy": bo["strategy"]}, api_key=bo["api_key"])
                    cur_status = st_resp.get("data", {}).get("order_status", "").lower() if st_ok else ""
                    if cur_status in ["cancelled", "rejected"]:
                        cancel_confirmed = True
                    elif cur_status == "complete":
                        target_fill_price = _get_fill_price(st_resp.get("data", {}), bo.get("target_price", 0.0))
                        update_bracket_order(bo_id, {
                            "status": "COMPLETED",
                            "exit_type": "TARGET",
                            "exit_price": target_fill_price,
                            "completed_at": datetime.now(timezone.utc)
                        })
                        bus.publish(BracketOrderCompletedEvent(bo_id=bo_id, exit_type="TARGET", exit_price=target_fill_price))
                        continue

                # IF CANCELLATION IS STILL UNCONFIRMED, ABORT MARKET ORDER PLACEMENT
                if not cancel_confirmed:
                    logger.critical(
                        f"CRITICAL ALERT: BO {bo_id} Target cancellation unconfirmed (status={cur_status}). "
                        f"ABORTING market order placement to prevent double-position! Transitioning to CRITICAL_UNPROTECTED."
                    )
                    update_bracket_order(bo_id, {
                        "status": "CRITICAL_UNPROTECTED",
                        "error_message": f"Target cancel unconfirmed (status={cur_status}). SL market placement aborted."
                    })
                    bus.publish(BracketOrderAlertEvent(bo_id=bo_id, error_message=f"Target cancel unconfirmed (status={cur_status})"))
                    continue

                # Step c: Place SL Market Exit Order ONLY after confirmed cancellation
                m_ok, m_resp = place_sl_market_exit_with_retries(bo, exit_action)

                # Step d: Explicit status verification & Elapsed-time Slippage Logging
                if m_ok and m_resp.get("status") == "success":
                    sl_order_id = m_resp.get("orderid")
                    v_ok, sl_fill_price = verify_order_filled(sl_order_id, bo["api_key"])
                    if sl_fill_price <= 0:
                        sl_fill_price = sl_price

                    elapsed_time = time.time() - breach_time
                    slippage = abs(sl_fill_price - sl_price)

                    logger.info(
                        f"[SL Exit Performance] BO {bo_id} SL Market Exit filled at {sl_fill_price}. "
                        f"Elapsed time: {elapsed_time:.3f}s (Slippage: {slippage:.2f} pts)."
                    )

                    update_bracket_order(bo_id, {
                        "status": "COMPLETED",
                        "exit_type": "STOPLOSS",
                        "exit_price": sl_fill_price,
                        "sl_order_id": sl_order_id,
                        "completed_at": datetime.now(timezone.utc)
                    })
                    bus.publish(BracketOrderCompletedEvent(bo_id=bo_id, exit_type="STOPLOSS", exit_price=sl_fill_price))
                else:
                    logger.critical(f"CRITICAL ALERT: BO {bo_id} Target cancelled but SL Market Exit failed after retries! Transitioning to CRITICAL_UNPROTECTED!")
                    update_bracket_order(bo_id, {
                        "status": "CRITICAL_UNPROTECTED",
                        "error_message": "Target cancelled but SL market exit failed"
                    })
                    bus.publish(BracketOrderAlertEvent(bo_id=bo_id, error_message="Target cancelled but SL market exit failed"))
                continue

            # -------------------------------------------------------------
            # STEP 3: CHECK TARGET ORDER STATUS AT BROKER
            # -------------------------------------------------------------
            if bo.get("target_order_id"):
                st_ok, st_resp, _ = get_order_status({"orderid": bo["target_order_id"], "strategy": bo["strategy"]}, api_key=bo["api_key"])
                t_status = st_resp.get("data", {}).get("order_status", "").lower() if st_ok else ""

                if t_status == "complete":
                    fill_price = _get_fill_price(st_resp.get("data", {}), bo.get("target_price", 0.0))
                    update_bracket_order(bo_id, {
                        "status": "COMPLETED",
                        "exit_type": "TARGET",
                        "exit_price": fill_price,
                        "completed_at": datetime.now(timezone.utc)
                    })
                    bus.publish(BracketOrderCompletedEvent(bo_id=bo_id, exit_type="TARGET", exit_price=fill_price))
                    logger.info(f"BO {bo_id} TARGET HIT at {fill_price}")
                    continue

                if t_status in ["cancelled", "rejected"]:
                    logger.warning(f"BO {bo_id} Target order was {t_status.upper()} externally! Triggering emergency SL market exit.")
                    m_ok, m_resp = place_sl_market_exit_with_retries(bo, exit_action)
                    fill_p = _get_fill_price(m_resp, 0.0) if m_ok else 0.0

                    if m_ok:
                        update_bracket_order(bo_id, {
                            "status": "COMPLETED",
                            "exit_type": "EXTERNAL_CANCEL_SQUAREOFF",
                            "exit_price": fill_p,
                            "completed_at": datetime.now(timezone.utc)
                        })
                        bus.publish(BracketOrderCompletedEvent(bo_id=bo_id, exit_type="EXTERNAL_CANCEL_SQUAREOFF", exit_price=fill_p))
                    else:
                        update_bracket_order(bo_id, {
                            "status": "CRITICAL_UNPROTECTED",
                            "error_message": f"Target {t_status} externally but emergency market exit failed"
                        })
                        bus.publish(BracketOrderAlertEvent(bo_id=bo_id, error_message=f"Target {t_status} externally but emergency exit failed"))
                    continue

            # -------------------------------------------------------------
            # STEP 4: RECONCILIATION SAFETY NET (SLOWER CADENCE: EVERY 5th CYCLE)
            # -------------------------------------------------------------
            if cycle_count % RECON_INTERVAL_CYCLES == 0:
                pos_ok, pos_resp, _ = get_positionbook(api_key=bo["api_key"])
                if pos_ok and pos_resp.get("status") == "success":
                    pos_list = pos_resp.get("data", [])
                    matching_pos = None
                    if isinstance(pos_list, list):
                        for p in pos_list:
                            if p.get("symbol") == bo["symbol"] and p.get("exchange") == bo["exchange"] and p.get("product") == bo["product"]:
                                matching_pos = p
                                break
                    current_net_qty = int(matching_pos.get("netqty", matching_pos.get("quantity", 0))) if matching_pos else 0

                    if current_net_qty == 0:
                        logger.warning(f"RECONCILIATION ALERT: BO {bo_id} position flat at broker. Cancelling target order.")
                        if bo.get("target_order_id"):
                            auth_token, broker = get_auth_token_broker(bo["api_key"])
                            sd = {"orderid": bo["target_order_id"], "strategy": bo["strategy"], "apikey": bo["api_key"]}
                            cancel_order_with_auth(bo["target_order_id"], auth_token, broker, sd)

                        update_bracket_order(bo_id, {
                            "status": "COMPLETED",
                            "exit_type": "EXTERNAL_SQUAREOFF",
                            "completed_at": datetime.now(timezone.utc)
                        })
                        bus.publish(BracketOrderCompletedEvent(bo_id=bo_id, exit_type="EXTERNAL_SQUAREOFF", exit_price=0.0))
                        continue

    except Exception as e:
        logger.error(f"Error in _process_active_orders: {e}")


def _poll_loop():
    logger.info(f"Bracket Order Manager started (pending_interval={POLL_INTERVAL}s, active_interval={ACTIVE_POLL_INTERVAL}s, timeout={ENTRY_TIMEOUT}s)")
    cycle_count = 0
    while _running:
        try:
            cycle_count += 1
            _process_pending_entries()
            _process_active_orders(cycle_count)
        except Exception as e:
            logger.error(f"Error in BO polling loop: {e}")

        # Use ACTIVE_POLL_INTERVAL if any BO is ACTIVE or CRITICAL_UNPROTECTED
        active_bos = get_orders_by_status(["ACTIVE", "CRITICAL_UNPROTECTED"])
        sleep_sec = ACTIVE_POLL_INTERVAL if len(active_bos) > 0 else POLL_INTERVAL
        time.sleep(sleep_sec)


def start_bo_manager():
    """Start the background daemon thread for bracket orders"""
    global _running, _thread

    if _running:
        logger.warning("Bracket Order Manager is already running")
        return

    _running = True
    _thread = threading.Thread(target=_poll_loop, name="BracketOrderManagerThread", daemon=True)
    _thread.start()


def stop_bo_manager():
    """Stop the background daemon thread"""
    global _running
    _running = False
    if _thread:
        _thread.join(timeout=2.0)
