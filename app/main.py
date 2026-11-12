"""Stellar Wholesale eSIM bridge for Dujiao-Next.

The implementation is split into small modules; this module is the import
entry point (`uvicorn app.main:app`) and re-exports the public helpers that
tests and operators use.
"""

from __future__ import annotations

from . import (  # noqa: F401  (module handles used as patch targets)
    api,
    catalogue,
    logs,
    orders,
    presentation,
    queues,
    settings,
    stellar_client,
    store,
    util,
    wallet_balance,
)
from .api import (  # noqa: F401
    BodyLimitMiddleware,
    app,
    background_tasks,
    cancel_upstream_order,
    create_order,
    get_upstream_order,
    health,
    lifespan,
    product,
    products,
    spawn,
    stellar_webhook,
    worker_loop,
)
from .catalogue import (  # noqa: F401
    best_mainland_plan_ids,
    catalogue_active_plan_count,
    catalogue_is_fresh,
    fetch_catalogue,
    is_daily_configurable,
    is_mainland_china_plan,
    plan_duration_days,
    plan_price_per_gb,
    plan_route_label,
    product_price,
    product_wallet_snapshot,
    reconcile_catalogue_event,
    reserved_wholesale_cost,
    sync_catalogue,
    upsert_plans,
    wallet_capacity_for_plan,
)
from .orders import (  # noqa: F401
    OrderLock,
    cancel_response,
    order_lock,
    order_locks,
    refresh_order,
    save_cancel_results,
    validate_purchase,
)
from .presentation import (  # noqa: F401
    available_plan_rows,
    display_data_label,
    fulfillment_from_stellar,
    order_response,
    product_bundle,
    product_description,
    product_from_plan,
    status_for_dujiao,
    stock_status_for_quantity,
)
from .queues import (  # noqa: F401
    enqueue_callback,
    maintenance_loop,
    outbox_lock,
    process_callback_outbox,
    process_webhook_events,
    prune_durable_queues,
    prune_loop,
    update_order,
    webhook_lock,
)
from .settings import DB_PATH  # noqa: F401  (documented backup snippet)
from .stellar_client import StellarClient, StellarError, stellar  # noqa: F401
from .store import find_order_by_downstream, find_order_by_stellar_id, get_order, get_plan  # noqa: F401
from .util import db_connect, init_db, iso, json_load, now, parse_iso, row_dict  # noqa: F401
from .wallet_balance import wallet_snapshot  # noqa: F401
