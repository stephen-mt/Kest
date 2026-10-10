from __future__ import annotations

import json
import os
from typing import Any

from superset.app import create_app

DATASETS = {
    "mart_daily_revenue": "Canonical daily captured, refunded and net revenue",
    "mart_conversion_funnel": "Campaign funnel sessions and spend",
    "mart_customer_orders": "Order-level customer and financial facts",
}


def get_or_create_database(db: Any, database_model: Any) -> Any:
    database = db.session.query(database_model).filter_by(database_name="Kest Gold").one_or_none()
    uri = f"clickhousedb://superset_ro:{os.environ['CLICKHOUSE_DA_PASSWORD']}@clickhouse:8123/gold"
    if database is None:
        database = database_model(database_name="Kest Gold", sqlalchemy_uri=uri, expose_in_sqllab=True)
        db.session.add(database)
    else:
        database.set_sqlalchemy_uri(uri)
    db.session.flush()
    return database


def get_or_create_dataset(db: Any, dataset_model: Any, database: Any, table_name: str, description: str) -> Any:
    dataset = (
        db.session.query(dataset_model)
        .filter_by(database_id=database.id, schema="gold", table_name=table_name)
        .one_or_none()
    )
    if dataset is None:
        dataset = dataset_model(
            database=database,
            schema="gold",
            table_name=table_name,
            description=description,
            is_sqllab_view=False,
        )
        db.session.add(dataset)
        db.session.flush()
        dataset.fetch_metadata()
    return dataset


def get_or_create_chart(db: Any, chart_model: Any, dataset: Any, name: str, params: dict) -> Any:
    chart = db.session.query(chart_model).filter_by(slice_name=name).one_or_none()
    payload = {"datasource": f"{dataset.id}__table", **params}
    if chart is None:
        chart = chart_model(
            slice_name=name,
            datasource_type="table",
            datasource_id=dataset.id,
            viz_type=payload["viz_type"],
            params=json.dumps(payload),
        )
        db.session.add(chart)
    else:
        chart.datasource_id = dataset.id
        chart.params = json.dumps(payload)
    db.session.flush()
    return chart


def get_or_create_dashboard(
    db: Any, dashboard_model: Any, title: str, slug: str, charts: list[Any]
) -> None:
    dashboard = db.session.query(dashboard_model).filter_by(slug=slug).one_or_none()
    if dashboard is None:
        dashboard = dashboard_model(dashboard_title=title, slug=slug, published=True)
        db.session.add(dashboard)
    dashboard.slices = charts


def main() -> None:
    app = create_app()
    with app.app_context():
        # Superset 6 creates its encrypted model fields during import.  Importing
        # ORM models before create_app() raises "App not initialized yet".
        from superset import db
        from superset.connectors.sqla.models import SqlaTable
        from superset.models.core import Database
        from superset.models.dashboard import Dashboard
        from superset.models.slice import Slice

        database = get_or_create_database(db, Database)
        datasets = {
            name: get_or_create_dataset(db, SqlaTable, database, name, description)
            for name, description in DATASETS.items()
        }
        revenue = get_or_create_chart(
            db,
            Slice,
            datasets["mart_daily_revenue"],
            "Daily Net Revenue",
            {
                "viz_type": "echarts_timeseries_line",
                "granularity_sqla": "order_date",
                "time_grain_sqla": "P1D",
                "metrics": [
                    {
                        "expressionType": "SIMPLE",
                        "column": {"column_name": "net_revenue"},
                        "aggregate": "SUM",
                        "label": "Net revenue",
                    }
                ],
                "groupby": ["currency"],
            },
        )
        orders = get_or_create_chart(
            db,
            Slice,
            datasets["mart_customer_orders"],
            "Customer Orders",
            {
                "viz_type": "table",
                "all_columns": [
                    "order_id",
                    "customer_email",
                    "order_status",
                    "gross_amount",
                    "paid_amount",
                    "refunded_amount",
                    "net_amount",
                ],
                "row_limit": 1000,
            },
        )
        funnel = get_or_create_chart(
            db,
            Slice,
            datasets["mart_conversion_funnel"],
            "Marketing Funnel",
            {
                "viz_type": "table",
                "all_columns": [
                    "event_date",
                    "campaign_name",
                    "viewing_sessions",
                    "cart_sessions",
                    "checkout_sessions",
                    "campaign_spend",
                ],
                "row_limit": 1000,
            },
        )
        get_or_create_dashboard(
            db, Dashboard, "Executive Revenue & Orders", "executive-revenue-orders", [revenue, orders]
        )
        get_or_create_dashboard(
            db, Dashboard, "Marketing Funnel & Spend", "marketing-funnel-spend", [funnel]
        )
        db.session.commit()


if __name__ == "__main__":
    main()
