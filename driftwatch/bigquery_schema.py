from __future__ import annotations

from google.api_core.exceptions import NotFound
from google.cloud import bigquery


def ensure_dataset_exists(
    client: bigquery.Client,
    project_id: str,
    dataset_name: str,
    location: str = "US",
) -> None:
    dataset_id = f"{project_id}.{dataset_name}"

    try:
        client.get_dataset(dataset_id)
    except NotFound:
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = location
        client.create_dataset(dataset)


def ensure_table_exists(
    client: bigquery.Client,
    table_id: str,
    schema: list[bigquery.SchemaField],
    *,
    time_partition_field: str | None = None,
    clustering_fields: list[str] | None = None,
) -> None:
    try:
        client.get_table(table_id)
        return
    except NotFound:
        pass

    table = bigquery.Table(table_id, schema=schema)

    if time_partition_field:
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field=time_partition_field,
        )

    if clustering_fields:
        table.clustering_fields = clustering_fields

    client.create_table(table)


def ensure_driftwatch_tables(
    client: bigquery.Client,
    project_id: str,
    dataset_name: str = "driftwatch_prod",
    location: str = "US",
) -> None:
    ensure_dataset_exists(
        client=client,
        project_id=project_id,
        dataset_name=dataset_name,
        location=location,
    )

    dataset_prefix = f"{project_id}.{dataset_name}"

    ohlcv_schema = [
        bigquery.SchemaField("symbol", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("trade_date", "DATE", mode="REQUIRED"),
        bigquery.SchemaField("open", "FLOAT64"),
        bigquery.SchemaField("high", "FLOAT64"),
        bigquery.SchemaField("low", "FLOAT64"),
        bigquery.SchemaField("close", "FLOAT64"),
        bigquery.SchemaField("volume", "INT64"),
        bigquery.SchemaField("avg_volume_30d", "FLOAT64"),
        bigquery.SchemaField("pe_ratio", "FLOAT64"),
        bigquery.SchemaField("ingested_at", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("data_source", "STRING", mode="REQUIRED"),
    ]

    profile_schema = [
        bigquery.SchemaField("symbol", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("snapshot_date", "DATE", mode="REQUIRED"),
        bigquery.SchemaField("net_assets", "FLOAT64"),
        bigquery.SchemaField("fifty_two_week_high", "FLOAT64"),
        bigquery.SchemaField("fifty_two_week_low", "FLOAT64"),
        bigquery.SchemaField("expense_ratio", "FLOAT64"),
        bigquery.SchemaField("category", "STRING"),
        bigquery.SchemaField("fund_family", "STRING"),
        bigquery.SchemaField("benchmark", "STRING"),
        bigquery.SchemaField("ingested_at", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("data_source", "STRING", mode="REQUIRED"),
    ]

    events_schema = [
        bigquery.SchemaField("event_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("symbol", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("event_date", "DATE", mode="REQUIRED"),
        bigquery.SchemaField("event_type", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("confidence_score", "FLOAT64"),
        bigquery.SchemaField("details", "STRING"),
        bigquery.SchemaField("source", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("detection_run_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("detected_at", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("notes", "STRING"),
    ]

    ensure_table_exists(
        client=client,
        table_id=f"{dataset_prefix}.etf_daily_ohlcv",
        schema=ohlcv_schema,
        time_partition_field="trade_date",
        clustering_fields=["symbol"],
    )

    ensure_table_exists(
        client=client,
        table_id=f"{dataset_prefix}.etf_profile_snapshot",
        schema=profile_schema,
        time_partition_field="snapshot_date",
        clustering_fields=["symbol"],
    )

    ensure_table_exists(
        client=client,
        table_id=f"{dataset_prefix}.etf_events",
        schema=events_schema,
        time_partition_field="event_date",
        clustering_fields=["symbol", "event_type"],
    )
