"""Fixtures compartilhadas dos testes PySpark do exercicio."""

from __future__ import annotations

from datetime import datetime, timedelta

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType, TimestampType

from common.source_schemas import get_source_schema


def _schema_with_bronze_metadata(source_name: str) -> StructType:
    """Retorna o schema de origem acrescido dos metadados bronze."""
    source_schema = get_source_schema(source_name)
    return StructType(
        [
            *source_schema.fields,
            StructField('ingestion_ts', TimestampType(), True),
            StructField('batch_id', StringType(), True),
            StructField('source_file', StringType(), True),
            StructField('record_hash', StringType(), True),
        ]
    )


@pytest.fixture(scope='session')
def spark() -> SparkSession:
    """Cria uma SparkSession simples para os testes unitarios."""
    session = (
        SparkSession.builder.master('local[1]')
        .appName('exercise-02-tests')
        .config('spark.ui.enabled', 'false')
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture
def bronze_df_builder(spark: SparkSession):
    """Constroi dataframes bronze de teste com schema e metadados canonicos."""

    def _build(source_name: str, rows: list[dict[str, object]]):
        schema = _schema_with_bronze_metadata(source_name)
        prepared_rows = []
        for index, row in enumerate(rows):
            prepared_rows.append(
                {
                    'ingestion_ts': datetime(2024, 1, 1, 0, 0, 0) + timedelta(seconds=index),
                    'batch_id': 'test-batch',
                    'source_file': f'{source_name}.csv',
                    'record_hash': f'{source_name}-{index}',
                    **row,
                }
            )
        return spark.createDataFrame(prepared_rows, schema=schema)

    return _build
