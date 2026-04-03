"""Configuracao do Spark local usada para desenvolvimento e validacao."""

from __future__ import annotations

from pathlib import Path

from pyspark.sql import SparkSession

LOCAL_CATALOG = 'local'
LOCAL_NAMESPACE = 'ae_challenge'
ICEBERG_PACKAGE = 'org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2'


def local_warehouse_path(project_dir: str | Path = '/workspace') -> str:
    """Retorna o caminho do warehouse local usado na execucao."""
    return f"file://{Path(project_dir) / '.local' / 'warehouse'}"


def build_local_iceberg_spark(
    app_name: str,
    project_dir: str | Path = '/workspace',
) -> SparkSession:
    """Cria uma SparkSession local com catalogo Iceberg baseado em arquivos."""
    warehouse = local_warehouse_path(project_dir)
    return (
        SparkSession.builder.master('local[1]')
        .appName(app_name)
        .config('spark.ui.enabled', 'false')
        .config('spark.jars.packages', ICEBERG_PACKAGE)
        .config(
            'spark.sql.extensions',
            'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions',
        )
        .config(f'spark.sql.catalog.{LOCAL_CATALOG}', 'org.apache.iceberg.spark.SparkCatalog')
        .config(f'spark.sql.catalog.{LOCAL_CATALOG}.type', 'hadoop')
        .config(f'spark.sql.catalog.{LOCAL_CATALOG}.warehouse', warehouse)
        .config('spark.sql.defaultCatalog', LOCAL_CATALOG)
        .getOrCreate()
    )
