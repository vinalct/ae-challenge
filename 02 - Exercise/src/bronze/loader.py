"""Funcoes agnosticas para preparar e carregar a camada bronze."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from pyspark.sql import DataFrame, SparkSession

from common.sql import execute_sql_file
from common.source_schemas import SOURCE_SCHEMAS, get_source_schema
from .ingestion import append_bronze_events

SUPPORTED_BRONZE_SOURCES = frozenset(SOURCE_SCHEMAS)


@dataclass(frozen=True)
class BronzeLoad:
    """Representa uma carga pronta para uma origem bronze."""

    source_name: str
    df: DataFrame
    source_file: str | None = None


def build_namespace_name(namespace: str, catalog: str | None = None) -> str:
    """Monta o nome completo do namespace quando houver catalogo."""
    if catalog:
        return f'{catalog}.{namespace}'
    return namespace


def prepare_bronze_tables(
    spark: SparkSession,
    ddl_path: str | Path,
    namespace: str | None = None,
    catalog: str | None = None,
    reset_namespace: bool = False,
) -> None:
    """Executa o DDL da bronze e opcionalmente recria o namespace."""
    ddl_path_obj = Path(ddl_path)
    if not ddl_path_obj.exists():
        raise FileNotFoundError(f'Arquivo DDL nao encontrado: {ddl_path_obj}')

    if reset_namespace:
        if not namespace:
            raise ValueError('namespace must be provided when reset_namespace=True.')
        full_namespace_name = build_namespace_name(namespace=namespace, catalog=catalog)
        spark.sql(f'DROP NAMESPACE IF EXISTS {full_namespace_name} CASCADE')

    execute_sql_file(spark, ddl_path_obj)


def load_bronze_source(
    load: BronzeLoad,
    namespace: str,
    batch_id: str,
    catalog: str | None = None,
    ingestion_ts: str | None = None,
) -> int:
    """Carrega uma unica origem bronze ja alinhada ao contrato canonico."""
    get_source_schema(load.source_name)
    bronze_df = append_bronze_events(
        df=load.df,
        source_name=load.source_name,
        namespace=namespace,
        batch_id=batch_id,
        catalog=catalog,
        source_file=load.source_file,
        ingestion_ts=ingestion_ts,
    )
    return int(bronze_df.count())


def load_bronze_sources(
    loads: Iterable[BronzeLoad],
    namespace: str,
    batch_id: str,
    catalog: str | None = None,
    ingestion_ts: str | None = None,
) -> dict[str, int]:
    """Carrega um conjunto de origens bronze e devolve o total por origem."""
    loaded_counts: dict[str, int] = {}
    for load in loads:
        if load.source_name in loaded_counts:
            raise ValueError(f'Origem duplicada na mesma carga: {load.source_name}')
        loaded_counts[load.source_name] = load_bronze_source(
            load=load,
            namespace=namespace,
            batch_id=batch_id,
            catalog=catalog,
            ingestion_ts=ingestion_ts,
        )
    return loaded_counts
