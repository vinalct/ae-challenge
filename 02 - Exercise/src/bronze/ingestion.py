"""Funcoes de apoio para enriquecer e gravar eventos brutos na bronze."""

from __future__ import annotations

from typing import Iterable

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from .contracts import BronzeTableSpec, get_bronze_table_spec


METADATA_COLUMNS = ('ingestion_ts', 'batch_id', 'source_file', 'record_hash')


def build_table_name(namespace: str, table_name: str, catalog: str | None = None) -> str:
    """Monta o nome completo da tabela considerando namespace e catalogo."""
    if catalog:
        return f'{catalog}.{namespace}.{table_name}'
    return f'{namespace}.{table_name}'


def _validate_source_columns(df: DataFrame, spec: BronzeTableSpec) -> None:
    """Valida se o dataframe possui exatamente as colunas esperadas."""
    expected = list(spec.schema.fieldNames())
    actual = list(df.columns)

    missing = [column for column in expected if column not in actual]
    unexpected = [column for column in actual if column not in expected]

    if missing or unexpected:
        parts = []
        if missing:
            parts.append(f'missing columns: {missing}')
        if unexpected:
            parts.append(f'unexpected columns: {unexpected}')
        raise ValueError(
            f"Schema mismatch for source '{spec.source_name}': " + '; '.join(parts)
        )


def _cast_to_spec_schema(df: DataFrame, spec: BronzeTableSpec) -> DataFrame:
    """Aplica cast para o schema bronze da origem."""
    return df.select(
        [F.col(field.name).cast(field.dataType).alias(field.name) for field in spec.schema.fields]
    )


def _payload_columns(spec: BronzeTableSpec) -> Iterable[str]:
    """Retorna as colunas do payload usadas para gerar o hash."""
    return spec.schema.fieldNames()


def enrich_bronze_dataframe(
    df: DataFrame,
    source_name: str,
    batch_id: str,
    source_file: str | None = None,
    ingestion_ts: str | None = None,
) -> DataFrame:
    """Enriquece o dataframe raw com metadados de ingestao da bronze."""
    if not batch_id:
        raise ValueError('batch_id must be provided for bronze ingestion.')

    spec = get_bronze_table_spec(source_name)
    _validate_source_columns(df, spec)

    typed_df = _cast_to_spec_schema(df, spec)
    payload_json = F.to_json(F.struct(*[F.col(column) for column in _payload_columns(spec)]))
    ingestion_ts_expr = (
        F.current_timestamp()
        if ingestion_ts is None
        else F.lit(ingestion_ts).cast('timestamp')
    )

    return (
        typed_df.withColumn('ingestion_ts', ingestion_ts_expr)
        .withColumn('batch_id', F.lit(batch_id))
        .withColumn('source_file', F.lit(source_file).cast('string'))
        .withColumn('record_hash', F.sha2(payload_json, 256))
    )


def append_bronze_events(
    df: DataFrame,
    source_name: str,
    namespace: str,
    batch_id: str,
    catalog: str | None = None,
    source_file: str | None = None,
    ingestion_ts: str | None = None,
) -> DataFrame:
    """Enriquece e grava os eventos na tabela bronze de destino."""
    spec = get_bronze_table_spec(source_name)
    bronze_df = enrich_bronze_dataframe(
        df=df,
        source_name=source_name,
        batch_id=batch_id,
        source_file=source_file,
        ingestion_ts=ingestion_ts,
    )
    full_table_name = build_table_name(
        namespace=namespace,
        table_name=spec.target_table,
        catalog=catalog,
    )
    bronze_df.writeTo(full_table_name).append()
    return bronze_df
