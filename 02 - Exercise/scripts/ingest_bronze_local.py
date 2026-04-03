#!/usr/bin/env python3
"""Script local para carregar os arquivos de amostra na bronze."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path

from bronze.loader import load_bronze_sources, prepare_bronze_tables
from bronze.sample_adapters import OPTIONAL_SAMPLE_SOURCES, build_sample_bronze_loads
from common.local_spark import LOCAL_NAMESPACE, build_local_iceberg_spark

PROJECT_DIR = Path(__file__).resolve().parents[1]
DEFAULT_DATA_DIR = PROJECT_DIR / 'data'
DEFAULT_DDL_PATH = PROJECT_DIR / 'sql' / 'ddl' / 'bronze_tables.sql'


def parse_args() -> argparse.Namespace:
    """Le os argumentos do fluxo local de ingestao bronze."""
    parser = argparse.ArgumentParser(
        description='Carrega os arquivos txt de data/ nas tabelas bronze Iceberg locais.',
    )
    parser.add_argument(
        '--data-dir',
        default=str(DEFAULT_DATA_DIR),
        help='Pasta com os arquivos txt de entrada. Default: %(default)s',
    )
    parser.add_argument(
        '--ddl-path',
        default=str(DEFAULT_DDL_PATH),
        help='Caminho do DDL das tabelas bronze. Default: %(default)s',
    )
    parser.add_argument(
        '--batch-id',
        default=f"local-bronze-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}",
        help='Identificador do lote de ingestao. Default: gerado automaticamente.',
    )
    parser.add_argument(
        '--ingestion-ts',
        default=None,
        help='Timestamp fixo para ingestion_ts no formato YYYY-MM-DD HH:MM:SS.',
    )
    parser.add_argument(
        '--reset',
        action='store_true',
        help='Remove e recria apenas as tabelas bronze locais antes da carga.',
    )
    return parser.parse_args()


def main() -> None:
    """Executa a carga local da bronze a partir da pasta data."""
    args = parse_args()
    spark = build_local_iceberg_spark(
        app_name='exercise-02-bronze-ingestion',
        project_dir=PROJECT_DIR,
    )
    try:
        prepare_bronze_tables(
            spark=spark,
            ddl_path=args.ddl_path,
            namespace=LOCAL_NAMESPACE,
            reset_namespace=args.reset,
        )
        loads = build_sample_bronze_loads(spark=spark, data_dir=args.data_dir)
        loaded_counts = load_bronze_sources(
            loads=loads,
            namespace=LOCAL_NAMESPACE,
            batch_id=args.batch_id,
            ingestion_ts=args.ingestion_ts,
        )
        for source_name in OPTIONAL_SAMPLE_SOURCES:
            loaded_counts.setdefault(source_name, 0)

        print('Carga bronze finalizada com sucesso.')
        for source_name, total in loaded_counts.items():
            print(f'- {source_name}: {total} registros ingeridos')
        print(f'Consulte as tabelas em {LOCAL_NAMESPACE} com `make pyspark`.')
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
