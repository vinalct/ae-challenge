#!/usr/bin/env python3
"""Script local para carregar os arquivos de amostra na bronze."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path

from bronze.loader import load_bronze_sources, prepare_bronze_tables
from bronze.sample_adapters import OPTIONAL_SAMPLE_SOURCES, build_sample_bronze_loads
from common.load_mode import (
    FULL_REFRESH_MODE,
    INCREMENTAL_MODE,
    SUPPORTED_LOAD_MODES,
    resolve_target_snapshot_date,
)
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
        '--mode',
        choices=SUPPORTED_LOAD_MODES,
        default=FULL_REFRESH_MODE,
        help='Modo de carga: full-refresh republica tudo; incremental carrega somente D-1.',
    )
    parser.add_argument(
        '--process-date',
        default=None,
        help='Data de processamento no formato YYYY-MM-DD. No incremental, publica D-1.',
    )
    parser.add_argument(
        '--reset',
        action='store_true',
        help=argparse.SUPPRESS,
    )
    return parser.parse_args()


def main() -> None:
    """Executa a carga local da bronze a partir da pasta data."""
    args = parse_args()
    load_mode = FULL_REFRESH_MODE if args.reset else args.mode
    target_transaction_date = (
        resolve_target_snapshot_date(args.process_date)
        if load_mode == INCREMENTAL_MODE
        else None
    )

    spark = build_local_iceberg_spark(
        app_name='exercise-02-bronze-ingestion',
        project_dir=PROJECT_DIR,
    )
    try:
        prepare_bronze_tables(
            spark=spark,
            ddl_path=args.ddl_path,
            namespace=LOCAL_NAMESPACE,
            reset_namespace=(load_mode == FULL_REFRESH_MODE),
        )
        loads = build_sample_bronze_loads(
            spark=spark,
            data_dir=args.data_dir,
            target_transaction_date=target_transaction_date,
        )
        loaded_counts = load_bronze_sources(
            loads=loads,
            namespace=LOCAL_NAMESPACE,
            batch_id=args.batch_id,
            ingestion_ts=args.ingestion_ts,
        )
        for source_name in OPTIONAL_SAMPLE_SOURCES:
            loaded_counts.setdefault(source_name, 0)

        print('Carga bronze finalizada com sucesso.')
        print(f'- modo: {load_mode}')
        if target_transaction_date is not None:
            print(f'- transaction_date incremental publicada: {target_transaction_date.isoformat()}')
        for source_name, total in loaded_counts.items():
            print(f'- {source_name}: {total} registros ingeridos')
        print(f'Consulte as tabelas em {LOCAL_NAMESPACE} com `make pyspark`.')
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
