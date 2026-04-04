#!/usr/bin/env python3
"""Script local para materializar a camada silver CDC a partir da bronze."""

from __future__ import annotations

import argparse
from pathlib import Path

from common.load_mode import (
    FULL_REFRESH_MODE,
    INCREMENTAL_MODE,
    SUPPORTED_LOAD_MODES,
    resolve_target_snapshot_date,
)
from common.local_spark import LOCAL_NAMESPACE, build_local_iceberg_spark
from silver.loader import load_silver_sources, prepare_silver_tables

PROJECT_DIR = Path(__file__).resolve().parents[1]
DEFAULT_DDL_PATH = PROJECT_DIR / 'sql' / 'ddl' / 'silver_tables.sql'


def parse_args() -> argparse.Namespace:
    """Le os argumentos do fluxo local da silver."""
    parser = argparse.ArgumentParser(
        description='Materializa as tabelas silver CDC locais a partir da bronze.',
    )
    parser.add_argument(
        '--ddl-path',
        default=str(DEFAULT_DDL_PATH),
        help='Caminho do DDL das tabelas silver. Default: %(default)s',
    )
    parser.add_argument(
        '--mode',
        choices=SUPPORTED_LOAD_MODES,
        default=FULL_REFRESH_MODE,
        help='Modo de carga: full-refresh republica tudo; incremental atualiza somente D-1.',
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
    """Executa a materializacao local da silver."""
    args = parse_args()
    load_mode = FULL_REFRESH_MODE if args.reset else args.mode
    target_transaction_date = (
        resolve_target_snapshot_date(args.process_date)
        if load_mode == INCREMENTAL_MODE
        else None
    )

    spark = build_local_iceberg_spark(
        app_name='exercise-02-silver-standardization',
        project_dir=PROJECT_DIR,
    )
    try:
        prepare_silver_tables(
            spark=spark,
            ddl_path=args.ddl_path,
            namespace=LOCAL_NAMESPACE,
            reset_tables=(load_mode == FULL_REFRESH_MODE),
        )
        loaded_counts = load_silver_sources(
            spark=spark,
            namespace=LOCAL_NAMESPACE,
            load_mode=load_mode,
            target_transaction_date=target_transaction_date,
        )
        print('Materializacao silver finalizada com sucesso.')
        print(f'- modo: {load_mode}')
        if target_transaction_date is not None:
            print(f'- transaction_date incremental republicada: {target_transaction_date.isoformat()}')
        for source_name, total in loaded_counts.items():
            print(f'- {source_name}: {total} registros publicados')
        print(f'Consulte as tabelas em {LOCAL_NAMESPACE} com `make pyspark`.')
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
