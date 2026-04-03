#!/usr/bin/env python3
"""Script local para materializar a camada silver CDC a partir da bronze."""

from __future__ import annotations

import argparse
from pathlib import Path

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
        '--reset',
        action='store_true',
        help='Remove e recria as tabelas silver antes da carga.',
    )
    return parser.parse_args()


def main() -> None:
    """Executa a materializacao local da silver."""
    args = parse_args()
    spark = build_local_iceberg_spark(
        app_name='exercise-02-silver-standardization',
        project_dir=PROJECT_DIR,
    )
    try:
        prepare_silver_tables(
            spark=spark,
            ddl_path=args.ddl_path,
            namespace=LOCAL_NAMESPACE,
            reset_tables=args.reset,
        )
        loaded_counts = load_silver_sources(
            spark=spark,
            namespace=LOCAL_NAMESPACE,
        )
        print('Materializacao silver finalizada com sucesso.')
        for source_name, total in loaded_counts.items():
            print(f'- {source_name}: {total} registros publicados')
        print(f'Consulte as tabelas em {LOCAL_NAMESPACE} com `make pyspark`.')
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
