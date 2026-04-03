#!/usr/bin/env python3
"""Script local para materializar as saidas de qualidade e observabilidade."""

from __future__ import annotations

import argparse
from pathlib import Path

from common.local_spark import LOCAL_NAMESPACE, build_local_iceberg_spark
from ops.loader import load_data_quality_outputs, prepare_ops_tables

PROJECT_DIR = Path(__file__).resolve().parents[1]
DEFAULT_DDL_PATH = PROJECT_DIR / 'sql' / 'ddl' / 'ops_tables.sql'


def parse_args() -> argparse.Namespace:
    """Le os argumentos do fluxo local de qualidade e observabilidade."""
    parser = argparse.ArgumentParser(
        description='Avalia os checks de bronze, silver e gold e publica as tabelas OPS.',
    )
    parser.add_argument(
        '--ddl-path',
        default=str(DEFAULT_DDL_PATH),
        help='Caminho do DDL das tabelas OPS. Default: %(default)s',
    )
    parser.add_argument(
        '--reset',
        action='store_true',
        help='Remove e recria as tabelas operacionais antes da carga.',
    )
    return parser.parse_args()


def main() -> None:
    """Executa a avaliacao local de qualidade e observabilidade."""
    args = parse_args()
    spark = build_local_iceberg_spark(
        app_name='exercise-02-data-quality-and-observability',
        project_dir=PROJECT_DIR,
    )
    try:
        prepare_ops_tables(
            spark=spark,
            ddl_path=args.ddl_path,
            namespace=LOCAL_NAMESPACE,
            reset_tables=args.reset,
        )
        summary = load_data_quality_outputs(
            spark=spark,
            namespace=LOCAL_NAMESPACE,
        )
        print('Avaliacao de qualidade e observabilidade finalizada com sucesso.')
        print(f'- run_id: {summary.run_id}')
        print(f'- status geral: {summary.pipeline_status}')
        print(f'- total de checks publicados: {summary.total_checks}')
        print(f'- checks bloqueantes falhos: {summary.failed_error_checks}')
        print(f'- checks nao bloqueantes falhos: {summary.failed_warning_checks}')
        print(f'- linhas publicadas em quarantine: {summary.quarantine_rows}')
        if summary.pipeline_status == 'failed':
            raise SystemExit(1)
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
