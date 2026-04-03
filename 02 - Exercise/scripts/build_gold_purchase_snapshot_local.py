#!/usr/bin/env python3
"""Script local para materializar o snapshot gold de compras."""

from __future__ import annotations

import argparse
from pathlib import Path

from common.local_spark import LOCAL_NAMESPACE, build_local_iceberg_spark
from gold.loader import load_purchase_state_snapshot, prepare_purchase_state_snapshot_table

PROJECT_DIR = Path(__file__).resolve().parents[1]
DEFAULT_DDL_PATH = PROJECT_DIR / 'sql' / 'ddl' / 'gold_purchase_state_snapshot.sql'


def parse_args() -> argparse.Namespace:
    """Le os argumentos do fluxo local do snapshot gold."""
    parser = argparse.ArgumentParser(
        description='Materializa o snapshot gold de estado da compra a partir da silver.',
    )
    parser.add_argument(
        '--ddl-path',
        default=str(DEFAULT_DDL_PATH),
        help='Caminho do DDL da tabela gold. Default: %(default)s',
    )
    parser.add_argument(
        '--snapshot-created-at',
        default=None,
        help='Timestamp fixo para snapshot_created_at no formato YYYY-MM-DD HH:MM:SS.',
    )
    parser.add_argument(
        '--reset',
        action='store_true',
        help='Remove e recria a tabela gold antes da carga.',
    )
    return parser.parse_args()


def main() -> None:
    """Executa a materializacao local do snapshot gold."""
    args = parse_args()
    spark = build_local_iceberg_spark(
        app_name='exercise-02-gold-purchase-snapshot',
        project_dir=PROJECT_DIR,
    )
    try:
        prepare_purchase_state_snapshot_table(
            spark=spark,
            ddl_path=args.ddl_path,
            namespace=LOCAL_NAMESPACE,
            reset_table=args.reset,
        )
        total_rows = load_purchase_state_snapshot(
            spark=spark,
            namespace=LOCAL_NAMESPACE,
            snapshot_created_at=args.snapshot_created_at,
        )
        print('Materializacao gold_purchase_state_snapshot finalizada com sucesso.')
        print(f'- total de linhas publicadas: {total_rows}')
        print(f'Consulte a tabela em {LOCAL_NAMESPACE} com `make pyspark`.')
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
