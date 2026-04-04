#!/usr/bin/env python3
"""Script local para materializar a agregacao final de GMV por subsidiaria."""

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
from gold.gmv_loader import (
    load_gmv_daily_by_subsidiary,
    prepare_gmv_daily_by_subsidiary_table,
)

PROJECT_DIR = Path(__file__).resolve().parents[1]
DEFAULT_DDL_PATH = PROJECT_DIR / 'sql' / 'ddl' / 'gold_gmv_daily_by_subsidiary_snapshot.sql'


def parse_args() -> argparse.Namespace:
    """Le os argumentos do fluxo local do agregado final de GMV."""
    parser = argparse.ArgumentParser(
        description='Materializa a tabela final de GMV diario por subsidiaria a partir do snapshot gold.',
    )
    parser.add_argument(
        '--ddl-path',
        default=str(DEFAULT_DDL_PATH),
        help='Caminho do DDL da tabela final. Default: %(default)s',
    )
    parser.add_argument(
        '--mode',
        choices=SUPPORTED_LOAD_MODES,
        default=FULL_REFRESH_MODE,
        help='Modo de carga: full-refresh republica tudo; incremental publica somente D-1.',
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
    """Executa a materializacao local do agregado final de GMV."""
    args = parse_args()
    load_mode = FULL_REFRESH_MODE if args.reset else args.mode
    target_snapshot_date = (
        resolve_target_snapshot_date(args.process_date)
        if load_mode == INCREMENTAL_MODE
        else None
    )

    spark = build_local_iceberg_spark(
        app_name='exercise-02-gold-gmv-daily-by-subsidiary',
        project_dir=PROJECT_DIR,
    )
    try:
        prepare_gmv_daily_by_subsidiary_table(
            spark=spark,
            ddl_path=args.ddl_path,
            namespace=LOCAL_NAMESPACE,
            reset_objects=(load_mode == FULL_REFRESH_MODE),
        )
        total_rows, current_view_created = load_gmv_daily_by_subsidiary(
            spark=spark,
            namespace=LOCAL_NAMESPACE,
            load_mode=load_mode,
            target_snapshot_date=target_snapshot_date,
        )
        print('Materializacao gold_gmv_daily_by_subsidiary_snapshot finalizada com sucesso.')
        print(f'- modo: {load_mode}')
        if target_snapshot_date is not None:
            print(f'- snapshot_date incremental publicada: {target_snapshot_date.isoformat()}')
        print(f'- total de linhas publicadas: {total_rows}')
        if current_view_created:
            print(
                'Consulte a tabela final e a view atual em '
                f'{LOCAL_NAMESPACE} com `make pyspark`.'
            )
        else:
            print(
                'O catalogo Iceberg local nao suporta views persistidas. '
                'Consulte a tabela final e use uma consulta com '
                '`MAX(snapshot_date)` para ler o snapshot atual em '
                f'{LOCAL_NAMESPACE}.'
            )
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
