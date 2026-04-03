#!/usr/bin/env python3
"""Console interativo com Spark local apontando para a bronze."""

from __future__ import annotations

import code
from pathlib import Path

from common.local_spark import LOCAL_CATALOG, LOCAL_NAMESPACE, build_local_iceberg_spark

PROJECT_DIR = Path(__file__).resolve().parents[1]


def main() -> None:
    """Abre um console interativo com catalogo e namespace ja selecionados."""
    spark = build_local_iceberg_spark(
        app_name='exercise-02-bronze-console',
        project_dir=PROJECT_DIR,
    )
    try:
        spark.catalog.setCurrentCatalog(LOCAL_CATALOG)
        spark.catalog.setCurrentDatabase(LOCAL_NAMESPACE)

        print(f'Catalogo atual: {LOCAL_CATALOG}')
        print(f'Namespace atual: {LOCAL_NAMESPACE}')
        print('Tabelas disponiveis:')
        spark.sql('SHOW TABLES').show(truncate=False)

        banner = (
            'Console Spark pronto.\n'
            'Exemplos:\n'
            '  spark.sql("SHOW TABLES").show(truncate=False)\n'
            '  spark.sql("SELECT * FROM bronze_purchase_events").show(truncate=False)\n'
        )
        code.interact(banner=banner, local={'spark': spark})
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
