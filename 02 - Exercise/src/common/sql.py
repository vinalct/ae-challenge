"""Utilitarios simples para executar arquivos SQL do projeto."""

from __future__ import annotations

from pathlib import Path

from pyspark.sql import SparkSession


def execute_sql_file(spark: SparkSession, sql_path: str | Path) -> None:
    """Executa um arquivo SQL simples separado por ponto e virgula."""
    statements = [
        statement.strip()
        for statement in Path(sql_path).read_text(encoding='utf-8').split(';')
        if statement.strip()
    ]
    for statement in statements:
        spark.sql(statement)
