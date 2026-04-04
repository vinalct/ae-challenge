"""Helpers simples para controlar o modo de carga local."""

from __future__ import annotations

from datetime import date, timedelta

FULL_REFRESH_MODE = 'full-refresh'
INCREMENTAL_MODE = 'incremental'
SUPPORTED_LOAD_MODES = (FULL_REFRESH_MODE, INCREMENTAL_MODE)


def parse_process_date(process_date: str | None) -> date | None:
    """Converte uma string ISO YYYY-MM-DD em date quando informada."""
    if process_date is None:
        return None
    try:
        return date.fromisoformat(process_date)
    except ValueError as exc:
        raise ValueError(
            'process_date must be in YYYY-MM-DD format.'
        ) from exc


def resolve_process_date(process_date: str | None = None) -> date:
    """Retorna a data de processamento informada ou a data atual local."""
    return parse_process_date(process_date) or date.today()


def resolve_target_snapshot_date(process_date: str | None = None) -> date:
    """No incremental, a publicacao sempre considera D-1."""
    return resolve_process_date(process_date) - timedelta(days=1)
