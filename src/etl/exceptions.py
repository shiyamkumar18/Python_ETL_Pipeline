"""Domain exceptions raised by ETL stages."""
from __future__ import annotations

class ETLError(Exception):
    """Base class for expected pipeline failures."""


class ExtractError(ETLError):
    """The source API could not provide a valid response.

    ``rows_extracted`` preserves partial progress when a later page fails.
    Partial batches are never passed to the load stage.
    """

    def __init__(self, message: str, *, rows_extracted: int = 0):
        super().__init__(message)
        self.rows_extracted = rows_extracted


class TransformError(ETLError):
    """The input batch could not be transformed."""


class LoadError(ETLError):
    """The PostgreSQL load or database health check failed."""


class QuarantineError(TransformError):
    """The data-quality rejection rate exceeded the configured limit."""

    def __init__(
        self,
        reason: str,
        *,
        total_count: int,
        valid_count: int,
        rejected_count: int,
        reject_rate_pct: float,
    ) -> None:
        super().__init__(reason)
        self.reason = reason
        self.total_count = total_count
        self.valid_count = valid_count
        self.rejected_count = rejected_count
        self.reject_rate_pct = reject_rate_pct

    @property
    def stats(self) -> dict[str, int | float]:
        return {
            "total_count": self.total_count,
            "valid_count": self.valid_count,
            "rejected_count": self.rejected_count,
            "reject_rate_pct": self.reject_rate_pct,
        }
