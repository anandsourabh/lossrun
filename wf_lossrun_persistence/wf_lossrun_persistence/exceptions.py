"""Custom exceptions for the WF Loss Run persistence service."""


class PersistenceError(Exception):
    """Base class for all persistence service errors."""


class DuplicateReportError(PersistenceError):
    """Raised when a report with the same source_file + valuation_date already exists
    and the caller has opted for strict (non-idempotent) mode."""

    def __init__(self, report_id: str, source_file: str):
        self.report_id = report_id
        self.source_file = source_file
        super().__init__(
            f"Report for '{source_file}' already exists (id={report_id}). "
            "Pass idempotent=True to skip re-processing."
        )


class ValidationError(PersistenceError):
    """Raised when the extraction payload fails pre-persistence validation."""


class PulseExtractionError(Exception):
    """Raised when the Pulse API fails to extract or apply a schema to a document."""

    def __init__(self, message: str, *, status_code: int | None = None, body: str | None = None):
        self.status_code = status_code
        self.body = body
        super().__init__(message)

    def __str__(self) -> str:
        base = super().__str__()
        extras = []
        if self.status_code is not None:
            extras.append(f"status={self.status_code}")
        if self.body:
            snippet = self.body if len(self.body) <= 500 else self.body[:500] + "..."
            extras.append(f"body={snippet}")
        return f"{base} [{'; '.join(extras)}]" if extras else base
