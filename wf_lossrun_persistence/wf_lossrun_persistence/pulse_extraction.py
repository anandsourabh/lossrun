"""
Pulse Extraction Service

Runs a PDF through runpulse.com's two-step Extract -> Schema pipeline:

    1. POST /extract  (multipart file upload)  -> extraction_id
    2. POST /schema   (extraction_id + JSON Schema) -> structured JSON

Returns both the raw Pulse JSON and a validated WFPropertyLossRunExtraction
pydantic instance ready to hand off to LossRunPersistenceService.

Usage:
    from wf_lossrun_persistence.pulse_extraction import PulseExtractionService

    svc = PulseExtractionService(api_key="pk_...")
    result = svc.extract_pdf("loss_run.pdf")
    persist_svc.persist(result.extraction, source_file="loss_run.pdf")

Docs: https://docs.runpulse.com/api-reference/introduction
"""

from __future__ import annotations

import json
import logging
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional, Union

import requests

from .exceptions import PulseExtractionError
from .models import WFPropertyLossRunExtraction

logger = logging.getLogger(__name__)

DEFAULT_BASE_URL = "https://api.runpulse.com"
DEFAULT_SCHEMA_FILENAME = "wf_property_lossrun_extraction_schema.json"
API_KEY_ENV_VAR = "PULSE_API_KEY"


@dataclass
class PulseExtractionResult:
    """Full output of a single extract+schema run."""

    extraction_id: str
    schema_id: Optional[str]
    raw_values: dict[str, Any]
    extraction: WFPropertyLossRunExtraction
    page_count: Optional[int] = None
    citations: Optional[dict[str, Any]] = None


class PulseExtractionService:
    """
    Thin client around the Pulse /extract and /schema endpoints.

    Args:
        api_key: Pulse API key (sent as the x-api-key header). If omitted, falls
                 back to the PULSE_API_KEY environment variable.
        base_url: Override the API host (defaults to https://api.runpulse.com).
        schema: Pre-loaded JSON Schema dict to send as schema_config.input_schema.
                If not supplied, schema_path is used.
        schema_path: Path to a JSON Schema file. If neither schema nor schema_path
                     is given, we locate wf_property_lossrun_extraction_schema.json
                     by walking up from this file.
        schema_prompt: Optional natural-language hint sent alongside the schema.
        effort: When True, enables Pulse's extended-reasoning mode (4 credits/page).
        request_timeout: HTTP timeout for any single request, in seconds.
        poll_interval: Seconds between async job polls.
        poll_timeout: Max total seconds to wait on an async job before erroring.
        use_async: When True, requests async jobs and polls. When False, relies
                   on Pulse's synchronous response (simpler, but may time out for
                   large PDFs).
    """

    def __init__(
        self,
        api_key: Optional[str] = None,
        *,
        base_url: str = DEFAULT_BASE_URL,
        schema: Optional[dict[str, Any]] = None,
        schema_path: Optional[Union[str, Path]] = None,
        schema_prompt: Optional[str] = None,
        effort: bool = False,
        request_timeout: float = 120.0,
        poll_interval: float = 3.0,
        poll_timeout: float = 600.0,
        use_async: bool = True,
    ):
        resolved_key = api_key or os.environ.get(API_KEY_ENV_VAR)
        if not resolved_key:
            raise ValueError(
                f"api_key is required (pass api_key=... or set ${API_KEY_ENV_VAR})"
            )
        self.api_key = resolved_key
        self.base_url = base_url.rstrip("/")
        self.schema_prompt = schema_prompt
        self.effort = effort
        self.request_timeout = request_timeout
        self.poll_interval = poll_interval
        self.poll_timeout = poll_timeout
        self.use_async = use_async

        self._schema = self._resolve_schema(schema, schema_path)

        self._session = requests.Session()
        self._session.headers.update({"x-api-key": resolved_key})

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def extract_pdf(self, pdf_path: Union[str, Path]) -> PulseExtractionResult:
        """Full pipeline: upload PDF, apply the schema, return parsed result."""
        extraction_id, schema_id, raw_values, page_count, citations = self._fetch(pdf_path)
        extraction = WFPropertyLossRunExtraction.model_validate(raw_values)
        return PulseExtractionResult(
            extraction_id=extraction_id,
            schema_id=schema_id,
            raw_values=raw_values,
            extraction=extraction,
            page_count=page_count,
            citations=citations,
        )

    def extract_to_dict(self, pdf_path: Union[str, Path]) -> dict[str, Any]:
        """Run extract+schema and return the raw Pulse JSON, skipping pydantic validation.

        Useful when Pulse returns data that doesn't conform to the pydantic model
        (e.g. a required field came back null) and you need to inspect what it
        actually produced.
        """
        return self._fetch(pdf_path)[2]

    def _fetch(
        self, pdf_path: Union[str, Path]
    ) -> tuple[str, Optional[str], dict[str, Any], Optional[int], Optional[dict[str, Any]]]:
        """Run extract+schema; returns (extraction_id, schema_id, raw_values, page_count, citations)."""
        pdf_path = Path(pdf_path)
        if not pdf_path.is_file():
            raise FileNotFoundError(f"PDF not found: {pdf_path}")

        extraction_id, page_count = self._extract(pdf_path)
        logger.info("Pulse /extract complete: extraction_id=%s pages=%s", extraction_id, page_count)

        schema_id, raw_values, citations = self._apply_schema(extraction_id)
        logger.info("Pulse /schema complete: schema_id=%s fields=%d", schema_id, len(raw_values))

        return extraction_id, schema_id, raw_values, page_count, citations

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _extract(self, pdf_path: Path) -> tuple[str, Optional[int]]:
        """POST /extract with a multipart file upload; returns (extraction_id, page_count)."""
        url = f"{self.base_url}/extract"

        form_fields: dict[str, str] = {
            "storage": json.dumps({"enabled": True}),
        }
        if self.use_async:
            form_fields["async"] = "true"

        with pdf_path.open("rb") as fh:
            files = {"file": (pdf_path.name, fh, "application/pdf")}
            try:
                resp = self._session.post(
                    url, files=files, data=form_fields, timeout=self.request_timeout
                )
            except requests.RequestException as exc:
                raise PulseExtractionError(f"/extract request failed: {exc}") from exc

        payload = self._handle_response(resp, endpoint="/extract")

        # Sync response contains extraction_id directly; async returns job_id.
        if "extraction_id" in payload:
            return payload["extraction_id"], payload.get("page_count")

        job_id = payload.get("job_id")
        if not job_id:
            raise PulseExtractionError(
                "/extract response missing both extraction_id and job_id",
                body=json.dumps(payload)[:2000],
            )

        job_result = self._poll_job(job_id)
        extraction_id = job_result.get("extraction_id")
        if not extraction_id:
            raise PulseExtractionError(
                "/extract job finished without extraction_id",
                body=json.dumps(job_result)[:2000],
            )
        return extraction_id, job_result.get("page_count")

    def _apply_schema(
        self, extraction_id: str
    ) -> tuple[Optional[str], dict[str, Any], Optional[dict[str, Any]]]:
        """POST /schema; returns (schema_id, values, citations)."""
        url = f"{self.base_url}/schema"

        schema_config: dict[str, Any] = {"input_schema": self._schema}
        if self.schema_prompt:
            schema_config["schema_prompt"] = self.schema_prompt
        if self.effort:
            schema_config["effort"] = True

        body: dict[str, Any] = {
            "extraction_id": extraction_id,
            "schema_config": schema_config,
        }
        if self.use_async:
            body["async"] = True

        try:
            resp = self._session.post(
                url,
                json=body,
                headers={"Content-Type": "application/json"},
                timeout=self.request_timeout,
            )
        except requests.RequestException as exc:
            raise PulseExtractionError(f"/schema request failed: {exc}") from exc

        payload = self._handle_response(resp, endpoint="/schema")

        if "schema_output" in payload:
            return self._unpack_schema_output(payload)

        job_id = payload.get("job_id")
        if not job_id:
            raise PulseExtractionError(
                "/schema response missing both schema_output and job_id",
                body=json.dumps(payload)[:2000],
            )

        job_result = self._poll_job(job_id)
        if "schema_output" not in job_result:
            raise PulseExtractionError(
                "/schema job finished without schema_output",
                body=json.dumps(job_result)[:2000],
            )
        return self._unpack_schema_output(job_result)

    @staticmethod
    def _unpack_schema_output(
        payload: dict[str, Any],
    ) -> tuple[Optional[str], dict[str, Any], Optional[dict[str, Any]]]:
        output = payload["schema_output"] or {}
        values = output.get("values")
        if not isinstance(values, dict):
            raise PulseExtractionError(
                "schema_output.values is missing or not an object",
                body=json.dumps(payload)[:2000],
            )
        return payload.get("schema_id"), values, output.get("citations")

    def _poll_job(self, job_id: str) -> dict[str, Any]:
        """GET /job/{job_id} until it reaches a terminal state."""
        url = f"{self.base_url}/job/{job_id}"
        deadline = time.monotonic() + self.poll_timeout

        while True:
            try:
                resp = self._session.get(url, timeout=self.request_timeout)
            except requests.RequestException as exc:
                raise PulseExtractionError(f"Job poll failed for {job_id}: {exc}") from exc

            payload = self._handle_response(resp, endpoint=f"/job/{job_id}")
            status = (payload.get("status") or "").lower()

            if status in {"completed", "complete", "success", "succeeded", "done"}:
                # Pulse nests the result under different keys depending on pipeline step.
                # Merge any of: {result}, {data}, top-level fields.
                return {**payload, **(payload.get("result") or {}), **(payload.get("data") or {})}

            if status in {"failed", "error", "cancelled", "canceled"}:
                raise PulseExtractionError(
                    f"Pulse job {job_id} ended in status '{status}'",
                    body=json.dumps(payload)[:2000],
                )

            if time.monotonic() >= deadline:
                raise PulseExtractionError(
                    f"Pulse job {job_id} did not finish within {self.poll_timeout:.0f}s "
                    f"(last status={status!r})"
                )

            time.sleep(self.poll_interval)

    @staticmethod
    def _handle_response(resp: requests.Response, *, endpoint: str) -> dict[str, Any]:
        if resp.status_code >= 400:
            raise PulseExtractionError(
                f"{endpoint} returned HTTP {resp.status_code}",
                status_code=resp.status_code,
                body=resp.text[:2000],
            )
        try:
            return resp.json()
        except ValueError as exc:
            raise PulseExtractionError(
                f"{endpoint} returned non-JSON body",
                status_code=resp.status_code,
                body=resp.text[:2000],
            ) from exc

    @staticmethod
    def _resolve_schema(
        schema: Optional[dict[str, Any]],
        schema_path: Optional[Union[str, Path]],
    ) -> dict[str, Any]:
        if schema is not None:
            return schema
        if schema_path is not None:
            return json.loads(Path(schema_path).read_text(encoding="utf-8"))

        start = Path(__file__).resolve().parent
        for parent in [start, *start.parents]:
            candidate = parent / DEFAULT_SCHEMA_FILENAME
            if candidate.is_file():
                return json.loads(candidate.read_text(encoding="utf-8"))

        raise FileNotFoundError(
            f"Could not locate {DEFAULT_SCHEMA_FILENAME}. "
            "Pass schema=... or schema_path=... explicitly."
        )
