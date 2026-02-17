"""
LLM Extraction Service for Silver Layer

Uses PydanticAI with OpenAI-compatible APIs to extract structured data
(bool/enum) from free-text columns. Supports batch processing with
configurable concurrency and automatic retry with exponential backoff
for rate-limited (429) and transient errors.

Configuration via environment variables:
    LLM_BASE_URL: API base URL (default: https://api.openai.com/v1)
    LLM_API_KEY: API authentication key
    LLM_MODEL: Model name (default: gpt-4o-mini)
    LLM_MAX_CONCURRENT: Max concurrent LLM calls (default: 10)
    LLM_MAX_RETRIES: Max retries per row on transient errors (default: 5)
"""

import os
import re
import asyncio
import logging
from typing import Any, Optional
from enum import Enum

from pydantic import BaseModel, Field, create_model
from pydantic_ai import Agent
from pydantic_ai.models.openai import OpenAIChatModel
from pydantic_ai.providers.openai import OpenAIProvider

logger = logging.getLogger(__name__)

# Patterns to detect retryable errors
_RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 529}
_RETRY_AFTER_PATTERN = re.compile(r'retry\s+(?:in|after)\s+([\d.]+)', re.IGNORECASE)


# ==================== DYNAMIC OUTPUT MODELS ====================

class BoolExtraction(BaseModel):
    """Structured output for boolean extraction from text."""
    result: bool = Field(description="True if the condition described in the prompt is met, False otherwise")


def create_enum_model(enum_values: list[str]) -> type[BaseModel]:
    """
    Dynamically create a Pydantic model with an Enum field constrained
    to the specified values. PydanticAI will enforce the LLM returns
    only one of these values via structured output.
    """
    DynamicEnum = Enum("DynamicEnum", {v: v for v in enum_values})

    model = create_model(
        "EnumExtraction",
        result=(DynamicEnum, Field(description=f"Must be one of: {', '.join(enum_values)}")),
    )
    return model


# ==================== SERVICE ====================

class LLMExtractionService:
    """
    Service for extracting structured data from free-text using LLM.

    Uses PydanticAI Agent with structured output_type to guarantee
    the LLM returns valid bool or enum values.
    """

    def __init__(self):
        self.base_url = os.getenv("LLM_BASE_URL", "https://api.openai.com/v1")
        self.api_key = os.getenv("LLM_API_KEY")
        self.model_name = os.getenv("LLM_MODEL", "gpt-4o-mini")
        self.max_concurrent = int(os.getenv("LLM_MAX_CONCURRENT", "10"))
        self.max_retries = int(os.getenv("LLM_MAX_RETRIES", "5"))

        if not self.api_key:
            logger.warning(
                "LLM_API_KEY not set. LLM extraction features will fail at runtime. "
                "Set LLM_API_KEY in your .env file."
            )

        self._model = None

    @property
    def model(self) -> OpenAIChatModel:
        """Lazy initialization of the OpenAI model."""
        if self._model is None:
            if not self.api_key:
                raise ValueError(
                    "LLM_API_KEY environment variable is not set. "
                    "Configure it in your .env file to use LLM extraction features."
                )
            self._model = OpenAIChatModel(
                self.model_name,
                provider=OpenAIProvider(
                    base_url=self.base_url,
                    api_key=self.api_key,
                ),
            )
        return self._model

    def _build_system_prompt(self, user_prompt: str, output_type: str, enum_values: Optional[list[str]] = None) -> str:
        """Build a clear system prompt for the extraction task."""
        base = (
            "You are a precise data extraction assistant. "
            "You will receive a text and must extract structured information from it.\n\n"
            f"EXTRACTION TASK: {user_prompt}\n\n"
        )

        if output_type == "bool":
            base += (
                "You must respond with a boolean value (true or false).\n"
                "- Respond true ONLY if the text clearly supports the condition.\n"
                "- Respond false if the condition is not met, unclear, or not mentioned.\n"
                "- When in doubt, respond false."
            )
        elif output_type == "enum":
            base += (
                f"You must classify the text into exactly ONE of these categories: {', '.join(enum_values or [])}\n"
                "- Choose the category that best matches the text content.\n"
                "- If none clearly applies, choose the most neutral/default option available."
            )

        return base

    def _is_retryable(self, error: Exception) -> bool:
        """Check if an error is retryable (rate limit, server error, etc.)."""
        error_str = str(error)
        # Check for status codes in the error message
        for code in _RETRYABLE_STATUS_CODES:
            if f"status_code: {code}" in error_str or f"'{code}'" in error_str:
                return True
        # Check for common retryable patterns
        retryable_phrases = [
            "rate limit", "rate_limit", "quota exceeded", "resource_exhausted",
            "too many requests", "server error", "service unavailable",
            "internal server error", "overloaded", "capacity",
        ]
        error_lower = error_str.lower()
        return any(phrase in error_lower for phrase in retryable_phrases)

    def _parse_retry_delay(self, error: Exception) -> Optional[float]:
        """Try to parse a retry delay from the error message (e.g. 'retry in 53.7s')."""
        match = _RETRY_AFTER_PATTERN.search(str(error))
        if match:
            try:
                return float(match.group(1))
            except ValueError:
                pass
        return None

    async def extract_single(
        self,
        text: str,
        prompt: str,
        output_type: str,
        enum_values: Optional[list[str]] = None,
    ) -> dict[str, Any]:
        """
        Extract structured data from a single text with automatic retry.

        Retries with exponential backoff on rate-limit (429) and transient
        server errors (500/502/503). Respects Retry-After hints from the API.

        Returns:
            {"success": True, "result": <value>} or
            {"success": False, "result": None, "error": "<message>"}
        """
        if not text or not text.strip():
            return {"success": True, "result": None}

        system_prompt = self._build_system_prompt(prompt, output_type, enum_values)

        if output_type == "bool":
            output_model = BoolExtraction
        else:
            output_model = create_enum_model(enum_values)

        agent = Agent(
            self.model,
            output_type=output_model,
            instructions=system_prompt,
        )

        last_error = None
        for attempt in range(1, self.max_retries + 1):
            try:
                result = await agent.run(text)
                value = result.output.result

                # Convert enum to its string value
                if isinstance(value, Enum):
                    value = value.value

                return {"success": True, "result": value}

            except Exception as e:
                last_error = e

                if attempt < self.max_retries and self._is_retryable(e):
                    # Use server-suggested delay if available, otherwise exponential backoff
                    delay = self._parse_retry_delay(e)
                    if delay is None:
                        delay = min(2 ** attempt, 120)  # 2s, 4s, 8s, 16s... cap at 120s

                    logger.warning(
                        f"LLM extraction retry {attempt}/{self.max_retries} "
                        f"(waiting {delay:.1f}s): {str(e)[:120]}..."
                    )
                    await asyncio.sleep(delay)
                    continue

                # Non-retryable error or last attempt
                logger.error(
                    f"LLM extraction failed (attempt {attempt}/{self.max_retries}) "
                    f"for text (first 100 chars: {text[:100]}...): {e}"
                )
                return {"success": False, "result": None, "error": str(e)}

        # Should not reach here, but just in case
        return {"success": False, "result": None, "error": str(last_error)}

    async def extract_batch(
        self,
        texts: list[str],
        prompt: str,
        output_type: str,
        enum_values: Optional[list[str]] = None,
    ) -> list[dict[str, Any]]:
        """
        Extract structured data from a batch of texts with concurrency control.

        Uses asyncio.Semaphore to limit concurrent LLM calls and avoid
        API throttling.

        Returns:
            List of results in the same order as input texts.
        """
        semaphore = asyncio.Semaphore(self.max_concurrent)

        async def process_with_semaphore(text: str) -> dict[str, Any]:
            async with semaphore:
                return await self.extract_single(text, prompt, output_type, enum_values)

        tasks = [process_with_semaphore(t) for t in texts]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        processed = []
        for i, r in enumerate(results):
            if isinstance(r, Exception):
                logger.error(f"Batch extraction exception at index {i}: {r}")
                processed.append({"success": False, "result": None, "error": str(r)})
            else:
                processed.append(r)

        return processed

    async def process_extraction(
        self,
        extraction_def: dict[str, Any],
        column_values: list[Any],
    ) -> dict[str, Any]:
        """
        Process a single LLM extraction definition for all values of a column.

        Args:
            extraction_def: LLM extraction definition dict with keys:
                source_column_id, new_column_name, prompt, output_type, enum_values
            column_values: List of text values from the source column.

        Returns:
            {
                "column_name": str,
                "output_type": str,
                "values": list,           # extracted values (same order as input)
                "rows_processed": int,
                "rows_success": int,
                "rows_failed": int,
                "error_rate": float,
            }
        """
        column_name = extraction_def["new_column_name"]
        prompt = extraction_def["prompt"]
        output_type = extraction_def["output_type"]
        enum_values = extraction_def.get("enum_values")

        texts = [str(v) if v is not None else "" for v in column_values]

        logger.info(
            f"LLM extraction '{column_name}': processing {len(texts)} rows "
            f"(type={output_type}, max_concurrent={self.max_concurrent})"
        )

        results = await self.extract_batch(texts, prompt, output_type, enum_values)

        values = [r["result"] for r in results]
        rows_success = sum(1 for r in results if r["success"])
        rows_failed = len(results) - rows_success
        error_rate = rows_failed / len(results) if results else 0.0

        logger.info(
            f"LLM extraction '{column_name}': completed. "
            f"success={rows_success}, failed={rows_failed}, error_rate={error_rate:.2%}"
        )

        return {
            "column_name": column_name,
            "output_type": output_type,
            "values": values,
            "rows_processed": len(results),
            "rows_success": rows_success,
            "rows_failed": rows_failed,
            "error_rate": error_rate,
        }


# Singleton-like accessor
_service_instance: Optional[LLMExtractionService] = None


def get_llm_extraction_service() -> LLMExtractionService:
    """Get or create the LLM extraction service singleton."""
    global _service_instance
    if _service_instance is None:
        _service_instance = LLMExtractionService()
    return _service_instance
