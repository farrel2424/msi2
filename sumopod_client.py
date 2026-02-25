"""
Sumopod AI Client
OpenAI-compatible client for the Sumopod AI Gateway.
"""

import json
import logging
from typing import Dict, List, Optional

from openai import OpenAI


class SumopodClient:
    """Client for the Sumopod AI Gateway (OpenAI-compatible API)."""

    DEFAULT_EXTRACTION_PROMPT = """You are a data extraction expert for Electronic Product Catalogs (EPC). Extract structured information from PDF markdown text.

**CRITICAL: Extract data in the EXACT format shown below — NO codes required.**

PDF Structure Pattern:
```
10          Frame System 车架系统                                    1
D C97259880020   Front Accessories 中保险杠...                        ...4
D C95259510002   Transmission Auxiliary Crossbeam 变速器辅助横梁...     ...6
```

Rules:
1. **Section headers** (number + bold text) → Category
   - `10 Frame System 车架系统` → category_name_en="Frame System", category_name_cn="车架系统"

2. **Part entries** (after header) → Type Categories
   - Include the part code at the start of the English name.
   - `D C97259880020 Front Accessories 中保险杠` → type_category_name_en="DC97259800020 Front Accessories Of Frame"

3. Ignore page numbers and dot leaders.
4. Return ONLY valid JSON — no markdown, no explanations.

**OUTPUT FORMAT:**
{
  "categories": [
    {
      "category_name_en": "string",
      "category_name_cn": "string",
      "data_type": [
        {
          "type_category_name_en": "string",
          "type_category_name_cn": "string",
          "type_category_description": "string (may be empty)"
        }
      ]
    }
  ]
}

Do NOT include type_category_code or categories_code."""

    def __init__(
        self,
        base_url: str = "https://ai.sumopod.com/v1",
        api_key: Optional[str] = None,
        model: str = "gpt4o",
        temperature: float = 0.7,
        max_tokens: int = 2000,
        max_retries: int = 3,
        custom_system_prompt: Optional[str] = None,
    ):
        self.model = model
        self.temperature = temperature
        self.max_tokens = max_tokens
        self.max_retries = max_retries
        self.system_prompt = custom_system_prompt or self.DEFAULT_EXTRACTION_PROMPT
        self.logger = logging.getLogger(__name__)

        self.client = OpenAI(base_url=base_url, api_key=api_key, timeout=60.0)
        self.logger.info("Sumopod client initialised — model: %s", model)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def extract_catalog_data(
        self,
        markdown_text: str,
        attempt: int = 1,
        custom_prompt: Optional[str] = None,
    ) -> Dict:
        """
        Extract structured EPC catalog data from PDF markdown.

        Validates the response and retries with corrective feedback on failure.

        Args:
            markdown_text: PDF content converted to markdown.
            attempt:       Current attempt number (used internally for recursion).
            custom_prompt: Override the instance-level system prompt for this call.

        Returns:
            Validated dict with "categories" list.
        """
        self.logger.info(
            "LLM extraction via Sumopod (attempt %d/%d)", attempt, self.max_retries
        )
        system_prompt = custom_prompt or self.system_prompt
        user_prompt = f"Extract structured EPC catalog data from this PDF markdown:\n\n{markdown_text}"

        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
                temperature=self.temperature,
                max_tokens=self.max_tokens,
            )
            response_text = response.choices[0].message.content.strip()
            self.logger.debug("Sumopod response: %s...", response_text[:200])

            extracted_data = self._parse_json(response_text)
            errors = self._validate(extracted_data)

            if not errors:
                self.logger.info("Extraction successful and validated.")
                return extracted_data

            if attempt < self.max_retries:
                self.logger.warning("Validation failed: %s", errors)
                return self._retry(markdown_text, errors, attempt + 1, custom_prompt)
            raise ValueError(f"Max retries reached. Validation errors: {errors}")

        except json.JSONDecodeError as e:
            if attempt < self.max_retries:
                self.logger.warning("JSON parsing failed: %s", e)
                return self._retry(
                    markdown_text, [f"JSON parsing error: {e}"], attempt + 1, custom_prompt
                )
            raise ValueError(f"Max retries reached. Could not parse JSON: {e}") from e

        except Exception:
            self.logger.exception("Extraction error")
            raise

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _parse_json(text: str) -> Dict:
        """Strip markdown fences from an LLM response and parse JSON."""
        if text.startswith("```"):
            lines, inside = [], False
            for line in text.split("\n"):
                if line.startswith("```"):
                    inside = not inside
                    continue
                if inside or (not line.startswith("```") and lines):
                    lines.append(line)
            text = "\n".join(lines).strip()
        return json.loads(text)

    @staticmethod
    def _validate(data: Dict) -> List[str]:
        """Return a list of validation error strings, empty if the structure is valid."""
        if not isinstance(data, dict):
            return ["Root must be a dictionary."]

        categories = data.get("categories")
        if categories is None:
            return ["Missing 'categories' field."]
        if not isinstance(categories, list):
            return ["'categories' must be a list."]
        if not categories:
            return ["'categories' list is empty."]

        errors = []
        for i, cat in enumerate(categories):
            if not isinstance(cat, dict):
                errors.append(f"Category {i} is not a dict.")
                continue
            if "category_name_en" not in cat:
                errors.append(f"Category {i}: missing 'category_name_en'.")
            if "data_type" not in cat:
                errors.append(f"Category {i}: missing 'data_type'.")
            elif not isinstance(cat["data_type"], list):
                errors.append(f"Category {i}: 'data_type' must be a list.")
            else:
                for j, tc in enumerate(cat["data_type"]):
                    if not isinstance(tc, dict):
                        errors.append(f"Category {i}, data_type {j}: not a dict.")
                    elif "type_category_name_en" not in tc:
                        errors.append(
                            f"Category {i}, data_type {j}: missing 'type_category_name_en'."
                        )
        return errors

    def _retry(
        self,
        markdown_text: str,
        errors: List[str],
        attempt: int,
        custom_prompt: Optional[str],
    ) -> Dict:
        """Retry extraction with error context for self-correction."""
        self.logger.info("Retrying with error feedback (attempt %d).", attempt)
        system_prompt = custom_prompt or self.system_prompt
        error_list = "\n".join(f"- {e}" for e in errors)

        corrective_prompt = (
            f"The previous extraction had these errors:\n{error_list}\n\n"
            "Please extract the data again, ensuring:\n"
            "1. Valid JSON (no markdown fences)\n"
            "2. All required fields present\n"
            "3. Correct data types\n"
            "4. data_type items have ONLY name_en, name_cn, description — no codes.\n\n"
            f"Original markdown:\n{markdown_text}"
        )

        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": corrective_prompt},
                ],
                temperature=self.temperature,
                max_tokens=self.max_tokens,
            )
            extracted_data = self._parse_json(response.choices[0].message.content.strip())
            retry_errors = self._validate(extracted_data)

            if not retry_errors:
                self.logger.info("Self-correction successful.")
                return extracted_data
            if attempt < self.max_retries:
                return self._retry(markdown_text, retry_errors, attempt + 1, custom_prompt)
            raise ValueError(f"Max retries reached. Final errors: {retry_errors}")

        except Exception:
            self.logger.exception("Retry failed")
            raise