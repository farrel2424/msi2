"""
Sumopod AI Client
OpenAI-compatible API client for Sumopod AI Gateway
"""

from openai import OpenAI
from typing import Dict, List, Optional
import logging
import json


class SumopodClient:
    """Client for Sumopod AI Gateway"""
    
    # System prompt for PDF data extraction
    EXTRACTION_SYSTEM_PROMPT = """You are a data extraction expert for Electronic Product Catalogs (EPC). Extract structured information from PDF markdown text.

**CRITICAL: This PDF format uses part codes and sequential names, NOT "English / Chinese" format!**

PDF Structure Pattern:
```
10          Frame System 车架系统                                    1
D C97259880020   Front Accessories 中保险杠...                        ...4
D C95259510002   Transmission Auxiliary Crossbeam 变速器辅助横梁...     ...6
```

Rules:
1. **Section headers** (number + bold text) = Category
   - Format: `<number> <English Name> <Chinese Name>`
   - Example: `10 Frame System 车架系统` → Category: "Frame System" / "车架系统"

2. **Part entries** (code + names) = Type Category (subcategory)
   - Format: `<PartCode> <EnglishName> <ChineseName>...`
   - Example: `D C97259880020 Front Accessories 中保险杠...`
   - Extract: code="D C97259880020", name_en="Front Accessories", name_cn="中保险杠"

3. **Pattern recognition:**
   - Part codes start with letters/numbers (e.g., "D C95259510002")
   - English names come after the code
   - Chinese names follow English names (look for Chinese characters)
   - Ignore page numbers and dots

4. Return ONLY valid JSON, no markdown formatting, no explanations

Output JSON schema:
{
  "categories": [
    {
      "category_name_en": "string (from section header, English part)",
      "category_name_cn": "string (from section header, Chinese part)",
      "subcategories": [
        {
          "subcategory_code": "string (part code, e.g., D C97259880020)",
          "subcategory_name_en": "string (English name after code)",
          "subcategory_name_cn": "string (Chinese name, look for Chinese characters)"
        }
      ]
    }
  ]
}

Example Input:
```
10          Frame System 车架系统                                    1
D C97259880020   Front Accessories 中保险杠...                        ...4
D C95259510002   Transmission Auxiliary Crossbeam 变速器辅助横梁...     ...6

11          Dynamic System 动力系统                                  8
D C62119011339   Engine Assembly 发动机总成...                        ...11
```

Example Output:
{
  "categories": [
    {
      "category_name_en": "Frame System",
      "category_name_cn": "车架系统",
      "subcategories": [
        {
          "subcategory_code": "D C97259880020",
          "subcategory_name_en": "Front Accessories",
          "subcategory_name_cn": "中保险杠"
        },
        {
          "subcategory_code": "D C95259510002",
          "subcategory_name_en": "Transmission Auxiliary Crossbeam",
          "subcategory_name_cn": "变速器辅助横梁"
        }
      ]
    },
    {
      "category_name_en": "Dynamic System",
      "category_name_cn": "动力系统",
      "subcategories": [
        {
          "subcategory_code": "D C62119011339",
          "subcategory_name_en": "Engine Assembly",
          "subcategory_name_cn": "发动机总成"
        }
      ]
    }
  ]
}"""
    
    def __init__(
        self, 
        base_url: str = "https://ai.sumopod.com/v1",
        api_key: Optional[str] = None,
        model: str = "gpt4o",
        temperature: float = 0.7,
        max_tokens: int = 2000,
        max_retries: int = 3
    ):
        """
        Initialize Sumopod client
        
        Args:
            base_url: Sumopod API base URL
            api_key: API key (sk-xxxx format)
            model: Model to use (gpt4o, gpt4.1nano)
            temperature: Temperature for generation (0.0-1.0)
            max_tokens: Maximum tokens in response
            max_retries: Maximum retry attempts for self-correction
        """
        self.base_url = base_url.rstrip('/')
        self.model = model
        self.temperature = temperature
        self.max_tokens = max_tokens
        self.max_retries = max_retries
        self.logger = logging.getLogger(__name__)
        
        # Initialize OpenAI client pointing to Sumopod
        self.client = OpenAI(
            base_url=base_url,
            api_key=api_key
        )
        
        self.logger.info(f"Initialized Sumopod client with model: {model}")
    
    def extract_catalog_data(self, markdown_text: str, attempt: int = 1) -> Dict:
        """
        Extract structured catalog data from PDF markdown using Sumopod
        
        Args:
            markdown_text: PDF content converted to markdown
            attempt: Current attempt number (for retry logic)
        
        Returns:
            Validated JSON data structure
        """
        self.logger.info(f"Starting LLM extraction via Sumopod (attempt {attempt}/{self.max_retries})")
        
        try:
            # Prepare user prompt
            user_prompt = f"Extract structured EPC catalog data from this PDF markdown:\n\n{markdown_text}"
            
            # Call Sumopod (OpenAI-compatible API)
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": self.EXTRACTION_SYSTEM_PROMPT},
                    {"role": "user", "content": user_prompt}
                ],
                temperature=self.temperature,
                max_tokens=self.max_tokens
            )
            
            # Extract response text
            response_text = response.choices[0].message.content.strip()
            self.logger.debug(f"Sumopod response: {response_text[:200]}...")
            
            # Parse JSON
            extracted_data = self._parse_json_response(response_text)
            
            # Validate data
            validation_result = self._validate_extracted_data(extracted_data)
            
            if validation_result['valid']:
                self.logger.info("Data extraction successful and validated")
                return extracted_data
            else:
                # Self-correction: Re-prompt with error details
                if attempt < self.max_retries:
                    self.logger.warning(f"Validation failed: {validation_result['errors']}")
                    return self._retry_with_correction(
                        markdown_text, 
                        validation_result['errors'], 
                        attempt + 1
                    )
                else:
                    raise ValueError(f"Max retries reached. Validation errors: {validation_result['errors']}")
        
        except json.JSONDecodeError as e:
            if attempt < self.max_retries:
                self.logger.warning(f"JSON parsing failed: {e}")
                return self._retry_with_correction(
                    markdown_text,
                    [f"JSON parsing error: {str(e)}"],
                    attempt + 1
                )
            else:
                raise ValueError(f"Max retries reached. Could not parse valid JSON: {e}")
        
        except Exception as e:
            self.logger.error(f"Extraction error: {e}")
            raise
    
    def _parse_json_response(self, response_text: str) -> Dict:
        """Parse JSON from LLM response, handling markdown code blocks"""
        # Remove markdown code blocks if present
        if response_text.startswith("```"):
            lines = response_text.split('\n')
            json_lines = []
            in_code_block = False
            
            for line in lines:
                if line.startswith("```"):
                    in_code_block = not in_code_block
                    continue
                if in_code_block or (not line.startswith("```") and json_lines):
                    json_lines.append(line)
            
            response_text = '\n'.join(json_lines).strip()
        
        return json.loads(response_text)
    
    def _validate_extracted_data(self, data: Dict) -> Dict[str, any]:
        """
        Validate extracted EPC catalog data structure
        
        Returns:
            Dict with 'valid' boolean and 'errors' list
        """
        errors = []
        
        # Check top-level structure
        if not isinstance(data, dict):
            errors.append("Root must be a dictionary")
            return {'valid': False, 'errors': errors}
        
        if 'categories' not in data:
            errors.append("Missing 'categories' field")
            return {'valid': False, 'errors': errors}
        
        if not isinstance(data['categories'], list):
            errors.append("'categories' must be a list")
            return {'valid': False, 'errors': errors}
        
        if len(data['categories']) == 0:
            errors.append("'categories' list is empty")
            return {'valid': False, 'errors': errors}
        
        # Validate each category
        for idx, category in enumerate(data['categories']):
            if not isinstance(category, dict):
                errors.append(f"Category {idx} is not a dictionary")
                continue
            
            # Check required fields
            if 'category_name_en' not in category:
                errors.append(f"Category {idx} missing 'category_name_en'")
            
            if 'subcategories' not in category:
                errors.append(f"Category {idx} missing 'subcategories'")
            elif not isinstance(category['subcategories'], list):
                errors.append(f"Category {idx} 'subcategories' must be a list")
            else:
                # Validate subcategories (Type Categories)
                for sub_idx, subcategory in enumerate(category['subcategories']):
                    if not isinstance(subcategory, dict):
                        errors.append(f"Category {idx}, subcategory {sub_idx} is not a dictionary")
                        continue
                    
                    # Required: at least English name
                    if 'subcategory_name_en' not in subcategory:
                        errors.append(f"Category {idx}, subcategory {sub_idx} missing 'subcategory_name_en'")
        
        return {
            'valid': len(errors) == 0,
            'errors': errors
        }
    
    def _retry_with_correction(self, markdown_text: str, errors: List[str], attempt: int) -> Dict:
        """Retry extraction with error feedback for self-correction"""
        self.logger.info(f"Retrying extraction with error feedback via Sumopod (attempt {attempt})")
        
        error_message = "\n".join(f"- {error}" for error in errors)
        corrective_prompt = f"""The previous extraction had these errors:
{error_message}

Please extract the data again, ensuring:
1. Valid JSON format (no markdown code blocks)
2. All required fields are present
3. Correct data types (lists, dictionaries, strings)
4. Category format: section headers with numbers and bold text
5. Subcategory format: part codes followed by names

Original markdown text:
{markdown_text}"""
        
        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": self.EXTRACTION_SYSTEM_PROMPT},
                    {"role": "user", "content": corrective_prompt}
                ],
                temperature=self.temperature,
                max_tokens=self.max_tokens
            )
            
            response_text = response.choices[0].message.content.strip()
            extracted_data = self._parse_json_response(response_text)
            
            # Validate again
            validation_result = self._validate_extracted_data(extracted_data)
            
            if validation_result['valid']:
                self.logger.info("Self-correction successful via Sumopod")
                return extracted_data
            else:
                if attempt < self.max_retries:
                    return self._retry_with_correction(
                        markdown_text,
                        validation_result['errors'],
                        attempt + 1
                    )
                else:
                    raise ValueError(f"Max retries reached. Final errors: {validation_result['errors']}")
        
        except Exception as e:
            self.logger.error(f"Retry failed: {e}")
            raise