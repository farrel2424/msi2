"""
Sumopod AI Client
OpenAI-compatible API client for Sumopod AI Gateway
CORRECTED: Extraction prompt updated to match actual API format (no codes required)
"""

from openai import OpenAI
from typing import Dict, List, Optional
import logging
import json


class SumopodClient:
    """Client for Sumopod AI Gateway"""
    
    # CORRECTED: Default system prompt matching actual API format
    DEFAULT_EXTRACTION_PROMPT = """You are a data extraction expert for Electronic Product Catalogs (EPC). Extract structured information from PDF markdown text.

**CRITICAL: Extract data in the EXACT format shown below - NO codes required!**

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

2. **Part entries** (after header) = Type Categories (subcategories)
   - Format: `<PartCode> <EnglishName> <ChineseName>...`
   - Example: `D C97259880020 Front Accessories 中保险杠...`
   - Extract ONLY: name_en="Front Accessories", name_cn="中保险杠"
   - IGNORE the part code - we don't need it!

3. **Pattern recognition:**
   - English names come after the code
   - Chinese names follow English names (look for Chinese characters)
   - Ignore page numbers and dots

4. Return ONLY valid JSON, no markdown formatting, no explanations

**EXACT OUTPUT FORMAT (from actual working API):**
{
  "categories": [
    {
      "category_name_en": "string (from section header, English part)",
      "category_name_cn": "string (from section header, Chinese part)",
      "data_type": [
        {
          "type_category_name_en": "string (English name after code)",
          "type_category_name_cn": "string (Chinese name)",
          "type_category_description": "string (optional, can be empty)"
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

Example Output (EXACT format that works):
{
  "categories": [
    {
      "category_name_en": "Frame System",
      "category_name_cn": "车架系统",
      "data_type": [
        {
          "type_category_name_en": "Front Accessories",
          "type_category_name_cn": "中保险杠",
          "type_category_description": ""
        },
        {
          "type_category_name_en": "Transmission Auxiliary Crossbeam",
          "type_category_name_cn": "变速器辅助横梁",
          "type_category_description": ""
        }
      ]
    },
    {
      "category_name_en": "Dynamic System",
      "category_name_cn": "动力系统",
      "data_type": [
        {
          "type_category_name_en": "Engine Assembly",
          "type_category_name_cn": "发动机总成",
          "type_category_description": ""
        }
      ]
    }
  ]
}

IMPORTANT: Do NOT include type_category_code or categories_code - the API doesn't need them!"""
    
    def __init__(
        self, 
        base_url: str = "https://ai.sumopod.com/v1",
        api_key: Optional[str] = None,
        model: str = "gpt4o",
        temperature: float = 0.7,
        max_tokens: int = 2000,
        max_retries: int = 3,
        custom_system_prompt: Optional[str] = None
    ):
        """
        Initialize Sumopod client
        
        Args:
            base_url: Sumopod API base URL
            api_key: API key (sk-xxxx format)
            model: Model to use (gpt4o, gpt4.1nano, claude-3-5-sonnet, etc.)
            temperature: Temperature for generation (0.0-1.0)
            max_tokens: Maximum tokens in response
            max_retries: Maximum retry attempts for self-correction
            custom_system_prompt: Optional custom system prompt to override default
        """
        self.base_url = base_url.rstrip('/')
        self.model = model
        self.temperature = temperature
        self.max_tokens = max_tokens
        self.max_retries = max_retries
        self.system_prompt = custom_system_prompt or self.DEFAULT_EXTRACTION_PROMPT
        self.logger = logging.getLogger(__name__)
        
        # Initialize OpenAI client pointing to Sumopod
        self.client = OpenAI(
            base_url=base_url,
            api_key=api_key
        )
        
        self.logger.info(f"Initialized Sumopod client with model: {model}")
    
    def extract_catalog_data(
        self, 
        markdown_text: str, 
        attempt: int = 1,
        custom_prompt: Optional[str] = None
    ) -> Dict:
        """
        Extract structured catalog data from PDF markdown using Sumopod
        
        Args:
            markdown_text: PDF content converted to markdown
            attempt: Current attempt number (for retry logic)
            custom_prompt: Optional custom prompt to override system prompt for this call
        
        Returns:
            Validated JSON data structure
        """
        self.logger.info(f"Starting LLM extraction via Sumopod (attempt {attempt}/{self.max_retries})")
        
        # Use custom prompt if provided, otherwise use instance system prompt
        system_prompt = custom_prompt or self.system_prompt
        
        try:
            # Prepare user prompt
            user_prompt = f"Extract structured EPC catalog data from this PDF markdown:\n\n{markdown_text}"
            
            # Call Sumopod (OpenAI-compatible API)
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": system_prompt},
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
                        attempt + 1,
                        custom_prompt=custom_prompt
                    )
                else:
                    raise ValueError(f"Max retries reached. Validation errors: {validation_result['errors']}")
        
        except json.JSONDecodeError as e:
            if attempt < self.max_retries:
                self.logger.warning(f"JSON parsing failed: {e}")
                return self._retry_with_correction(
                    markdown_text,
                    [f"JSON parsing error: {str(e)}"],
                    attempt + 1,
                    custom_prompt=custom_prompt
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
            
            if 'data_type' not in category:
                errors.append(f"Category {idx} missing 'data_type'")
            elif not isinstance(category['data_type'], list):
                errors.append(f"Category {idx} 'data_type' must be a list")
            else:
                # Validate data_type items (Type Categories)
                for sub_idx, type_cat in enumerate(category['data_type']):
                    if not isinstance(type_cat, dict):
                        errors.append(f"Category {idx}, data_type {sub_idx} is not a dictionary")
                        continue
                    
                    # Required: at least English name
                    if 'type_category_name_en' not in type_cat:
                        errors.append(f"Category {idx}, data_type {sub_idx} missing 'type_category_name_en'")
        
        return {
            'valid': len(errors) == 0,
            'errors': errors
        }
    
    def _retry_with_correction(
        self, 
        markdown_text: str, 
        errors: List[str], 
        attempt: int,
        custom_prompt: Optional[str] = None
    ) -> Dict:
        """Retry extraction with error feedback for self-correction"""
        self.logger.info(f"Retrying extraction with error feedback via Sumopod (attempt {attempt})")
        
        # Use custom prompt if provided
        system_prompt = custom_prompt or self.system_prompt
        
        error_message = "\n".join(f"- {error}" for error in errors)
        corrective_prompt = f"""The previous extraction had these errors:
{error_message}

Please extract the data again, ensuring:
1. Valid JSON format (no markdown code blocks)
2. All required fields are present
3. Correct data types (lists, dictionaries, strings)
4. Category format: section headers with numbers and bold text
5. data_type format: ONLY name_en, name_cn, description (NO codes!)

IMPORTANT: Do NOT include type_category_code or categories_code!

Original markdown text:
{markdown_text}"""
        
        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": system_prompt},
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
                        attempt + 1,
                        custom_prompt=custom_prompt
                    )
                else:
                    raise ValueError(f"Max retries reached. Final errors: {validation_result['errors']}")
        
        except Exception as e:
            self.logger.error(f"Retry failed: {e}")
            raise