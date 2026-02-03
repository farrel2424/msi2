"""
Motorsights EPC API Client
Handles all interactions with the dev-epc.motorsights.com API
"""

import requests
from typing import Dict, List, Optional, Tuple
import logging
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


class MotorsightsEPCClient:
    """Client for Motorsights Electronic Product Catalog API"""
    
    def __init__(self, base_url: str, bearer_token: str, max_retries: int = 3):
        """
        Initialize EPC API client
        
        Args:
            base_url: Base URL for EPC API (e.g., https://dev-epc.motorsights.com)
            bearer_token: Bearer token for authentication
            max_retries: Maximum number of retry attempts
        """
        self.base_url = base_url.rstrip('/')
        self.bearer_token = bearer_token
        self.logger = logging.getLogger(__name__)
        self.session = self._create_session(max_retries)
    
    def _create_session(self, max_retries: int) -> requests.Session:
        """Create requests session with retry configuration"""
        session = requests.Session()
        
        retry_strategy = Retry(
            total=max_retries,
            backoff_factor=2.0,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "POST", "PUT", "DELETE"]
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        return session
    
    def _get_headers(self) -> Dict[str, str]:
        """Get request headers with authentication"""
        return {
            "Authorization": f"Bearer {self.bearer_token}",
            "Content-Type": "application/json"
        }
    
    # ===== MASTER CATEGORY ENDPOINTS =====
    
    def get_master_categories(self, filters: Optional[Dict] = None) -> Tuple[bool, Optional[Dict]]:
        """
        Get all master categories with pagination and filters
        
        Args:
            filters: Optional filters (e.g., {"page": 1, "limit": 10})
        
        Returns:
            Tuple of (success, response_data)
        """
        url = f"{self.base_url}/master_category/get"
        
        try:
            response = self.session.post(
                url,
                json=filters or {},
                headers=self._get_headers(),
                timeout=30
            )
            response.raise_for_status()
            return True, response.json()
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Failed to get master categories: {e}")
            return False, None
    
    def create_master_category(self, data: Dict) -> Tuple[bool, Optional[Dict]]:
        """
        Create new master category
        
        Args:
            data: Master category data (e.g., {"name": "Electronics", "name_zh": "电子产品"})
        
        Returns:
            Tuple of (success, response_data)
        """
        url = f"{self.base_url}/master_category/create"
        
        try:
            response = self.session.post(
                url,
                json=data,
                headers=self._get_headers(),
                timeout=30
            )
            response.raise_for_status()
            return True, response.json()
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Failed to create master category: {e}")
            return False, None
    
    def get_master_category_by_id(self, category_id: int) -> Tuple[bool, Optional[Dict]]:
        """Get master category by ID"""
        url = f"{self.base_url}/master_category/{category_id}"
        
        try:
            response = self.session.get(
                url,
                headers=self._get_headers(),
                timeout=30
            )
            response.raise_for_status()
            return True, response.json()
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Failed to get master category {category_id}: {e}")
            return False, None
    
    # ===== TYPE CATEGORY ENDPOINTS =====
    
    def get_type_categories(self, filters: Optional[Dict] = None) -> Tuple[bool, Optional[Dict]]:
        """Get all type categories with pagination and filters"""
        url = f"{self.base_url}/type_category/get"
        
        try:
            response = self.session.post(
                url,
                json=filters or {},
                headers=self._get_headers(),
                timeout=30
            )
            response.raise_for_status()
            return True, response.json()
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Failed to get type categories: {e}")
            return False, None
    
    def create_type_category(self, data: Dict) -> Tuple[bool, Optional[Dict]]:
        """
        Create new type category
        
        Args:
            data: Type category data
                Example: {
                    "category_id": "uuid-string",  # Master category UUID
                    "type_category_name_en": "Electronics",
                    "type_category_name_cn": "电子产品",
                    "type_category_description": "Description (optional)"
                }
        
        Returns:
            Tuple of (success, response_data)
            Success response format:
            {
                "success": true,
                "data": {
                    "type_category_id": "uuid",
                    "type_category_name_en": "...",
                    "type_category_name_cn": "...",
                    ...
                }
            }
        """
        url = f"{self.base_url}/type_category/create"
        
        try:
            response = self.session.post(
                url,
                json=data,
                headers=self._get_headers(),
                timeout=30
            )
            response.raise_for_status()
            result = response.json()
            
            # Check if API returned success
            if not result.get('success', False):
                error_msg = result.get('error', 'Unknown error')
                self.logger.error(f"API returned error: {error_msg}")
                return False, result
            
            self.logger.info(f"Created type category: {result.get('data', {}).get('type_category_name_en')}")
            return True, result
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Failed to create type category: {e}")
            if hasattr(e, 'response') and e.response is not None:
                self.logger.error(f"Response: {e.response.text}")
            return False, None
    
    def get_type_category_by_id(self, type_category_id: int) -> Tuple[bool, Optional[Dict]]:
        """Get type category by ID"""
        url = f"{self.base_url}/type_category/{type_category_id}"
        
        try:
            response = self.session.get(
                url,
                headers=self._get_headers(),
                timeout=30
            )
            response.raise_for_status()
            return True, response.json()
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Failed to get type category {type_category_id}: {e}")
            return False, None
    
    # ===== CATEGORIES ENDPOINTS =====
    
    def get_categories(self, filters: Optional[Dict] = None) -> Tuple[bool, Optional[Dict]]:
        """Get all categories with pagination and filters"""
        url = f"{self.base_url}/categories/get"
        
        try:
            response = self.session.post(
                url,
                json=filters or {},
                headers=self._get_headers(),
                timeout=30
            )
            response.raise_for_status()
            return True, response.json()
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Failed to get categories: {e}")
            return False, None
    
    def create_category(self, data: Dict) -> Tuple[bool, Optional[Dict]]:
        """
        Create new category with type categories
        
        Args:
            data: Category data
                Example: {
                    "master_category_id": "uuid-string",
                    "master_category_name_en": "Electronics",
                    "category_name_cn": "电子产品",
                    "category_description": "Description (optional)",
                    "categories_code": "1234567890",
                    "data_type": [
                        {
                            "type_category_code": "1234567890",
                            "type_category_name_en": "Mobile Phones",
                            "type_category_name_cn": "手机",
                            "type_category_description": "Description (optional)"
                        }
                    ]
                }
        
        Returns:
            Tuple of (success, response_data)
            Success response format:
            {
                "success": true,
                "data": {
                    "category_id": "uuid",
                    "category_name_en": "...",
                    "data_type": [...]
                }
            }
        """
        url = f"{self.base_url}/categories/create"
        
        try:
            response = self.session.post(
                url,
                json=data,
                headers=self._get_headers(),
                timeout=30
            )
            response.raise_for_status()
            result = response.json()
            
            # Check if API returned success
            if not result.get('success', False):
                error_msg = result.get('error', 'Unknown error')
                self.logger.error(f"API returned error: {error_msg}")
                return False, result
            
            self.logger.info(f"Created category: {result.get('data', {}).get('category_name_en')}")
            return True, result
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Failed to create category: {e}")
            if hasattr(e, 'response') and e.response is not None:
                self.logger.error(f"Response: {e.response.text}")
            return False, None
    
    def get_category_by_id(self, category_id: int) -> Tuple[bool, Optional[Dict]]:
        """Get category by ID"""
        url = f"{self.base_url}/categories/{category_id}"
        
        try:
            response = self.session.get(
                url,
                headers=self._get_headers(),
                timeout=30
            )
            response.raise_for_status()
            return True, response.json()
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Failed to get category {category_id}: {e}")
            return False, None
    
    # ===== PRODUCTS ENDPOINTS =====
    
    def create_product(self, data: Dict) -> Tuple[bool, Optional[Dict]]:
        """
        Create new product
        
        Args:
            data: Product data
        
        Returns:
            Tuple of (success, response_data)
        """
        url = f"{self.base_url}/products/create"
        
        try:
            response = self.session.post(
                url,
                json=data,
                headers=self._get_headers(),
                timeout=30
            )
            response.raise_for_status()
            return True, response.json()
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Failed to create product: {e}")
            return False, None
    
    # ===== BATCH OPERATIONS =====
    
    def batch_create_type_categories_and_categories(
        self, 
        catalog_data: Dict,
        master_category_id: str  # Now required as UUID string
    ) -> Tuple[bool, Dict]:
        """
        Batch create categories with nested type categories from extracted PDF data
        
        IMPORTANT: The actual API structure is:
        - Categories contain Type Categories (nested relationship)
        - NOT Type Categories contain Categories as originally assumed
        
        Args:
            catalog_data: Extracted data from PDF
                Format: {
                    "categories": [
                        {
                            "category_name_en": "Electronics",
                            "category_name_zh": "电子产品",
                            "subcategories": [
                                {
                                    "subcategory_name_en": "Mobile Phones",
                                    "subcategory_name_zh": "手机"
                                }
                            ]
                        }
                    ]
                }
            master_category_id: Required master category UUID (e.g., "123e4567-...")
        
        Returns:
            Tuple of (success, results_dict)
        """
        import uuid
        
        if not master_category_id:
            raise ValueError("master_category_id is required and must be a valid UUID")
        
        results = {
            'categories_created': [],
            'type_categories_created': [],
            'errors': []
        }
        
        for pdf_category in catalog_data.get('categories', []):
            # Each PDF category becomes a Category in EPC
            # Its subcategories become Type Categories nested within
            
            # Generate UUID for this category
            category_id = str(uuid.uuid4())
            
            # Build data_type array (Type Categories)
            data_type = []
            for subcategory in pdf_category.get('subcategories', []):
                type_cat_data = {
                    "type_category_name_en": subcategory['subcategory_name_en']
                }
                
                # Use extracted code if present, otherwise generate
                if subcategory.get('subcategory_code'):
                    type_cat_data['type_category_code'] = subcategory['subcategory_code']
                else:
                    type_cat_data['type_category_code'] = str(uuid.uuid4())[:10]
                
                # Add Chinese name if present
                if subcategory.get('subcategory_name_zh'):
                    type_cat_data['type_category_name_cn'] = subcategory['subcategory_name_zh']
                
                # Optional description
                type_cat_data['type_category_description'] = f"Type category for {subcategory['subcategory_name_en']}"
                
                data_type.append(type_cat_data)
            
            # Build category creation request
            category_request = {
                "master_category_id": master_category_id,
                "master_category_name_en": pdf_category['category_name_en']
            }
            
            # Add Chinese name if present
            if pdf_category.get('category_name_zh'):
                category_request['category_name_cn'] = pdf_category['category_name_zh']
            
            # Add optional fields
            category_request['category_description'] = f"Category for {pdf_category['category_name_en']}"
            category_request['categories_code'] = str(uuid.uuid4())[:10]  # Generate code
            category_request['data_type'] = data_type
            
            # Create the category with nested type categories
            success, cat_response = self.create_category(category_request)
            
            if success:
                results['categories_created'].append(cat_response.get('data', {}))
                
                # Count type categories
                nested_types = cat_response.get('data', {}).get('data_type', [])
                results['type_categories_created'].extend(nested_types)
                
                self.logger.info(
                    f"Created category '{pdf_category['category_name_en']}' "
                    f"with {len(nested_types)} type categories"
                )
            else:
                error_detail = cat_response.get('error') if cat_response else 'Unknown error'
                results['errors'].append({
                    'type': 'category',
                    'data': category_request,
                    'error': error_detail
                })
                self.logger.error(f"Failed to create category: {error_detail}")
        
        overall_success = len(results['errors']) == 0
        
        self.logger.info(
            f"Batch operation complete: "
            f"{len(results['categories_created'])} categories created, "
            f"{len(results['type_categories_created'])} type categories created, "
            f"{len(results['errors'])} errors"
        )
        
        return overall_success, results