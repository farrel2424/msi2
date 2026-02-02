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
                    "name": "Electronics",
                    "name_zh": "电子产品",
                    "master_category_id": 1
                }
        
        Returns:
            Tuple of (success, response_data with created type_category_id)
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
            self.logger.info(f"Created type category: {result}")
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
        Create new category
        
        Args:
            data: Category data
                Example: {
                    "name": "Mobile Phones",
                    "name_zh": "手机",
                    "type_category_id": 123
                }
        
        Returns:
            Tuple of (success, response_data with created category_id)
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
            self.logger.info(f"Created category: {result}")
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
        master_category_id: Optional[int] = None
    ) -> Tuple[bool, Dict]:
        """
        Batch create type categories and their subcategories from extracted PDF data
        
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
            master_category_id: Optional master category ID to link to
        
        Returns:
            Tuple of (success, results_dict)
        """
        results = {
            'type_categories_created': [],
            'categories_created': [],
            'errors': []
        }
        
        for category in catalog_data.get('categories', []):
            # Create Type Category (Bold text from PDF)
            type_category_data = {
                "name": category['category_name_en'],
            }
            
            # Add Chinese name if present
            if category.get('category_name_zh'):
                type_category_data['name_zh'] = category['category_name_zh']
            
            # Add master category link if provided
            if master_category_id:
                type_category_data['master_category_id'] = master_category_id
            
            success, type_cat_response = self.create_type_category(type_category_data)
            
            if not success:
                results['errors'].append({
                    'type': 'type_category',
                    'data': type_category_data,
                    'error': 'Failed to create type category'
                })
                continue
            
            results['type_categories_created'].append(type_cat_response)
            
            # Get the created type_category_id
            type_category_id = type_cat_response.get('id') or type_cat_response.get('data', {}).get('id')
            
            if not type_category_id:
                self.logger.error(f"No ID returned for type category: {type_cat_response}")
                results['errors'].append({
                    'type': 'type_category_id_missing',
                    'response': type_cat_response
                })
                continue
            
            # Create Categories (Normal text from PDF) under this Type Category
            for subcategory in category.get('subcategories', []):
                category_data = {
                    "name": subcategory['subcategory_name_en'],
                    "type_category_id": type_category_id
                }
                
                # Add Chinese name if present
                if subcategory.get('subcategory_name_zh'):
                    category_data['name_zh'] = subcategory['subcategory_name_zh']
                
                success, cat_response = self.create_category(category_data)
                
                if success:
                    results['categories_created'].append(cat_response)
                else:
                    results['errors'].append({
                        'type': 'category',
                        'data': category_data,
                        'error': 'Failed to create category'
                    })
        
        overall_success = len(results['errors']) == 0
        
        self.logger.info(
            f"Batch operation complete: "
            f"{len(results['type_categories_created'])} type categories, "
            f"{len(results['categories_created'])} categories created, "
            f"{len(results['errors'])} errors"
        )
        
        return overall_success, results