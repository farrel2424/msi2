"""
Motorsights EPC API Client
Handles all interactions with the dev-epc.motorsights.com API
Updated to support dynamic bearer token generation via SSO

CORRECTED based on actual network inspection - simplified format without codes
"""

import requests
from typing import Dict, List, Optional, Tuple
import logging
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from motorsights_auth_client import MotorsightsAuthClient


class MotorsightsEPCClient:
    """Client for Motorsights Electronic Product Catalog API"""
    
    def __init__(
        self, 
        base_url: str, 
        bearer_token: Optional[str] = None,
        auth_client: Optional[MotorsightsAuthClient] = None,
        max_retries: int = 3
    ):
        """
        Initialize EPC API client
        
        Args:
            base_url: Base URL for EPC API (e.g., https://dev-gateway.motorsights.com/api/epc/)
            bearer_token: Static bearer token (deprecated - use auth_client instead)
            auth_client: MotorsightsAuthClient for dynamic token generation
            max_retries: Maximum number of retry attempts
        
        Note: Either bearer_token OR auth_client must be provided.
              auth_client is recommended for automatic token refresh.
        """
        self.base_url = base_url.rstrip('/')
        self.bearer_token = bearer_token  # Static token (deprecated)
        self.auth_client = auth_client    # Dynamic token client (recommended)
        self.logger = logging.getLogger(__name__)
        self.session = self._create_session(max_retries)
        
        if not bearer_token and not auth_client:
            raise ValueError("Either bearer_token or auth_client must be provided")
    
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
    
    def _get_bearer_token(self) -> str:
        """
        Get bearer token (either static or dynamic)
        
        Returns:
            Bearer token string
        """
        if self.auth_client:
            # Use dynamic token from auth client (auto-refreshes if expired)
            return self.auth_client.get_bearer_token()
        else:
            # Use static token
            return self.bearer_token
    
    def _get_headers(self) -> Dict[str, str]:
        """Get request headers with authentication"""
        bearer_token = self._get_bearer_token()
        return {
            "Authorization": f"Bearer {bearer_token}",
            "Content-Type": "application/json"
        }
    
    def _handle_401_retry(self, func, *args, **kwargs):
        """
        Handle 401 errors by refreshing token and retrying once
        
        Args:
            func: Function to call
            *args, **kwargs: Arguments to pass to function
        
        Returns:
            Function result
        """
        try:
            return func(*args, **kwargs)
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 401 and self.auth_client:
                # Token might be expired, refresh and retry once
                self.logger.warning("Got 401, refreshing bearer token and retrying...")
                self.auth_client.invalidate_token()
                return func(*args, **kwargs)
            else:
                raise
    
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
        
        def _request():
            response = self.session.post(
                url,
                json=filters or {},
                headers=self._get_headers(),
                timeout=30
            )
            response.raise_for_status()
            return True, response.json()
        
        try:
            return self._handle_401_retry(_request)
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
        
        def _request():
            response = self.session.post(
                url,
                json=data,
                headers=self._get_headers(),
                timeout=30
            )
            response.raise_for_status()
            return True, response.json()
        
        try:
            return self._handle_401_retry(_request)
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Failed to create master category: {e}")
            return False, None
    
    def get_master_category_by_id(self, category_id: int) -> Tuple[bool, Optional[Dict]]:
        """Get master category by ID"""
        url = f"{self.base_url}/master_category/{category_id}"
        
        def _request():
            response = self.session.get(
                url,
                headers=self._get_headers(),
                timeout=30
            )
            response.raise_for_status()
            return True, response.json()
        
        try:
            return self._handle_401_retry(_request)
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Failed to get master category {category_id}: {e}")
            return False, None
    
    # ===== TYPE CATEGORY ENDPOINTS =====
    
    def get_type_categories(self, filters: Optional[Dict] = None) -> Tuple[bool, Optional[Dict]]:
        """Get all type categories with pagination and filters"""
        url = f"{self.base_url}/type_category/get"
        
        def _request():
            response = self.session.post(
                url,
                json=filters or {},
                headers=self._get_headers(),
                timeout=30
            )
            response.raise_for_status()
            return True, response.json()
        
        try:
            return self._handle_401_retry(_request)
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
        """
        url = f"{self.base_url}/type_category/create"
        
        def _request():
            response = self.session.post(
                url,
                json=data,
                headers=self._get_headers(),
                timeout=30
            )
            response.raise_for_status()
            result = response.json()
            
            if not result.get('success', False):
                error_msg = result.get('error', 'Unknown error')
                self.logger.error(f"API returned error: {error_msg}")
                return False, result
            
            self.logger.info(f"Created type category: {result.get('data', {}).get('type_category_name_en')}")
            return True, result
        
        try:
            return self._handle_401_retry(_request)
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Failed to create type category: {e}")
            if hasattr(e, 'response') and e.response is not None:
                self.logger.error(f"Response: {e.response.text}")
            return False, None
    
    def get_type_category_by_id(self, type_category_id: int) -> Tuple[bool, Optional[Dict]]:
        """Get type category by ID"""
        url = f"{self.base_url}/type_category/{type_category_id}"
        
        def _request():
            response = self.session.get(
                url,
                headers=self._get_headers(),
                timeout=30
            )
            response.raise_for_status()
            return True, response.json()
        
        try:
            return self._handle_401_retry(_request)
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Failed to get type category {type_category_id}: {e}")
            return False, None
    
    # ===== CATEGORIES ENDPOINTS =====
    
    def get_categories(self, filters: Optional[Dict] = None) -> Tuple[bool, Optional[Dict]]:
        """Get all categories with pagination and filters"""
        url = f"{self.base_url}/categories/get"
        
        def _request():
            response = self.session.post(
                url,
                json=filters or {},
                headers=self._get_headers(),
                timeout=30
            )
            response.raise_for_status()
            return True, response.json()
        
        try:
            return self._handle_401_retry(_request)
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Failed to get categories: {e}")
            return False, None
    
    def create_category(self, data: Dict) -> Tuple[bool, Optional[Dict]]:
        """
        Create new category with type categories
        
        ACTUAL WORKING FORMAT (from network inspection):
        {
            "master_category_id": "ee6eb407-fd6e-4c3d-b696-e56feed4fdf6",
            "master_category_name_en": "Engine",
            "category_name_en": "gem",
            "category_name_cn": "em",
            "category_description": "em",
            "data_type": [
                {
                    "type_category_name_en": "em",
                    "type_category_name_cn": "em",
                    "type_category_description": "em"
                }
            ]
        }
        
        Args:
            data: Category data (must follow format above)
        
        Returns:
            Tuple of (success, response_data)
        """
        url = f"{self.base_url}/categories/create"
        
        def _request():
            response = self.session.post(
                url,
                json=data,
                headers=self._get_headers(),
                timeout=30
            )
            response.raise_for_status()
            result = response.json()
            
            if not result.get('success', False):
                error_msg = result.get('error', 'Unknown error')
                self.logger.error(f"API returned error: {error_msg}")
                return False, result
            
            self.logger.info(f"Created category: {result.get('data', {}).get('category_name_en', result.get('data', {}).get('master_category_name_en'))}")
            return True, result
        
        try:
            return self._handle_401_retry(_request)
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Failed to create category: {e}")
            if hasattr(e, 'response') and e.response is not None:
                self.logger.error(f"Response: {e.response.text}")
            return False, None
    
    def get_category_by_id(self, category_id: int) -> Tuple[bool, Optional[Dict]]:
        """Get category by ID"""
        url = f"{self.base_url}/categories/{category_id}"
        
        def _request():
            response = self.session.get(
                url,
                headers=self._get_headers(),
                timeout=30
            )
            response.raise_for_status()
            return True, response.json()
        
        try:
            return self._handle_401_retry(_request)
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
        
        def _request():
            response = self.session.post(
                url,
                json=data,
                headers=self._get_headers(),
                timeout=30
            )
            response.raise_for_status()
            return True, response.json()
        
        try:
            return self._handle_401_retry(_request)
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Failed to create product: {e}")
            return False, None
    
    # ===== BATCH OPERATIONS =====
    
    def batch_create_type_categories_and_categories(
        self, 
        catalog_data: Dict,
        master_category_id: str  # Required as UUID string
    ) -> Tuple[bool, Dict]:
        """
        Batch create categories with nested type categories from extracted PDF data
        
        ACTUAL WORKING FORMAT (discovered via network inspection):
        {
            "master_category_id": "uuid-string",
            "master_category_name_en": "Engine",
            "category_name_en": "gem",
            "category_name_cn": "em",
            "category_description": "em",
            "data_type": [
                {
                    "type_category_name_en": "em",
                    "type_category_name_cn": "em",
                    "type_category_description": "em"
                }
            ]
        }
        
        Args:
            catalog_data: Extracted data from PDF
                Format: {
                    "categories": [
                        {
                            "category_name_en": "Electronics",
                            "category_name_cn": "电子产品",
                            "data_type": [
                                {
                                    "type_category_name_en": "Mobile Phones",
                                    "type_category_name_cn": "手机"
                                }
                            ]
                        }
                    ]
                }
            master_category_id: Required master category UUID (e.g., "123e4567-...")
        
        Returns:
            Tuple of (success, results_dict)
        """
        if not master_category_id:
            raise ValueError("master_category_id is required and must be a valid UUID")
        
        results = {
            'categories_created': [],
            'type_categories_created': [],
            'errors': []
        }
        
        for pdf_category in catalog_data.get('categories', []):
            # Build data_type array (Type Categories) - SIMPLIFIED FORMAT
            data_type = []
            for type_cat in pdf_category.get('data_type', []):
                # ACTUAL WORKING FORMAT - no type_category_code needed!
                type_cat_data = {
                    "type_category_name_en": type_cat.get('type_category_name_en', ''),
                    "type_category_name_cn": type_cat.get('type_category_name_cn', ''),
                    "type_category_description": type_cat.get('type_category_description', '')
                }
                data_type.append(type_cat_data)
            
            # Build category request - EXACT FORMAT FROM NETWORK INSPECTION
            category_request = {
                "master_category_id": master_category_id,
                "master_category_name_en": pdf_category['category_name_en'],
                "category_name_en": pdf_category.get('category_name_en', ''),  # REQUIRED field
                "category_name_cn": pdf_category.get('category_name_cn', ''),
                "category_description": pdf_category.get('category_description', f"Category for {pdf_category['category_name_en']}"),
                "data_type": data_type
            }
            
            self.logger.debug(f"Creating category with request: {category_request}")
            
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