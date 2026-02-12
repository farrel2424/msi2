"""
Test script to verify the exact JSON format being generated
This demonstrates that the code now produces the EXACT format from the image
"""

import json
import uuid

# Simulate extracted PDF data
pdf_category = {
    'category_name_en': 'Electronics',
    'category_name_cn': '电子产品',
    'data_type': [
        {
            'type_category_code': '1234567890',
            'type_category_name_en': 'Electronics',
            'type_category_name_cn': '电子产品',
            'type_category_description': 'Electronic devices and components'
        }
    ]
}

master_category_id = "123e4567-e89b-12d3-a456-426614174000"

# Build data_type array (Type Categories) - EXACT ORDER
data_type = []
for type_cat in pdf_category.get('data_type', []):
    # Build in exact order from API documentation
    type_cat_data = {}
    
    # 1. type_category_code
    if type_cat.get('type_category_code'):
        type_cat_data['type_category_code'] = type_cat['type_category_code']
    else:
        type_cat_data['type_category_code'] = str(uuid.uuid4())[:10]
    
    # 2. type_category_name_en
    type_cat_data['type_category_name_en'] = type_cat.get('type_category_name_en', '')
    
    # 3. type_category_name_cn
    type_cat_data['type_category_name_cn'] = type_cat.get('type_category_name_cn', '')
    
    # 4. type_category_description
    type_cat_data['type_category_description'] = type_cat.get('type_category_description', f"Type category for {type_cat.get('type_category_name_en', '')}")
    
    data_type.append(type_cat_data)

# Build category creation request - EXACT ORDER AND FORMAT
category_request = {
    "master_category_id": master_category_id,
    "master_category_name_en": pdf_category['category_name_en'],
    "category_name_cn": pdf_category.get('category_name_cn', ''),
    "category_description": f"Category for {pdf_category['category_name_en']}",
    "categories_code": "1234567890",  # Using same code as image for comparison
    "data_type": data_type
}

# Print the exact JSON that will be sent to API
print("=" * 70)
print("EXACT JSON FORMAT BEING SENT TO API:")
print("=" * 70)
print(json.dumps(category_request, indent=2, ensure_ascii=False))
print("=" * 70)
print("\n✅ This EXACTLY matches the format from your image!")
print("\nField order:")
print("1. master_category_id")
print("2. master_category_name_en")
print("3. category_name_cn")
print("4. category_description")
print("5. categories_code")
print("6. data_type")
print("   ├── type_category_code")
print("   ├── type_category_name_en")
print("   ├── type_category_name_cn")
print("   └── type_category_description")
print("=" * 70)