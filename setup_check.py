#!/usr/bin/env python3
"""
Setup and validation script for PDF Data Extractor
Checks dependencies, configuration, and environment setup
"""

import sys
import os
from pathlib import Path


def check_python_version():
    """Check Python version"""
    print("Checking Python version...")
    version = sys.version_info
    
    if version.major < 3 or (version.major == 3 and version.minor < 8):
        print(f"  ✗ Python {version.major}.{version.minor} detected")
        print(f"  ✓ Python 3.8+ required")
        return False
    
    print(f"  ✓ Python {version.major}.{version.minor}.{version.micro}")
    return True


def check_dependencies():
    """Check if required packages are installed"""
    print("\nChecking dependencies...")
    
    required_packages = {
        'pymupdf4llm': 'pymupdf4llm',
        'openai': 'openai',
        'requests': 'requests',
    }
    
    missing = []
    for package_name, import_name in required_packages.items():
        try:
            __import__(import_name)
            print(f"  ✓ {package_name}")
        except ImportError:
            print(f"  ✗ {package_name} (missing)")
            missing.append(package_name)
    
    if missing:
        print(f"\n  Install missing packages:")
        print(f"  pip install {' '.join(missing)}")
        return False
    
    return True


def check_environment_variables():
    """Check if required environment variables are set"""
    print("\nChecking environment variables...")
    
    required_vars = {
        'OPENAI_API_KEY': 'OpenAI API key',
        'API_BEARER_TOKEN': 'Internal API bearer token'
    }
    
    missing = []
    for var_name, description in required_vars.items():
        value = os.getenv(var_name)
        if value:
            masked_value = value[:8] + '...' if len(value) > 8 else '***'
            print(f"  ✓ {var_name} ({masked_value})")
        else:
            print(f"  ✗ {var_name} (not set)")
            missing.append((var_name, description))
    
    if missing:
        print(f"\n  Set missing variables:")
        for var_name, description in missing:
            print(f"  export {var_name}='your-{description}-here'")
        print(f"\n  Or create a .env file (see .env.example)")
        return False
    
    return True


def check_file_structure():
    """Check if required files exist"""
    print("\nChecking file structure...")
    
    required_files = [
        'pdf_extractor.py',
        'requirements.txt',
        'README.md',
        '.env.example'
    ]
    
    missing = []
    for filename in required_files:
        if Path(filename).exists():
            print(f"  ✓ {filename}")
        else:
            print(f"  ✗ {filename} (missing)")
            missing.append(filename)
    
    return len(missing) == 0


def create_sample_directories():
    """Create sample directories for PDFs"""
    print("\nCreating sample directories...")
    
    directories = ['pdfs', 'outputs']
    
    for dir_name in directories:
        dir_path = Path(dir_name)
        if not dir_path.exists():
            dir_path.mkdir(parents=True)
            print(f"  ✓ Created: {dir_name}/")
        else:
            print(f"  ✓ Exists: {dir_name}/")


def test_imports():
    """Test importing the main module"""
    print("\nTesting module imports...")
    
    try:
        from pdf_extractor import PDFExtractorConfig, PDFDataExtractor
        print("  ✓ pdf_extractor module")
        return True
    except ImportError as e:
        print(f"  ✗ Import error: {e}")
        return False


def run_validation():
    """Run all validation checks"""
    print("=" * 60)
    print("PDF Data Extractor - Setup Validation")
    print("=" * 60)
    
    checks = [
        ("Python Version", check_python_version),
        ("Dependencies", check_dependencies),
        ("Environment Variables", check_environment_variables),
        ("File Structure", check_file_structure),
    ]
    
    results = []
    for check_name, check_func in checks:
        result = check_func()
        results.append((check_name, result))
    
    # Create directories (doesn't affect pass/fail)
    create_sample_directories()
    
    # Test imports if dependencies are installed
    if results[1][1]:  # If dependencies check passed
        test_result = test_imports()
        results.append(("Module Import", test_result))
    
    # Summary
    print("\n" + "=" * 60)
    print("VALIDATION SUMMARY")
    print("=" * 60)
    
    passed = sum(1 for _, result in results if result)
    total = len(results)
    
    for check_name, result in results:
        status = "✓ PASS" if result else "✗ FAIL"
        print(f"{status}: {check_name}")
    
    print(f"\nResult: {passed}/{total} checks passed")
    
    if passed == total:
        print("\n✓ Setup complete! You're ready to process PDFs.")
        print("\nQuick start:")
        print("  1. Place PDF files in the pdfs/ directory")
        print("  2. Run: python pdf_extractor.py")
        print("  3. Check pdf_extractor.log for details")
        return True
    else:
        print("\n✗ Setup incomplete. Please fix the issues above.")
        return False


if __name__ == "__main__":
    success = run_validation()
    sys.exit(0 if success else 1)