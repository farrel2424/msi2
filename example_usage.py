"""
Example usage script for PDF Data Extractor
Run this after setting up environment variables
"""

from pdf_extractor import PDFDataExtractor, PDFExtractorConfig
from pathlib import Path
import json


def example_single_file():
    """Process a single PDF file"""
    print("=" * 60)
    print("EXAMPLE 1: Processing Single PDF")
    print("=" * 60)
    
    config = PDFExtractorConfig(
        api_endpoint="https://internal-api.example.com/upload",
        openai_model="gpt-4"
    )
    
    extractor = PDFDataExtractor(config)
    
    # Process single file
    pdf_path = Path("example.pdf")
    
    if pdf_path.exists():
        result = extractor.process_pdf(pdf_path)
        
        print("\nResult:")
        print(f"Success: {result['success']}")
        print(f"Stage: {result['stage']}")
        
        if result['success']:
            print("\nExtracted Data:")
            print(json.dumps(result['data'], indent=2))
        else:
            print(f"Error: {result['error']}")
    else:
        print(f"File not found: {pdf_path}")


def example_batch_processing():
    """Process all PDFs in a directory"""
    print("\n" + "=" * 60)
    print("EXAMPLE 2: Batch Processing")
    print("=" * 60)
    
    config = PDFExtractorConfig(
        api_endpoint="https://internal-api.example.com/upload",
        openai_model="gpt-4",
        max_retries=3
    )
    
    extractor = PDFDataExtractor(config)
    
    # Create sample directory if it doesn't exist
    pdf_dir = Path("./pdfs")
    pdf_dir.mkdir(exist_ok=True)
    
    print(f"\nProcessing directory: {pdf_dir}")
    
    results = extractor.process_directory(pdf_dir, recursive=False)
    
    # Display summary
    print("\n" + "=" * 60)
    print("PROCESSING SUMMARY")
    print("=" * 60)
    
    successful = [r for r in results if r['success']]
    failed = [r for r in results if not r['success']]
    
    print(f"Total: {len(results)}")
    print(f"Successful: {len(successful)}")
    print(f"Failed: {len(failed)}")
    
    if failed:
        print("\nFailed files:")
        for result in failed:
            print(f"  - {Path(result['filename']).name}: {result['error']}")


def example_with_custom_config():
    """Example with custom configuration"""
    print("\n" + "=" * 60)
    print("EXAMPLE 3: Custom Configuration")
    print("=" * 60)
    
    config = PDFExtractorConfig(
        api_endpoint="https://your-api.example.com/v2/upload",
        openai_model="gpt-4-turbo-preview",  # Use different model
        max_retries=5,  # More retries
        retry_backoff_factor=1.5,  # Slower backoff
        processed_log_file="custom_processed.json"  # Custom log file
    )
    
    print("Configuration:")
    print(f"  API Endpoint: {config.api_endpoint}")
    print(f"  Model: {config.openai_model}")
    print(f"  Max Retries: {config.max_retries}")
    print(f"  Backoff Factor: {config.retry_backoff_factor}")
    print(f"  Log File: {config.processed_log_file}")
    
    extractor = PDFDataExtractor(config)
    
    # Use the custom configured extractor
    # extractor.process_pdf(Path("example.pdf"))


def example_check_processed_files():
    """Check which files have been processed"""
    print("\n" + "=" * 60)
    print("EXAMPLE 4: Check Processed Files")
    print("=" * 60)
    
    log_file = Path("processed_files.json")
    
    if log_file.exists():
        with open(log_file, 'r') as f:
            processed = json.load(f)
        
        print(f"\nTotal processed files: {len(processed)}")
        
        for filename, details in processed.items():
            status = "✓" if details['success'] else "✗"
            print(f"{status} {Path(filename).name}")
            print(f"   Timestamp: {details['timestamp']}")
            print(f"   Success: {details['success']}")
    else:
        print("No processed files log found.")


def example_reprocess_failed():
    """Reprocess only failed files"""
    print("\n" + "=" * 60)
    print("EXAMPLE 5: Reprocess Failed Files")
    print("=" * 60)
    
    log_file = Path("processed_files.json")
    
    if not log_file.exists():
        print("No processed files log found.")
        return
    
    with open(log_file, 'r') as f:
        processed = json.load(f)
    
    # Find failed files
    failed_files = [
        Path(filename) 
        for filename, details in processed.items() 
        if not details['success']
    ]
    
    if not failed_files:
        print("No failed files to reprocess.")
        return
    
    print(f"Found {len(failed_files)} failed files")
    
    config = PDFExtractorConfig(
        api_endpoint="https://internal-api.example.com/upload"
    )
    extractor = PDFDataExtractor(config)
    
    # Reprocess each failed file
    for pdf_path in failed_files:
        if pdf_path.exists():
            print(f"\nReprocessing: {pdf_path.name}")
            result = extractor.process_pdf(pdf_path)
            print(f"Result: {'Success' if result['success'] else 'Failed'}")
        else:
            print(f"\nFile not found: {pdf_path}")


if __name__ == "__main__":
    # Uncomment the example you want to run
    
    # example_single_file()
    # example_batch_processing()
    # example_with_custom_config()
    # example_check_processed_files()
    # example_reprocess_failed()
    
    print("\nPlease uncomment an example function in the script to run it.")
    print("Don't forget to set your environment variables:")
    print("  export OPENAI_API_KEY='sk-...'")
    print("  export API_BEARER_TOKEN='your-token'")