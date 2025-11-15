import os
import time
import sys
import json
from ingestion_framework.utils.logger import get_logger

logger = get_logger(__name__)

class ClassificationService:
    """Classify and process ingested data"""
    
    @staticmethod
    def classify_opportunity(opportunity):
        """Add classification logic"""
        try:
            # Example classification
            amount = opportunity.get("Amount", 0)
            
            if amount > 100000:
                opportunity["classification"] = "High Value"
            elif amount > 50000:
                opportunity["classification"] = "Medium Value"
            else:
                opportunity["classification"] = "Low Value"
            
            return opportunity
        except Exception as e:
            logger.error(f"Classification failed: {e}")
            raise

def process_data(input_file, output_file):
    """Process and classify data"""
    try:
        if not os.path.exists(input_file):
            raise FileNotFoundError(f"Input file not found: {input_file}")
        
        with open(input_file, "r") as f:
            data = json.load(f)
        
        # Ensure data is a list
        if not isinstance(data, list):
            data = [data]
        
        # Classify each record
        processed_data = []
        for record in data:
            classified_record = ClassificationService.classify_opportunity(record)
            processed_data.append(classified_record)
        
        # Write output
        os.makedirs(os.path.dirname(output_file) or ".", exist_ok=True)
        with open(output_file, "w") as f:
            json.dump(processed_data, f, indent=2)
        
        logger.info(f"Classification completed. Processed {len(processed_data)} records")
        return True
        
    except Exception as e:
        logger.error(f"Processing failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    if len(sys.argv) < 3:
        logger.error("Usage: python classification_service.py <input_file> <output_file>")
        sys.exit(1)

    input_file = sys.argv[1]
    output_file = sys.argv[2]
    
    process_data(input_file, output_file)
