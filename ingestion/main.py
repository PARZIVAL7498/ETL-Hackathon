"""
Main ingestion module
"""

import sys
from pathlib import Path

# Add project root to path
PROJECT_ROOT = str(Path(__file__).parent.parent)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

# Now use absolute imports
from ingestion.sources.api import APIIngestion
from ingestion.base import BaseIngestion

def main():
    """Run ingestion"""
    print("Starting ingestion...")
    # Your ingestion logic here

if __name__ == '__main__':
    main()
