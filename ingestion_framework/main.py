from ingestion_framework.services.ingestion_service import IngestionService

def main():
    """
    Main function to trigger the ingestion process.
    """
    # Example for sales source
    sales_ingestion = IngestionService(source="sales")
    sales_data = {"product_id": 123, "quantity": 10, "price": 9.99}
    sales_ingestion.ingest(sales_data)

    # Example for another source with invalid data
    inventory_ingestion = IngestionService(source="inventory")
    inventory_data = {"item_id": 456, "stock": 100}
    inventory_ingestion.ingest(inventory_data)

if __name__ == "__main__":
    main()
