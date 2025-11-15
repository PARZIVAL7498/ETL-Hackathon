import json
import logging
from kafka import KafkaProducer
from kafka.errors import KafkaError
import sys
import os

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
sys.path.insert(0, PROJECT_ROOT)

from ingestion_framework.utils.logger import get_logger

logger = get_logger(__name__)

class DataProducer:
    """Kafka producer for ETL pipeline"""
    
    def __init__(self, topic, bootstrap_servers="localhost:9092"):
        """
        Initialize Kafka producer
        
        Args:
            topic: Kafka topic to produce to
            bootstrap_servers: Kafka broker addresses
        """
        try:
            self.topic = topic
            self.producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                acks='all',
                retries=3,
                max_in_flight_requests_per_connection=5,
            )
            logger.info(f"✅ Producer initialized for topic: {topic}")
        except KafkaError as e:
            logger.error(f"❌ Failed to initialize producer: {e}")
            raise

    def send_message(self, key, value):
        """
        Send a single message to Kafka
        
        Args:
            key: Message key
            value: Message value (dict or string)
            
        Returns:
            FutureRecordMetadata
        """
        try:
            future = self.producer.send(
                self.topic,
                key=key.encode('utf-8') if isinstance(key, str) else key,
                value=value
            )
            
            record_metadata = future.get(timeout=10)
            logger.info(f"✅ Message sent to partition {record_metadata.partition} at offset {record_metadata.offset}")
            return record_metadata
            
        except KafkaError as e:
            logger.error(f"❌ Failed to send message: {e}")
            raise

    def send_batch(self, messages):
        """
        Send multiple messages to Kafka
        
        Args:
            messages: List of (key, value) tuples
            
        Returns:
            List of FutureRecordMetadata
        """
        try:
            futures = []
            logger.info(f"📤 Sending batch of {len(messages)} messages")
            
            for key, value in messages:
                future = self.producer.send(
                    self.topic,
                    key=key.encode('utf-8') if isinstance(key, str) else key,
                    value=value
                )
                futures.append(future)
            
            # Wait for all messages to be sent
            for i, future in enumerate(futures):
                try:
                    record_metadata = future.get(timeout=10)
                    logger.info(f"✅ Message {i+1}/{len(messages)} sent successfully")
                except KafkaError as e:
                    logger.error(f"❌ Failed to send message {i+1}: {e}")
            
            self.producer.flush()
            logger.info(f"✅ Batch of {len(messages)} messages sent successfully")
            return futures
            
        except Exception as e:
            logger.error(f"❌ Batch send failed: {e}")
            raise

    def send_from_file(self, input_file):
        """
        Send messages from a JSON file
        
        Args:
            input_file: Path to JSON file
            
        Returns:
            Number of messages sent
        """
        try:
            with open(input_file, 'r') as f:
                data = json.load(f)
            
            # Handle both list and dict
            if isinstance(data, dict):
                data = [data]
            
            messages = []
            for idx, record in enumerate(data):
                key = record.get('id', str(idx))
                messages.append((key, record))
            
            self.send_batch(messages)
            logger.info(f"✅ Sent {len(messages)} messages from file: {input_file}")
            return len(messages)
            
        except Exception as e:
            logger.error(f"❌ Failed to send from file: {e}")
            raise
        finally:
            self.producer.close()

def main():
    """Main entry point"""
    try:
        # Configuration
        topic = "salesforce_opportunities"
        bootstrap_servers = "localhost:9092"
        input_file = "raw_zone/salesforce_opportunities.json"
        
        # Create producer
        producer = DataProducer(
            topic=topic,
            bootstrap_servers=bootstrap_servers
        )
        
        # Send messages from file
        if os.path.exists(input_file):
            count = producer.send_from_file(input_file)
            logger.info(f"✅ Producer pipeline completed: {count} messages sent")
        else:
            logger.error(f"❌ Input file not found: {input_file}")
            sys.exit(1)
            
    except Exception as e:
        logger.error(f"❌ Producer pipeline failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()