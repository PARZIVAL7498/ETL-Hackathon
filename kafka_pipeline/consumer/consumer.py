import json
import logging
from kafka import KafkaConsumer
from kafka.errors import KafkaError
import sys
import os

# Add project root to path
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
sys.path.insert(0, PROJECT_ROOT)

from ingestion_framework.utils.logger import get_logger
from ingestion_framework.core.config import settings

logger = get_logger(__name__)

class DataConsumer:
    """Kafka consumer for ETL pipeline"""
    
    def __init__(self, topic, bootstrap_servers="localhost:9092", group_id="etl-group"):
        """
        Initialize Kafka consumer
        
        Args:
            topic: Kafka topic to consume from
            bootstrap_servers: Kafka broker addresses
            group_id: Consumer group ID
        """
        try:
            self.topic = topic
            self.consumer = KafkaConsumer(
                topic,
                bootstrap_servers=bootstrap_servers,
                group_id=group_id,
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                session_timeout_ms=30000,
                max_poll_records=500,
            )
            logger.info(f"✅ Consumer initialized for topic: {topic}")
        except KafkaError as e:
            logger.error(f"❌ Failed to initialize consumer: {e}")
            raise

    def consume_messages(self, max_messages=None):
        """
        Consume messages from Kafka topic
        
        Args:
            max_messages: Maximum number of messages to consume
            
        Returns:
            List of consumed messages
        """
        messages = []
        try:
            logger.info(f"🔍 Consuming messages from topic: {self.topic}")
            
            for message in self.consumer:
                try:
                    data = message.value
                    messages.append(data)
                    logger.info(f"✅ Consumed message: {message.key} | Offset: {message.offset}")
                    
                    if max_messages and len(messages) >= max_messages:
                        logger.info(f"Reached max messages limit: {max_messages}")
                        break
                        
                except Exception as e:
                    logger.error(f"❌ Error processing message: {e}")
                    continue
            
            logger.info(f"✅ Total messages consumed: {len(messages)}")
            return messages
            
        except KafkaError as e:
            logger.error(f"❌ Consumer error: {e}")
            raise
        finally:
            self.consumer.close()
            logger.info("Consumer closed")

    def consume_batch(self, batch_size=100, timeout_ms=10000):
        """
        Consume messages in batches
        
        Args:
            batch_size: Number of messages per batch
            timeout_ms: Timeout in milliseconds
            
        Yields:
            Batches of messages
        """
        try:
            batch = []
            logger.info(f"🔍 Consuming messages in batches of {batch_size}")
            
            for message in self.consumer:
                try:
                    batch.append(message.value)
                    
                    if len(batch) >= batch_size:
                        logger.info(f"✅ Batch ready: {len(batch)} messages")
                        yield batch
                        batch = []
                        
                except Exception as e:
                    logger.error(f"❌ Error processing batch message: {e}")
                    continue
            
            # Yield remaining messages
            if batch:
                logger.info(f"✅ Final batch: {len(batch)} messages")
                yield batch
                
        except KafkaError as e:
            logger.error(f"❌ Batch consumer error: {e}")
            raise
        finally:
            self.consumer.close()

    def save_to_file(self, messages, output_file):
        """
        Save consumed messages to file
        
        Args:
            messages: List of messages to save
            output_file: Output file path
        """
        try:
            os.makedirs(os.path.dirname(output_file) or ".", exist_ok=True)
            
            with open(output_file, 'w') as f:
                json.dump(messages, f, indent=2)
            
            logger.info(f"✅ Saved {len(messages)} messages to: {output_file}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to save messages: {e}")
            raise

def main():
    """Main entry point"""
    try:
        # Configuration
        topic = "salesforce_opportunities"
        bootstrap_servers = "localhost:9092"
        group_id = "etl-group"
        output_file = "raw_zone/kafka_messages.json"
        
        # Create consumer
        consumer = DataConsumer(
            topic=topic,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id
        )
        
        # Consume messages
        messages = consumer.consume_messages(max_messages=1000)
        
        # Save to file
        if messages:
            consumer.save_to_file(messages, output_file)
            logger.info(f"✅ Pipeline completed: {len(messages)} messages processed")
        else:
            logger.warning("⚠️ No messages consumed")
            
    except Exception as e:
        logger.error(f"❌ Consumer pipeline failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()