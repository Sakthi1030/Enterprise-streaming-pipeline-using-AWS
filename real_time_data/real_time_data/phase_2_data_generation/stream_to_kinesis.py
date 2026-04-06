#!/usr/bin/env python3
"""
Stream generated transactions to AWS Kinesis Data Stream.
Handles real-time streaming with error handling, retries, and monitoring.
"""

import json
import time
import random
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List
from pathlib import Path
import boto3
from botocore.exceptions import ClientError, BotoCoreError
import threading
from queue import Queue
from concurrent.futures import ThreadPoolExecutor, as_completed
import signal
import sys
from dataclasses import dataclass, asdict
import hashlib
import gzip
from io import BytesIO

# Configure logging
Path("logs").mkdir(exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/kinesis_streaming.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

@dataclass
class StreamMetrics:
    """Metrics for streaming operations"""
    records_sent: int = 0
    records_failed: int = 0
    bytes_sent: int = 0
    start_time: Optional[datetime] = None
    last_success_time: Optional[datetime] = None
    last_error_time: Optional[datetime] = None
    error_count: int = 0
    retry_count: int = 0
    
    def to_dict(self) -> Dict:
        """Convert metrics to dictionary"""
        return {
            "records_sent": self.records_sent,
            "records_failed": self.records_failed,
            "bytes_sent": self.bytes_sent,
            "success_rate": self.success_rate,
            "throughput_mbps": self.throughput_mbps,
            "uptime_seconds": self.uptime_seconds,
            "error_rate": self.error_rate
        }
    
    @property
    def success_rate(self) -> float:
        """Calculate success rate"""
        total = self.records_sent + self.records_failed
        return (self.records_sent / total * 100) if total > 0 else 100.0
    
    @property
    def throughput_mbps(self) -> float:
        """Calculate throughput in MB/s"""
        if not self.start_time:
            return 0.0
        elapsed = (datetime.now() - self.start_time).total_seconds()
        return (self.bytes_sent / (1024 * 1024)) / elapsed if elapsed > 0 else 0.0
    
    @property
    def uptime_seconds(self) -> float:
        """Calculate uptime in seconds"""
        if not self.start_time:
            return 0.0
        return (datetime.now() - self.start_time).total_seconds()
    
    @property
    def error_rate(self) -> float:
        """Calculate error rate per minute"""
        if not self.start_time:
            return 0.0
        elapsed_minutes = self.uptime_seconds / 60
        return self.error_count / elapsed_minutes if elapsed_minutes > 0 else 0.0

class KinesisStreamer:
    """Stream data to AWS Kinesis with monitoring and retry logic"""
    
    def __init__(self, stream_name: str, region: str = "us-east-1", 
                 max_retries: int = 3, batch_size: int = 500):
        """
        Initialize Kinesis streamer
        
        Args:
            stream_name: Kinesis stream name
            region: AWS region
            max_retries: Maximum number of retry attempts
            batch_size: Maximum records per batch (max 500 for Kinesis)
        """
        self.stream_name = stream_name
        self.region = region
        self.max_retries = max_retries
        self.batch_size = min(batch_size, 500)  # Kinesis limit
        
        # Initialize clients
        self.kinesis_client = boto3.client('kinesis', region_name=region)
        self.cloudwatch = boto3.client('cloudwatch', region_name=region)
        
        # Metrics and state
        self.metrics = StreamMetrics()
        self.running = False
        self.queue = Queue(maxsize=10000)  # Buffer queue
        
        # Thread pool for concurrent sends
        self.executor = ThreadPoolExecutor(max_workers=10)
        
        # Stream description
        self.stream_info = self._describe_stream()
        
        logger.info(f"Initialized KinesisStreamer for stream: {stream_name}")
        logger.info(f"Stream status: {self.stream_info.get('StreamStatus', 'UNKNOWN')}")
        logger.info(f"Shard count: {self.stream_info.get('Shards', [])}")
    
    def _describe_stream(self) -> Dict:
        """Get stream information"""
        try:
            response = self.kinesis_client.describe_stream(StreamName=self.stream_name)
            return response.get('StreamDescription', {})
        except Exception as e:
            logger.error(f"Failed to describe stream: {str(e)}")
            return {}
    
    def _calculate_partition_key(self, record: Dict) -> str:
        """Calculate partition key for record"""
        # Use customer_id if available, otherwise random
        if "customer" in record and "customer_id" in record["customer"]:
            return record["customer"]["customer_id"]
        elif "order_id" in record:
            return record["order_id"]
        else:
            # Generate consistent hash from record
            record_str = json.dumps(record, sort_keys=True)
            return hashlib.md5(record_str.encode()).hexdigest()
    
    def _compress_record(self, record: Dict) -> bytes:
        """Compress record using gzip"""
        record_str = json.dumps(record)
        buf = BytesIO()
        with gzip.GzipFile(fileobj=buf, mode='w') as f:
            f.write(record_str.encode())
        return buf.getvalue()
    
    def _create_record_entry(self, record: Dict, compress: bool = False) -> Dict:
        """Create Kinesis record entry"""
        if compress:
            data = self._compress_record(record)
            return {
                'Data': data,
                'PartitionKey': self._calculate_partition_key(record),
                'ExplicitHashKey': str(random.getrandbits(128))
            }
        else:
            return {
                'Data': json.dumps(record).encode('utf-8'),
                'PartitionKey': self._calculate_partition_key(record),
                'ExplicitHashKey': str(random.getrandbits(128))
            }
    
    def send_record(self, record: Dict, compress: bool = False) -> bool:
        """
        Send a single record to Kinesis
        
        Args:
            record: Data record to send
            compress: Whether to compress the record
            
        Returns:
            bool: True if successful, False otherwise
        """
        for attempt in range(self.max_retries + 1):
            try:
                entry = self._create_record_entry(record, compress)
                
                response = self.kinesis_client.put_record(
                    StreamName=self.stream_name,
                    Data=entry['Data'],
                    PartitionKey=entry['PartitionKey'],
                    ExplicitHashKey=entry.get('ExplicitHashKey')
                )
                
                # Update metrics
                self.metrics.records_sent += 1
                self.metrics.bytes_sent += len(entry['Data'])
                self.metrics.last_success_time = datetime.now()
                
                logger.debug(f"Record sent successfully. SequenceNumber: {response['SequenceNumber']}")
                return True
                
            except (ClientError, BotoCoreError) as e:
                self.metrics.error_count += 1
                self.metrics.last_error_time = datetime.now()
                
                if attempt < self.max_retries:
                    retry_delay = (2 ** attempt) + random.uniform(0, 1)
                    logger.warning(f"Attempt {attempt + 1} failed: {str(e)}. Retrying in {retry_delay:.2f}s")
                    time.sleep(retry_delay)
                    self.metrics.retry_count += 1
                else:
                    logger.error(f"Failed to send record after {self.max_retries} retries: {str(e)}")
                    self.metrics.records_failed += 1
                    return False
            
            except Exception as e:
                logger.error(f"Unexpected error sending record: {str(e)}")
                self.metrics.records_failed += 1
                return False
    
    def send_batch(self, records: List[Dict], compress: bool = False) -> Dict:
        """
        Send batch of records to Kinesis
        
        Args:
            records: List of data records
            compress: Whether to compress records
            
        Returns:
            Dict: Batch results with success/failure counts
        """
        if not records:
            return {"success": 0, "failed": 0, "total": 0}
        
        # Split into batches of max batch_size
        batches = [records[i:i + self.batch_size] 
                  for i in range(0, len(records), self.batch_size)]
        
        results = {"success": 0, "failed": 0, "total": len(records)}
        
        for batch in batches:
            batch_result = self._send_single_batch(batch, compress)
            results["success"] += batch_result["success"]
            results["failed"] += batch_result["failed"]
        
        return results
    
    def _send_single_batch(self, batch: List[Dict], compress: bool) -> Dict:
        """Send a single batch to Kinesis"""
        entries = []
        for record in batch:
            entry = self._create_record_entry(record, compress)
            entries.append(entry)
        
        for attempt in range(self.max_retries + 1):
            try:
                response = self.kinesis_client.put_records(
                    Records=entries,
                    StreamName=self.stream_name
                )
                
                # Process response
                success_count = response.get('FailedRecordCount', len(batch))
                success_count = len(batch) - success_count
                failed_count = len(batch) - success_count
                
                # Update metrics
                self.metrics.records_sent += success_count
                self.metrics.records_failed += failed_count
                
                for entry in entries:
                    self.metrics.bytes_sent += len(entry['Data'])
                
                self.metrics.last_success_time = datetime.now()
                
                if failed_count > 0:
                    logger.warning(f"Batch partially failed: {success_count}/{len(batch)} sent")
                    
                    # Log failed records
                    for i, record in enumerate(response.get('Records', [])):
                        if 'ErrorCode' in record:
                            logger.error(f"Record {i} failed: {record.get('ErrorCode')} - {record.get('ErrorMessage')}")
                
                else:
                    logger.debug(f"Batch sent successfully: {len(batch)} records")
                
                return {"success": success_count, "failed": failed_count}
                
            except (ClientError, BotoCoreError) as e:
                self.metrics.error_count += 1
                self.metrics.last_error_time = datetime.now()
                
                if attempt < self.max_retries:
                    retry_delay = (2 ** attempt) + random.uniform(0, 1)
                    logger.warning(f"Batch attempt {attempt + 1} failed: {str(e)}. Retrying in {retry_delay:.2f}s")
                    time.sleep(retry_delay)
                    self.metrics.retry_count += 1
                else:
                    logger.error(f"Failed to send batch after {self.max_retries} retries: {str(e)}")
                    self.metrics.records_failed += len(batch)
                    return {"success": 0, "failed": len(batch)}
            
            except Exception as e:
                logger.error(f"Unexpected error sending batch: {str(e)}")
                self.metrics.records_failed += len(batch)
                return {"success": 0, "failed": len(batch)}
    
    def start_streaming(self, data_generator, records_per_second: int = 100,
                       duration_minutes: Optional[int] = None):
        """
        Start streaming data from generator
        
        Args:
            data_generator: Generator function that yields records
            records_per_second: Target records per second
            duration_minutes: Duration in minutes (None for infinite)
        """
        self.running = True
        self.metrics.start_time = datetime.now()
        
        logger.info(f"Starting streaming to {self.stream_name}")
        logger.info(f"Target rate: {records_per_second} records/sec")
        logger.info(f"Duration: {'infinite' if duration_minutes is None else f'{duration_minutes} minutes'}")
        
        # Handle termination signals
        def signal_handler(sig, frame):
            logger.info("Received termination signal, stopping stream...")
            self.running = False
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        
        # Calculate end time if duration specified
        end_time = None
        if duration_minutes:
            end_time = datetime.now() + timedelta(minutes=duration_minutes)
        
        # Producer thread: generate and queue records
        def producer():
            records_generated = 0
            try:
                while self.running:
                    if end_time and datetime.now() >= end_time:
                        logger.info("Duration limit reached, stopping producer")
                        break
                    
                    # Generate records
                    record = data_generator() if callable(data_generator) else next(data_generator)

                    if record:
                        # Add to queue with timeout
                        try:
                            self.queue.put(record, timeout=1)
                            records_generated += 1
                            
                            # Log progress
                            if records_generated % 1000 == 0:
                                logger.info(f"Generated {records_generated} records")
                                
                        except Exception as e:
                            logger.error(f"Failed to queue record: {str(e)}")
                    
                    # Control generation rate
                    time.sleep(1.0 / records_per_second)
            
            except StopIteration:
                logger.info("Data generator exhausted")
            except Exception as e:
                logger.error(f"Producer error: {str(e)}")
            finally:
                self.running = False
                logger.info(f"Producer stopped. Total generated: {records_generated}")
        
        # Consumer thread: send records from queue
        def consumer():
            batch = []
            batch_start_time = time.time()
            
            while self.running or not self.queue.empty():
                try:
                    # Get record from queue with timeout
                    record = self.queue.get(timeout=1)
                    batch.append(record)
                    
                    # Send batch when full or timeout
                    current_time = time.time()
                    batch_full = len(batch) >= self.batch_size
                    batch_timeout = (current_time - batch_start_time) >= 1.0  # 1 second
                    
                    if batch_full or (batch_timeout and batch):
                        # Send batch
                        result = self.send_batch(batch, compress=True)
                        
                        if result["failed"] > 0:
                            logger.warning(f"Batch had {result['failed']} failures")
                        
                        # Reset batch
                        batch = []
                        batch_start_time = current_time
                    
                    self.queue.task_done()
                    
                    # Periodic metrics logging
                    if self.metrics.records_sent % 1000 == 0:
                        self._log_metrics()
                
                except Exception as e:
                    if self.running:  # Only log if we're supposed to be running
                        logger.error(f"Consumer error: {str(e)}")
            
            # Send any remaining records
            if batch:
                result = self.send_batch(batch, compress=True)
                logger.info(f"Sent final batch: {result['success']} successful, {result['failed']} failed")
        
        # Start threads
        producer_thread = threading.Thread(target=producer, daemon=True)
        consumer_thread = threading.Thread(target=consumer, daemon=True)
        
        producer_thread.start()
        consumer_thread.start()
        
        # Monitor threads
        try:
            while self.running and (end_time is None or datetime.now() < end_time):
                # Check if threads are alive
                if not producer_thread.is_alive() and not consumer_thread.is_alive():
                    logger.info("All threads stopped")
                    break
                
                # Log metrics every 30 seconds
                if int(time.time()) % 30 == 0:
                    self._log_metrics()
                    self._publish_cloudwatch_metrics()
                
                time.sleep(1)
        
        except KeyboardInterrupt:
            logger.info("Keyboard interrupt received")
        
        finally:
            # Stop streaming
            self.running = False
            
            # Wait for threads to finish
            producer_thread.join(timeout=5)
            consumer_thread.join(timeout=5)
            
            # Final metrics
            self._log_metrics(final=True)
            self._publish_cloudwatch_metrics()
            
            logger.info("Streaming stopped")
    
    def stream_from_file(self, file_path: str, records_per_second: int = 100,
                        duration_minutes: Optional[int] = None):
        """
        Stream data from JSON file
        
        Args:
            file_path: Path to JSON file containing records
            records_per_second: Target records per second
            duration_minutes: Duration in minutes
        """
        logger.info(f"Streaming from file: {file_path}")
        
        def file_generator():
            with open(file_path, 'r') as f:
                data = json.load(f)
                for record in data:
                    yield record
        
        self.start_streaming(file_generator(), records_per_second, duration_minutes)
    
    def stream_from_transaction_generator(self, generator, records_per_second: int = 100,
                                         duration_minutes: Optional[int] = None):
        """
        Stream from TransactionGenerator instance
        
        Args:
            generator: TransactionGenerator instance
            records_per_second: Target records per second
            duration_minutes: Duration in minutes
        """
        logger.info("Streaming from TransactionGenerator")
        
        def transaction_generator():
            while True:
                yield generator.generate_transaction()
        
        self.start_streaming(transaction_generator(), records_per_second, duration_minutes)
    
    def _log_metrics(self, final: bool = False):
        """Log streaming metrics"""
        metrics = self.metrics.to_dict()
        
        if final:
            logger.info("=" * 60)
            logger.info("FINAL STREAMING METRICS")
            logger.info("=" * 60)
        
        log_message = (
            f"Metrics - Sent: {self.metrics.records_sent:,} | "
            f"Failed: {self.metrics.records_failed:,} | "
            f"Success: {metrics['success_rate']:.2f}% | "
            f"Throughput: {metrics['throughput_mbps']:.2f} MB/s | "
            f"Errors: {self.metrics.error_count} | "
            f"Retries: {self.metrics.retry_count}"
        )
        
        if final:
            logger.info(log_message)
            logger.info(f"Total runtime: {metrics['uptime_seconds']:.2f} seconds")
        else:
            logger.info(log_message)
    
    def _publish_cloudwatch_metrics(self):
        """Publish metrics to CloudWatch"""
        try:
            namespace = f"SalesPlatform/Kinesis/{self.stream_name}"
            
            metrics_data = [
                {
                    'MetricName': 'RecordsSent',
                    'Value': self.metrics.records_sent,
                    'Unit': 'Count'
                },
                {
                    'MetricName': 'RecordsFailed',
                    'Value': self.metrics.records_failed,
                    'Unit': 'Count'
                },
                {
                    'MetricName': 'BytesSent',
                    'Value': self.metrics.bytes_sent,
                    'Unit': 'Bytes'
                },
                {
                    'MetricName': 'SuccessRate',
                    'Value': self.metrics.success_rate,
                    'Unit': 'Percent'
                },
                {
                    'MetricName': 'ErrorCount',
                    'Value': self.metrics.error_count,
                    'Unit': 'Count'
                }
            ]
            
            self.cloudwatch.put_metric_data(
                Namespace=namespace,
                MetricData=metrics_data
            )
            
            logger.debug("Published metrics to CloudWatch")
            
        except Exception as e:
            logger.error(f"Failed to publish CloudWatch metrics: {str(e)}")

class TransactionGeneratorWrapper:
    """Wrapper for transaction generation with real-time streaming"""
    
    def __init__(self, master_data_dir: str = "data"):
        """Initialize wrapper"""
        from generate_transactions import TransactionGenerator
        
        self.generator = TransactionGenerator(master_data_dir=master_data_dir)
        logger.info("TransactionGeneratorWrapper initialized")
    
    def generate(self) -> Dict:
        """Generate a transaction"""
        return self.generator.generate_transaction()

def main():
    """Main function"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Stream data to AWS Kinesis")
    parser.add_argument("--stream-name", required=True,
                       help="Kinesis stream name")
    parser.add_argument("--region", default="us-east-1",
                       help="AWS region")
    parser.add_argument("--mode", choices=["file", "generator", "real-time"], 
                       default="generator",
                       help="Data source mode")
    parser.add_argument("--input-file",
                       help="Input JSON file (file mode)")
    parser.add_argument("--master-data-dir", default="data",
                       help="Master data directory (generator mode)")
    parser.add_argument("--records-per-second", type=int, default=100,
                       help="Records per second target")
    parser.add_argument("--duration", type=int,
                       help="Duration in minutes (optional)")
    parser.add_argument("--batch-size", type=int, default=500,
                       help="Batch size for sending")
    parser.add_argument("--max-retries", type=int, default=3,
                       help="Maximum retry attempts")
    parser.add_argument("--compress", action="store_true",
                       help="Compress records before sending")
    
    args = parser.parse_args()
    
    # Create logs directory
    Path("logs").mkdir(exist_ok=True)
    
    # Initialize streamer
    streamer = KinesisStreamer(
        stream_name=args.stream_name,
        region=args.region,
        max_retries=args.max_retries,
        batch_size=args.batch_size
    )
    
    try:
        if args.mode == "file":
            if not args.input_file:
                logger.error("Input file required for file mode")
                return
            
            streamer.stream_from_file(
                file_path=args.input_file,
                records_per_second=args.records_per_second,
                duration_minutes=args.duration
            )
        
        elif args.mode == "generator":
            wrapper = TransactionGeneratorWrapper(
                master_data_dir=args.master_data_dir
            )
            
            streamer.stream_from_transaction_generator(
                generator=wrapper.generator,
                records_per_second=args.records_per_second,
                duration_minutes=args.duration
            )
        
        elif args.mode == "real-time":
            # Real-time mode with simulated data
            wrapper = TransactionGeneratorWrapper(
                master_data_dir=args.master_data_dir
            )
            
            streamer.start_streaming(
                data_generator=wrapper.generate,
                records_per_second=args.records_per_second,
                duration_minutes=args.duration
            )
    
    except Exception as e:
        logger.error(f"Streaming failed: {str(e)}")
        raise
    
    finally:
        logger.info("Streaming application terminated")

if __name__ == "__main__":
    main()