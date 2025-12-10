import asyncio
import json
import time
import hashlib
import logging
from dataclasses import dataclass, field, asdict
from typing import Dict, List, Any, Optional
from datetime import datetime
from collections import defaultdict
from enum import Enum
import queue
import threading
from concurrent.futures import ThreadPoolExecutor
import signal
import sys
import random

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class EventPriority(Enum):
    """Event priority levels"""
    CRITICAL = 4
    HIGH = 3
    MEDIUM = 2
    LOW = 1


class EventCategory(Enum):
    """Event classification categories"""
    SYSTEM = "SYSTEM"
    USER = "USER"
    TRANSACTION = "TRANSACTION"
    MONITORING = "MONITORING"
    ALERT = "ALERT"


@dataclass
class Event:
    """Internal event representation"""
    event_id: str
    source_id: str
    timestamp: float
    received_at: float
    priority: EventPriority
    category: Optional[EventCategory] = None
    load_indicator: float = 0.0
    metadata_hash: Optional[str] = None
    metadata_summary: Optional[Dict[str, Any]] = None
    raw_metadata: Optional[Dict[str, Any]] = None
    payload: Dict[str, Any] = field(default_factory=dict)
    derived_fields: Dict[str, Any] = field(default_factory=dict)
    processing_stage: str = "INTAKE"
    retry_count: int = 0
    sequence_number: int = 0
    
    def __lt__(self, other):
        """Comparison for priority queue ordering"""
        if self.priority.value != other.priority.value:
            return self.priority.value > other.priority.value
        if self.source_id != other.source_id:
            return self.source_id < other.source_id
        return self.sequence_number < other.sequence_number


@dataclass
class ProcessingMetrics:
    """System metrics tracking"""
    total_received: int = 0
    total_processed: int = 0
    total_published: int = 0
    total_failed: int = 0
    total_retried: int = 0
    malformed_events: int = 0
    stage_latencies: Dict[str, List[float]] = field(default_factory=lambda: defaultdict(list))
    
    def add_latency(self, stage: str, latency: float):
        self.stage_latencies[stage].append(latency)
    
    def get_avg_latency(self, stage: str) -> float:
        latencies = self.stage_latencies.get(stage, [])
        return sum(latencies) / len(latencies) if latencies else 0.0


class EventProcessor:
    """
    Real-Time Event Processing System
    
    Architecture:
    1. Intake Stage: Receives, validates, and sequences events
    2. Ordering Stage: Orders events per source with global sequence
    3. Classification Stage: Categorizes events using metadata analysis
    4. Transformation Stage: Derives new fields and enriches events
    5. Publishing Stage: Outputs events in deterministic order
    """
    
    def __init__(self, max_workers: int = 10, queue_size: int = 1000):
        """Initialize the event processing system"""
        
        # Thread-safe queues for each stage
        self.intake_queue = queue.Queue(maxsize=queue_size)
        self.ordering_queue = queue.PriorityQueue(maxsize=queue_size)
        self.classification_queue = queue.Queue(maxsize=queue_size)
        self.transformation_queue = queue.Queue(maxsize=queue_size)
        self.publishing_queue = queue.PriorityQueue(maxsize=queue_size)
        
        # Source tracking for ordering
        self.source_sequences = defaultdict(int)
        self.source_locks = defaultdict(threading.Lock)
        
        # Global sequence counter
        self.global_sequence = 0
        self.global_lock = threading.Lock()
        
        # Processing metrics
        self.metrics = ProcessingMetrics()
        
        # Thread pool
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        
        # Control flags
        self.running = False
        self.shutdown_event = threading.Event()
        
        # Worker threads
        self.workers = []
        
        # Retry configuration
        self.max_retries = 3
        self.retry_delay = 0.1
        
        logger.info("Event Processing System initialized")
    
    # ==================== STAGE 1: INTAKE ====================
    
    def submit_event(self, raw_event: Dict[str, Any]) -> bool:
        """
        Public API: Submit event to the system
        Returns True if accepted, False if rejected
        """
        try:
            # Validate and normalize event
            event = self._validate_and_normalize(raw_event)
            
            if event is None:
                self.metrics.malformed_events += 1
                logger.warning(f"Malformed event rejected: {raw_event.get('event_id', 'UNKNOWN')}")
                return False
            
            # Try to add to intake queue (non-blocking with timeout)
            try:
                self.intake_queue.put(event, timeout=1.0)
                self.metrics.total_received += 1
                return True
            except queue.Full:
                logger.warning("Intake queue full, applying backpressure")
                return False
                
        except Exception as e:
            logger.error(f"Error submitting event: {e}")
            return False
    
    def _validate_and_normalize(self, raw_event: Dict[str, Any]) -> Optional[Event]:
        """
        Validate and normalize incoming event
        Returns None if event is invalid
        """
        try:
            # Required fields
            event_id = raw_event.get('event_id') or raw_event.get('id')
            source_id = raw_event.get('source_id') or raw_event.get('source')
            
            if not event_id or not source_id:
                return None
            
            # Parse timestamp (may be unreliable)
            timestamp = float(raw_event.get('timestamp', time.time()))
            
            # Parse priority
            priority_str = str(raw_event.get('priority', 'MEDIUM')).upper()
            try:
                priority = EventPriority[priority_str]
            except KeyError:
                priority = EventPriority.MEDIUM
            
            # Parse load indicator
            load = float(raw_event.get('load_indicator', 0.0))
            load = max(0.0, min(1.0, load))  # Clamp to [0, 1]
            
            # Extract and hash metadata
            metadata = raw_event.get('metadata', {})
            if not isinstance(metadata, dict):
                metadata = {}
            
            metadata_hash = self._hash_metadata(metadata)
            metadata_summary = self._summarize_metadata(metadata)
            
            # Extract remaining payload
            payload = {k: v for k, v in raw_event.items() 
                      if k not in ['event_id', 'id', 'source_id', 'source', 
                                   'timestamp', 'priority', 'load_indicator', 'metadata']}
            
            return Event(
                event_id=str(event_id),
                source_id=str(source_id),
                timestamp=timestamp,
                received_at=time.time(),
                priority=priority,
                load_indicator=load,
                metadata_hash=metadata_hash,
                metadata_summary=metadata_summary,
                raw_metadata=metadata,
                payload=payload
            )
            
        except Exception as e:
            logger.error(f"Validation error: {e}")
            return None
    
    def _hash_metadata(self, metadata: Dict[str, Any]) -> str:
        """Create deterministic hash of metadata"""
        try:
            # Sort keys for deterministic hashing
            sorted_json = json.dumps(metadata, sort_keys=True)
            return hashlib.sha256(sorted_json.encode()).hexdigest()[:16]
        except:
            return "INVALID_HASH"
    
    def _summarize_metadata(self, metadata: Dict[str, Any]) -> Dict[str, Any]:
        """
        Create metadata summary without reading raw content
        Used for classification without direct metadata access
        """
        summary = {
            'field_count': len(metadata),
            'has_nested': any(isinstance(v, dict) for v in metadata.values()),
            'has_arrays': any(isinstance(v, list) for v in metadata.values()),
            'key_signature': hashlib.md5(
                ''.join(sorted(metadata.keys())).encode()
            ).hexdigest()[:8],
            'complexity_score': self._calculate_complexity(metadata)
        }
        return summary
    
    def _calculate_complexity(self, obj: Any, depth: int = 0) -> float:
        """Calculate structural complexity of nested data"""
        if depth > 5:
            return 1.0
        
        score = 0.0
        if isinstance(obj, dict):
            score = len(obj) * 0.1
            for v in obj.values():
                score += self._calculate_complexity(v, depth + 1)
        elif isinstance(obj, list):
            score = len(obj) * 0.05
            for item in obj[:10]:  # Limit to avoid huge lists
                score += self._calculate_complexity(item, depth + 1)
        else:
            score = 0.01
        
        return min(score, 10.0)  # Cap at 10
    
    # ==================== STAGE 2: ORDERING ====================
    
    def _ordering_worker(self):
        """
        Ordering Stage: Assign sequence numbers per source
        Strategy: Each source gets monotonic sequence, global timestamp for tie-breaking
        """
        logger.info("Ordering worker started")
        
        while self.running or not self.intake_queue.empty():
            try:
                event = self.intake_queue.get(timeout=0.5)
                start_time = time.time()
                
                # Assign source-specific sequence number
                with self.source_locks[event.source_id]:
                    event.sequence_number = self.source_sequences[event.source_id]
                    self.source_sequences[event.source_id] += 1
                
                # Update processing stage
                event.processing_stage = "ORDERING"
                
                # Add to ordering queue (priority queue for global ordering)
                self.ordering_queue.put(event)
                
                self.metrics.add_latency("ordering", time.time() - start_time)
                
            except queue.Empty:
                continue
            except Exception as e:
                logger.error(f"Ordering error: {e}")
                self.metrics.total_failed += 1
        
        logger.info("Ordering worker stopped")
    
    # ==================== STAGE 3: CLASSIFICATION ====================
    
    def _classification_worker(self):
        """
        Classification Stage: Categorize events using metadata summary
        Strategy: Use metadata features WITHOUT reading raw metadata directly
        """
        logger.info("Classification worker started")
        
        while self.running or not self.ordering_queue.empty():
            try:
                event = self.ordering_queue.get(timeout=0.5)
                start_time = time.time()
                
                # Classify based on multiple factors
                event.category = self._classify_event(event)
                event.processing_stage = "CLASSIFICATION"
                
                # Add to classification queue
                self.classification_queue.put(event)
                
                self.metrics.add_latency("classification", time.time() - start_time)
                
            except queue.Empty:
                continue
            except Exception as e:
                logger.error(f"Classification error: {e}")
                if hasattr(locals().get('event'), 'retry_count'):
                    self._handle_retry(event, self.classification_queue)
        
        logger.info("Classification worker stopped")
    
    def _classify_event(self, event: Event) -> EventCategory:
        """
        Classify event using metadata summary (not raw metadata)
        Multi-factor classification logic
        """
        score_map = defaultdict(float)
        
        # Factor 1: Priority hints
        if event.priority == EventPriority.CRITICAL:
            score_map[EventCategory.ALERT] += 3.0
        elif event.priority == EventPriority.HIGH:
            score_map[EventCategory.SYSTEM] += 2.0
        
        # Factor 2: Load indicator
        if event.load_indicator > 0.8:
            score_map[EventCategory.MONITORING] += 2.0
            score_map[EventCategory.ALERT] += 1.5
        elif event.load_indicator > 0.5:
            score_map[EventCategory.SYSTEM] += 1.0
        
        # Factor 3: Metadata complexity (without reading raw metadata)
        if event.metadata_summary:
            complexity = event.metadata_summary.get('complexity_score', 0.0)
            field_count = event.metadata_summary.get('field_count', 0)
            
            if complexity > 5.0:
                score_map[EventCategory.TRANSACTION] += 2.0
            elif complexity > 2.0:
                score_map[EventCategory.USER] += 1.5
            
            if field_count > 10:
                score_map[EventCategory.TRANSACTION] += 1.0
            
            if event.metadata_summary.get('has_nested'):
                score_map[EventCategory.TRANSACTION] += 0.5
        
        # Factor 4: Source ID patterns
        source_lower = event.source_id.lower()
        if 'alert' in source_lower or 'warning' in source_lower:
            score_map[EventCategory.ALERT] += 2.0
        elif 'user' in source_lower:
            score_map[EventCategory.USER] += 2.0
        elif 'monitor' in source_lower:
            score_map[EventCategory.MONITORING] += 2.0
        elif 'system' in source_lower:
            score_map[EventCategory.SYSTEM] += 1.5
        
        # Factor 5: Payload analysis
        if 'transaction_id' in event.payload or 'txn_id' in event.payload:
            score_map[EventCategory.TRANSACTION] += 2.0
        
        # Select category with highest score
        if score_map:
            category = max(score_map.items(), key=lambda x: x[1])[0]
        else:
            category = EventCategory.SYSTEM  # Default
        
        return category
    
    # ==================== STAGE 4: TRANSFORMATION ====================
    
    def _transformation_worker(self):
        """
        Transformation Stage: Generate derived fields
        Strategy: Add computed fields based on event characteristics
        """
        logger.info("Transformation worker started")
        
        while self.running or not self.classification_queue.empty():
            try:
                event = self.classification_queue.get(timeout=0.5)
                start_time = time.time()
                
                # Generate derived fields
                event.derived_fields = self._generate_derived_fields(event)
                event.processing_stage = "TRANSFORMATION"
                
                # Add to transformation queue
                self.transformation_queue.put(event)
                
                self.metrics.add_latency("transformation", time.time() - start_time)
                
            except queue.Empty:
                continue
            except Exception as e:
                logger.error(f"Transformation error: {e}")
                if hasattr(locals().get('event'), 'retry_count'):
                    self._handle_retry(event, self.transformation_queue)
        
        logger.info("Transformation worker stopped")
    
    def _generate_derived_fields(self, event: Event) -> Dict[str, Any]:
        """Generate computed fields from event data"""
        derived = {}
        
        # Processing latency
        derived['intake_latency_ms'] = (event.received_at - event.timestamp) * 1000
        derived['total_processing_time_ms'] = (time.time() - event.received_at) * 1000
        
        # Risk score based on multiple factors
        risk_score = 0.0
        risk_score += event.priority.value * 0.25
        risk_score += event.load_indicator * 0.25
        
        if event.metadata_summary:
            complexity = event.metadata_summary.get('complexity_score', 0.0)
            risk_score += min(complexity / 10.0, 1.0) * 0.25
        
        if event.category == EventCategory.ALERT:
            risk_score += 0.25
        
        derived['risk_score'] = round(risk_score, 3)
        
        # Category confidence
        derived['category_confidence'] = self._calculate_category_confidence(event)
        
        # Anomaly flag
        derived['is_anomaly'] = (
            event.load_indicator > 0.9 or
            risk_score > 0.8 or
            event.priority == EventPriority.CRITICAL
        )
        
        # Processing path signature
        derived['processing_signature'] = hashlib.md5(
            f"{event.source_id}:{event.category.value}:{event.priority.name}".encode()
        ).hexdigest()[:12]
        
        return derived
    
    def _calculate_category_confidence(self, event: Event) -> float:
        """Calculate confidence in category assignment"""
        # Simplified confidence calculation
        confidence = 0.5
        
        if event.priority in [EventPriority.CRITICAL, EventPriority.HIGH]:
            confidence += 0.2
        
        if event.metadata_summary and event.metadata_summary.get('field_count', 0) > 5:
            confidence += 0.15
        
        if event.load_indicator > 0.7:
            confidence += 0.15
        
        return min(confidence, 1.0)
    
    # ==================== STAGE 5: PUBLISHING ====================
    
    def _publishing_worker(self):
        """
        Publishing Stage: Output events in deterministic order
        Global Ordering Rule: Priority > Source ID > Sequence Number
        """
        logger.info("Publishing worker started")
        
        published_count = 0
        
        while self.running or not self.transformation_queue.empty():
            try:
                event = self.transformation_queue.get(timeout=0.5)
                start_time = time.time()
                
                # Add to priority queue for final ordering
                with self.global_lock:
                    self.global_sequence += 1
                    event.derived_fields['global_publish_sequence'] = self.global_sequence
                
                event.processing_stage = "PUBLISHING"
                
                # Publish event
                self._publish_event(event)
                
                published_count += 1
                self.metrics.total_published += 1
                self.metrics.total_processed += 1
                
                self.metrics.add_latency("publishing", time.time() - start_time)
                
                if published_count % 100 == 0:
                    logger.info(f"Published {published_count} events")
                
            except queue.Empty:
                continue
            except Exception as e:
                logger.error(f"Publishing error: {e}")
                if hasattr(locals().get('event'), 'retry_count'):
                    self._handle_retry(event, self.transformation_queue)
        
        logger.info(f"Publishing worker stopped. Total published: {published_count}")
    
    def _publish_event(self, event: Event):
        """
        Final event publication
        In production: send to message queue, database, API, etc.
        """
        # Create final output
        output = {
            'event_id': event.event_id,
            'source_id': event.source_id,
            'timestamp': event.timestamp,
            'priority': event.priority.name,
            'category': event.category.value,
            'sequence_number': event.sequence_number,
            'global_sequence': event.derived_fields.get('global_publish_sequence'),
            'derived_fields': event.derived_fields,
            'metadata_hash': event.metadata_hash,
            'processing_stage': event.processing_stage
        }
        
        # Log output (in production, send to external system)
        logger.debug(f"PUBLISHED: {json.dumps(output)}")
        
        # Optional: Write to file, send to message queue, etc.
        # self._write_to_output_stream(output)
    
    # ==================== RETRY & ERROR HANDLING ====================
    
    def _handle_retry(self, event: Event, target_queue: queue.Queue):
        """Handle event retry with exponential backoff"""
        if event.retry_count < self.max_retries:
            event.retry_count += 1
            delay = self.retry_delay * (2 ** event.retry_count)
            
            logger.warning(f"Retrying event {event.event_id}, attempt {event.retry_count}")
            time.sleep(delay)
            
            try:
                target_queue.put(event, timeout=1.0)
                self.metrics.total_retried += 1
            except queue.Full:
                logger.error(f"Failed to retry event {event.event_id}: queue full")
                self.metrics.total_failed += 1
        else:
            logger.error(f"Event {event.event_id} exceeded max retries")
            self.metrics.total_failed += 1
    
    # ==================== SYSTEM CONTROL ====================
    
    def start(self):
        """Start the processing system"""
        if self.running:
            logger.warning("System already running")
            return
        
        self.running = True
        logger.info("Starting Event Processing System...")
        
        # Start worker threads for each stage
        stages = [
            ("Ordering", self._ordering_worker),
            ("Classification", self._classification_worker),
            ("Transformation", self._transformation_worker),
            ("Publishing", self._publishing_worker),
        ]
        
        for stage_name, worker_func in stages:
            thread = threading.Thread(target=worker_func, name=f"{stage_name}Worker")
            thread.daemon = False
            thread.start()
            self.workers.append(thread)
        
        logger.info(f"System started with {len(self.workers)} worker threads")
    
    def shutdown(self, timeout: int = 30):
        """
        Graceful shutdown: process remaining events then stop
        """
        logger.info("Initiating graceful shutdown...")
        self.running = False
        self.shutdown_event.set()
        
        # Wait for workers to finish
        start_time = time.time()
        for worker in self.workers:
            remaining = timeout - (time.time() - start_time)
            if remaining > 0:
                worker.join(timeout=remaining)
                if worker.is_alive():
                    logger.warning(f"Worker {worker.name} did not stop gracefully")
            else:
                logger.warning("Shutdown timeout exceeded")
                break
        
        # Shutdown thread pool
        self.executor.shutdown(wait=True)
        
        logger.info("System shutdown complete")
        self._print_metrics()
    
    def _print_metrics(self):
        """Print final system metrics"""
        logger.info("="*70)
        logger.info("FINAL SYSTEM METRICS")
        logger.info("="*70)
        logger.info(f"Total Received: {self.metrics.total_received}")
        logger.info(f"Total Processed: {self.metrics.total_processed}")
        logger.info(f"Total Published: {self.metrics.total_published}")
        logger.info(f"Total Failed: {self.metrics.total_failed}")
        logger.info(f"Total Retried: {self.metrics.total_retried}")
        logger.info(f"Malformed Events: {self.metrics.malformed_events}")
        logger.info("")
        logger.info("Average Stage Latencies:")
        for stage in ['ordering', 'classification', 'transformation', 'publishing']:
            avg_latency = self.metrics.get_avg_latency(stage)
            logger.info(f"  {stage.capitalize()}: {avg_latency*1000:.2f}ms")
        logger.info("="*70)


# ==================== DEMO & TESTING ====================

def generate_test_events(count: int = 100) -> List[Dict[str, Any]]:
    """Generate realistic test events"""
    import random
    
    events = []
    sources = ['service-a', 'service-b', 'user-api', 'monitor-system', 'alert-engine']
    priorities = ['LOW', 'MEDIUM', 'HIGH', 'CRITICAL']
    
    for i in range(count):
        event = {
            'event_id': f'evt-{i:05d}',
            'source_id': random.choice(sources),
            'timestamp': time.time() + random.uniform(-10, 10),
            'priority': random.choice(priorities),
            'load_indicator': random.uniform(0.0, 1.0),
            'metadata': {
                'user_id': f'user-{random.randint(1000, 9999)}',
                'session_id': f'session-{random.randint(100, 999)}',
                'nested_data': {
                    'level1': {
                        'level2': {
                            'value': random.randint(1, 100)
                        }
                    }
                },
                'tags': ['tag1', 'tag2', 'tag3'][:random.randint(1, 3)],
                'extra_field_1': random.choice(['A', 'B', 'C']),
                'extra_field_2': random.randint(1, 1000)
            },
            'custom_field': f'custom-{i}',
            'irregular_field': random.choice([None, 'value', 123, True])
        }
        events.append(event)
    
    return events


def main():
    """Main demonstration"""
    logger.info("="*70)
    logger.info("REAL-TIME EVENT PROCESSING SYSTEM - DEMO")
    logger.info("="*70)
    
    # Initialize processor
    processor = EventProcessor(max_workers=8, queue_size=500)
    
    # Setup signal handler for graceful shutdown
    def signal_handler(sig, frame):
        logger.info("Interrupt received, shutting down...")
        processor.shutdown()
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    
    # Start system
    processor.start()
    
    # Generate and submit test events
    logger.info("Generating test events...")
    test_events = generate_test_events(count=200)
    
    logger.info(f"Submitting {len(test_events)} events...")
    submitted = 0
    rejected = 0
    
    for event in test_events:
        if processor.submit_event(event):
            submitted += 1
        else:
            rejected += 1
        
        # Simulate irregular intake rate
        time.sleep(random.uniform(0.001, 0.01))
    
    logger.info(f"Submitted: {submitted}, Rejected: {rejected}")
    
    # Wait for processing to complete
    logger.info("Waiting for processing to complete...")
    time.sleep(5)
    
    # Graceful shutdown
    processor.shutdown(timeout=30)
    
    logger.info("\n✅ Demo complete!")


if __name__ == "__main__":
    main()
