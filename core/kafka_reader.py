import logging
import polars as pl
import threading
from confluent_kafka import Consumer, TopicPartition
from typing import Optional, List
from core.data_schemas.txf_data_pb2 import Tick

class KafkaTickReader:
    """
    Kafka consumer dedicated to fetching live and historical ticks for the charting engine.
    This implementation uses a strict threading.RLock() to prevent `Erroneous state` exceptions
    when the UI thread queries gap history concurrently with the background thread polling live ticks.
    """
    def __init__(self, broker_url: str = "192.168.1.50:9092", topic: str = "txf-tick", group_id: str = "txf_chart_live_v2"):
        self.broker_url = broker_url
        self.topic = topic
        self.group_id = group_id
        self.consumer: Optional[Consumer] = None
        self.logger = logging.getLogger("KafkaTickReader")
        # RLock allows re-entrant locks within the same thread if necessary,
        # perfectly protecting the single underlying librdkafka Consumer instance.
        self.lock = threading.RLock()
        
        # Configure logging if not already configured
        if not self.logger.handlers:
            logging.basicConfig(level=logging.INFO)

        self.connect()

    def connect(self):
        conf = {
            'bootstrap.servers': self.broker_url,
            'group.id': self.group_id,
            'auto.offset.reset': 'latest',
            'enable.auto.commit': False,
            'fetch.min.bytes': 1,
            'fetch.wait.max.ms': 10,
            'socket.nagle.disable': True
        }
        self.consumer = Consumer(conf)
        self.logger.info(f"Connected to Kafka broker: {self.broker_url}, Topic: {self.topic}")

    def fetch_gap_ticks(self, since_ts_ms: int) -> pl.DataFrame:
        """
        Fetches all historical ticks from since_ts_ms to the current High Watermark.
        Thread-safe: Blocks `poll_new_ticks` during execution to prevent State/Offset crashes.
        """
        with self.lock:
            if not self.consumer:
                raise RuntimeError("Kafka consumer not connected.")
            
            self.logger.info(f"Gap Replay: Fetching history from timestamp {since_ts_ms}...")
            
            tp = TopicPartition(self.topic, 0)
            tp.offset = since_ts_ms
            
            # offsets_for_times queries the broker for the earliest offset whose timestamp >= since_ts_ms
            offsets_found = self.consumer.offsets_for_times([tp], timeout=10.0)
            start_offset = offsets_found[0].offset if offsets_found and offsets_found[0].offset != -1 else -1
            
            if start_offset == -1:
                self.logger.info("No newer historical data found for the given timestamp.")
                return pl.DataFrame()
                
            _, high_watermark = self.consumer.get_watermark_offsets(TopicPartition(self.topic, 0), timeout=5.0)
            
            if start_offset >= high_watermark:
                self.logger.info("Local data is already up to date with Kafka High Watermark.")
                return pl.DataFrame()
                
            self.logger.info(f"Replaying from offset {start_offset} to {high_watermark}...")
            
            tp.offset = start_offset
            self.consumer.assign([tp])
            self.consumer.seek(tp)
            
            ticks = []
            target_count = high_watermark - start_offset
            fetched = 0
            
            while fetched < target_count:
                msgs = self.consumer.consume(num_messages=2000, timeout=1.0)
                if not msgs:
                    self.logger.warning("Kafka poll timeout during history replay.")
                    break
                    
                for msg in msgs:
                    if msg.error():
                        continue
                    if msg.offset() >= high_watermark:
                        break
                        
                    t = Tick()
                    t.ParseFromString(msg.value())
                    
                    # Extra safety: Ensure we strictly filter out older ticks in case Kafka 
                    # returned a slightly older segment boundary offset
                    if t.timestamp_ms >= since_ts_ms:
                        ticks.append({
                            "ts": t.timestamp_ms,
                            "close": t.close,
                            "volume": t.volume,
                            "underlying_price": getattr(t, "underlying_price", t.close)
                        })
                    fetched += 1
                    
            self.logger.info(f"Gap Replay completed. Fetched {len(ticks)} matching ticks.")
            
            if not ticks:
                return pl.DataFrame()
                
            df = pl.DataFrame(ticks).sort("ts")
            
            # Prepare for live polling: move offset to high watermark
            tp.offset = high_watermark
            self.consumer.assign([tp])
            # Removed self.consumer.seek(tp) to prevent Erroneous state crash
            
            return df

    def poll_new_ticks(self) -> List[dict]:
        """
        Polls non-blocking for new live ticks.
        Thread-safe: Will politely wait if the UI thread is currently running `fetch_gap_ticks`.
        """
        with self.lock:
            if not self.consumer:
                return []
                
            msgs = self.consumer.consume(num_messages=500, timeout=0.01) # 10ms Non-blocking
            
        if not msgs:
            return []
            
        new_ticks = []
        for msg in msgs:
            if msg.error():
                continue
            t = Tick()
            t.ParseFromString(msg.value())
            new_ticks.append({
                "ts": t.timestamp_ms,
                "close": t.close,
                "volume": t.volume,
                "underlying_price": getattr(t, "underlying_price", t.close)
            })
            
        return new_ticks

    def close(self):
        with self.lock:
            if self.consumer:
                self.consumer.close()
                self.consumer = None
