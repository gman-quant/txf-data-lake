import logging
import polars as pl
import threading
from confluent_kafka import Consumer, TopicPartition
from typing import Optional, List, Union
from core.data_schemas.txf_data_pb2 import Tick

class KafkaTickReader:
    """
    Kafka consumer dedicated to fetching live and historical ticks for the charting engine.
    This implementation uses a strict threading.RLock() to prevent `Erroneous state` exceptions
    when the UI thread queries gap history concurrently with the background thread polling live ticks.
    """
    def __init__(self, broker_url: str = "192.168.1.50:9092", topics: Union[str, List[str]] = "txf-tick", group_id: str = "txf_chart_live_v2"):
        self.broker_url = broker_url
        self.topics = [topics] if isinstance(topics, str) else topics
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
        self.logger.info(f"Connected to Kafka broker: {self.broker_url}, Topics: {self.topics}")

    def fetch_gap_ticks(self, since_ts_ms: int) -> dict:
        """
        Fetches all historical ticks from since_ts_ms to the current High Watermark for all topics.
        Returns a dictionary mapping topic name to its historical pl.DataFrame.
        """
        with self.lock:
            if not self.consumer:
                raise RuntimeError("Kafka consumer not connected.")
            
            self.logger.info(f"Gap Replay: Fetching history from timestamp {since_ts_ms} for topics {self.topics}...")
            results = {}
            
            for topic in self.topics:
                tp = TopicPartition(topic, 0)
                tp.offset = since_ts_ms
                
                offsets_found = self.consumer.offsets_for_times([tp], timeout=10.0)
                start_offset = offsets_found[0].offset if offsets_found and offsets_found[0].offset != -1 else -1
                
                if start_offset == -1:
                    results[topic] = pl.DataFrame()
                    continue
                    
                _, high_watermark = self.consumer.get_watermark_offsets(TopicPartition(topic, 0), timeout=5.0)
                
                if start_offset >= high_watermark:
                    results[topic] = pl.DataFrame()
                    continue
                    
                tp.offset = start_offset
                self.consumer.assign([tp])
                self.consumer.seek(tp)
                
                ticks = []
                target_count = high_watermark - start_offset
                fetched = 0
                
                while fetched < target_count:
                    msgs = self.consumer.consume(num_messages=2000, timeout=1.0)
                    if not msgs:
                        break
                        
                    for msg in msgs:
                        if msg.error(): continue
                        if msg.offset() >= high_watermark: break
                            
                        t = Tick()
                        t.ParseFromString(msg.value())
                        
                        if t.timestamp_ms >= since_ts_ms:
                            ticks.append({
                                "ts": t.timestamp_ms,
                                "close": t.close / 10000.0,
                                "volume": t.volume,
                                "underlying_price": getattr(t, "underlying_price", t.close) / 10000.0
                            })
                        fetched += 1
                        
                # 恢復為 live listening mode
                tp.offset = high_watermark
                self.consumer.assign([tp])
                
                if ticks:
                    results[topic] = pl.DataFrame(ticks).sort("ts")
                else:
                    results[topic] = pl.DataFrame()
                    
            # 全部抓完後，將 consumer 訂閱所有 topics
            self.consumer.subscribe(self.topics)
            return results

    def poll_new_ticks(self) -> dict:
        """
        Polls non-blocking for new live ticks.
        Returns a dictionary mapping topic name to a list of dict ticks.
        """
        with self.lock:
            if not self.consumer:
                return {}
                
            msgs = self.consumer.consume(num_messages=500, timeout=0.01) # 10ms Non-blocking
            
        if not msgs:
            return {}
            
        results = {}
        for msg in msgs:
            if msg.error():
                continue
            
            topic = msg.topic()
            t = Tick()
            t.ParseFromString(msg.value())
            
            if topic not in results:
                results[topic] = []
                
            results[topic].append({
                "ts": t.timestamp_ms,
                "close": t.close / 10000.0,
                "volume": t.volume,
                "underlying_price": getattr(t, "underlying_price", t.close) / 10000.0
            })
            
        return results

    def close(self):
        with self.lock:
            if self.consumer:
                self.consumer.close()
                self.consumer = None
