"""Heartbeat Monitor - Failure detection"""
import time
import threading
from typing import Dict, Callable, Optional
from utils.logger import setup_logger

logger = setup_logger(__name__)


class HeartbeatMonitor:
    def __init__(self, interval=10, timeout=30):
        self.interval = interval
        self.timeout = timeout
        self.last_heartbeat: Dict[str, float] = {}
        self.lock = threading.Lock()
        self.running = False
        self.monitor_thread = None
        self.on_failure_callback: Optional[Callable] = None
    
    def register_node(self, node_id):
        with self.lock:
            self.last_heartbeat[node_id] = time.time()
    
    def update_heartbeat(self, node_id):
        with self.lock:
            self.last_heartbeat[node_id] = time.time()
    
    def is_healthy(self, node_id) -> bool:
        with self.lock:
            if node_id not in self.last_heartbeat:
                return False
            elapsed = time.time() - self.last_heartbeat[node_id]
            return elapsed < self.timeout
    
    def start(self, on_failure_callback: Optional[Callable] = None):
        self.running = True
        self.on_failure_callback = on_failure_callback
        self.monitor_thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self.monitor_thread.start()
        logger.info(f"💓 Heartbeat monitor started ({self.interval}s/{self.timeout}s)")
    
    def stop(self):
        self.running = False
        if self.monitor_thread:
            self.monitor_thread.join(timeout=2)
    
    def _monitor_loop(self):
        failed = set()
        while self.running:
            time.sleep(self.interval)
            with self.lock:
                now = time.time()
                for nid, last in self.last_heartbeat.items():
                    elapsed = now - last
                    if elapsed > self.timeout and nid not in failed:
                        logger.error(f"💔 Node {nid} failed ({elapsed:.1f}s)")
                        failed.add(nid)
                        if self.on_failure_callback:
                            self.on_failure_callback(nid)
                    elif elapsed < self.timeout and nid in failed:
                        logger.info(f"💚 Node {nid} recovered")
                        failed.remove(nid)