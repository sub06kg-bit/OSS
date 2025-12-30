"""Master Node - Central coordinator"""
import time
import threading
from flask import Flask, request, jsonify
from typing import Dict

from master.shard_map import ShardMap
from master.heartbeat import HeartbeatMonitor
from distribution.strategies import RoundRobinStrategy, ErasureCodingStrategy
from utils.logger import setup_logger

logger = setup_logger(__name__)


class MasterNode:
    def __init__(self, port=5000, db_path='metadata/master.db'):
        self.port = port
        self.app = Flask(__name__)
        self.shard_map = ShardMap(db_path)
        self.heartbeat = HeartbeatMonitor(interval=10, timeout=30)
        self.nodes: Dict[str, str] = {}
        self.lock = threading.Lock()
        
        self.round_robin = RoundRobinStrategy()
        self.erasure_coding = ErasureCodingStrategy(k=6, m=3)
        
        self._setup_routes()
        self.server_thread = None
    
    def _setup_routes(self):
        @self.app.route('/assign_shards', methods=['POST'])
        def assign_shards():
            data = request.json
            file_size = data['file_size']
            num_shards = data['num_shards']
            
            strategy = self._select_strategy(file_size)
            
            with self.lock:
                available = [n for n in self.nodes if self.heartbeat.is_healthy(n)]
                if len(available) < 3:
                    return jsonify({'error': 'Insufficient nodes'}), 503
                assignments = strategy.assign(num_shards, available)
            
            logger.info(f"📋 Assigned {num_shards} shards using {strategy.name}")
            return jsonify({'assignments': assignments, 'strategy': strategy.name})
        
        @self.app.route('/register_shard', methods=['POST'])
        def register_shard():
            data = request.json
            self.shard_map.register_shard(**data)
            return jsonify({'status': 'ok'})
        
        @self.app.route('/register_file', methods=['POST'])
        def register_file():
            data = request.json
            self.shard_map.register_file(**data)
            return jsonify({'status': 'ok'})
        
        @self.app.route('/get_shard_locations', methods=['POST'])
        def get_locations():
            file_id = request.json['file_id']
            locations = self.shard_map.get_shard_locations(file_id)
            if not locations:
                return jsonify({'error': 'Not found'}), 404
            return jsonify({'locations': locations})
        
        @self.app.route('/heartbeat', methods=['POST'])
        def heartbeat():
            node_id = request.json['node_id']
            self.heartbeat.update_heartbeat(node_id)
            return jsonify({'status': 'ok'})
        
        @self.app.route('/status', methods=['GET'])
        def status():
            with self.lock:
                healthy = [n for n in self.nodes if self.heartbeat.is_healthy(n)]
                failed = [n for n in self.nodes if not self.heartbeat.is_healthy(n)]
            return jsonify({
                'total_nodes': len(self.nodes),
                'healthy_nodes': len(healthy),
                'failed_nodes': len(failed),
                'failed_node_ids': failed,
                'total_files': self.shard_map.get_file_count()
            })
    
    def _select_strategy(self, file_size):
        return self.round_robin if file_size < 10*1024*1024 else self.erasure_coding
    
    def start(self):
        self.server_thread = threading.Thread(
            target=lambda: self.app.run(host='0.0.0.0', port=self.port, 
                                        threaded=True, use_reloader=False),
            daemon=True
        )
        self.server_thread.start()
        self.heartbeat.start(on_failure_callback=self._handle_failure)
        logger.info(f"🚀 Master Node started on port {self.port}")
        time.sleep(0.5)
    
    def stop(self):
        self.heartbeat.stop()
        logger.info("🛑 Master stopped")
    
    def _handle_failure(self, node_id):
        logger.warning(f"⚠️ Node failure: {node_id}")
        affected = self.shard_map.get_files_on_node(node_id)
        for fid in affected:
            logger.info(f"🔧 Recovery needed: {fid}")
    
    def register_node(self, node_id, url):
        with self.lock:
            self.nodes[node_id] = url
            self.heartbeat.register_node(node_id)
