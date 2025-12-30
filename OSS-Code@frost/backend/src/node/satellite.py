"""Satellite Node - Storage node"""
import hashlib
import time
import threading
import requests
from flask import Flask, request, jsonify, send_file
from pathlib import Path
from utils.logger import setup_logger

logger = setup_logger(__name__)


class SatelliteNode:
    def __init__(self, node_id, port, master_url, storage_dir='storage'):
        self.node_id = node_id
        self.port = port
        self.master_url = master_url
        self.storage_dir = Path(storage_dir) / node_id
        self.storage_dir.mkdir(parents=True, exist_ok=True)
        self.app = Flask(__name__)
        self.url = f"http://localhost:{port}"
        self.running = False
        self._setup_routes()
    
    def _setup_routes(self):
        @self.app.route('/store', methods=['POST'])
        def store():
            try:
                shard = request.files['shard']
                fid = request.form['file_id']
                sid = int(request.form['shard_id'])
                expected = request.form['checksum']
                
                data = shard.read()
                actual = hashlib.sha256(data).hexdigest()
                
                if actual != expected:
                    return jsonify({'error': 'Checksum mismatch'}), 400
                
                path = self.storage_dir / f"{fid}_shard_{sid}.dat"
                with open(path, 'wb') as f:
                    f.write(data)
                
                logger.info(f"💾 {self.node_id}: Stored shard {sid} for {fid}")
                return jsonify({'status': 'stored', 'node_id': self.node_id, 
                              'size': len(data), 'checksum': actual})
            except Exception as e:
                logger.error(f"❌ {self.node_id}: {e}")
                return jsonify({'error': str(e)}), 500
        
        @self.app.route('/retrieve/<file_id>/<int:shard_id>', methods=['GET'])
        def retrieve(file_id, shard_id):
            try:
                path = self.storage_dir / f"{file_id}_shard_{shard_id}.dat"
                if not path.exists():
                    return jsonify({'error': 'Not found'}), 404
                logger.info(f"📤 {self.node_id}: Sending shard {shard_id}")
                return send_file(path, as_attachment=True)
            except Exception as e:
                return jsonify({'error': str(e)}), 500
        
        @self.app.route('/status', methods=['GET'])
        def status():
            shards = len(list(self.storage_dir.glob('*.dat')))
            size = sum(f.stat().st_size for f in self.storage_dir.glob('*.dat'))
            return jsonify({'node_id': self.node_id, 'shards': shards, 
                          'size_mb': size/(1024*1024)})
    
    def start(self):
        self.running = True
        self.server_thread = threading.Thread(
            target=lambda: self.app.run(host='0.0.0.0', port=self.port, 
                                       threaded=True, use_reloader=False),
            daemon=True
        )
        self.server_thread.start()
        self.heartbeat_thread = threading.Thread(target=self._heartbeat, daemon=True)
        self.heartbeat_thread.start()
        logger.info(f"🛰️ {self.node_id} started on port {self.port}")
        time.sleep(0.3)
    
    def stop(self):
        self.running = False
        logger.info(f"🛑 {self.node_id} stopped")
    
    def _heartbeat(self):
        while self.running:
            try:
                requests.post(f"{self.master_url}/heartbeat", 
                            json={'node_id': self.node_id}, timeout=2)
            except:
                pass
            time.sleep(10)
