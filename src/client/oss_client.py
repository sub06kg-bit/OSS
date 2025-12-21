"""OSS Client - User interface"""
import time
import requests
import uuid
from pathlib import Path
from sharding.engine import ShardingEngine
from utils.logger import setup_logger

logger = setup_logger(__name__)


class OSSClient:
    def __init__(self, master_url='http://localhost:5000'):
        self.master_url = master_url
        self.engine = ShardingEngine()
    
    def upload(self, file_path: str) -> str:
        start = time.time()
        path = Path(file_path)
        if not path.exists():
            raise FileNotFoundError(f"Not found: {path}")
        
        file_id = f"{path.stem}_{uuid.uuid4().hex[:8]}"
        size = path.stat().st_size
        logger.info(f"📤 Uploading {path.name} ({size/(1024*1024):.2f} MB)")
        
        shards = self.engine.shard_file(file_path)
        
        resp = requests.post(f"{self.master_url}/assign_shards", 
                           json={'file_id': file_id, 'num_shards': len(shards), 
                                'file_size': size})
        if resp.status_code != 200:
            raise Exception(f"Assignment failed: {resp.text}")
        
        assignments = resp.json()['assignments']
        strategy = resp.json()['strategy']
        logger.info(f"📋 Strategy: {strategy}")
        
        for sid, data, checksum in shards:
            nid = assignments[str(sid)]
            nurl = self._node_url(nid)
            self._upload_shard(nurl, file_id, sid, data, checksum)
            requests.post(f"{self.master_url}/register_shard", 
                        json={'file_id': file_id, 'shard_id': sid, 
                             'node_id': nid, 'checksum': checksum, 'size': len(data)})
        
        requests.post(f"{self.master_url}/register_file",
                    json={'file_id': file_id, 'original_name': path.name,
                         'total_size': size, 'num_shards': len(shards), 
                         'strategy': strategy})
        
        elapsed = time.time() - start
        throughput = size / elapsed / (1024*1024)
        logger.info(f"✅ Uploaded: {file_id}")
        logger.info(f"📊 {elapsed:.2f}s | {throughput:.2f} MB/s")
        return file_id
    
    def download(self, file_id: str, output: str):
        start = time.time()
        logger.info(f"📥 Downloading {file_id}")
        
        resp = requests.post(f"{self.master_url}/get_shard_locations",
                           json={'file_id': file_id})
        if resp.status_code != 200:
            raise Exception(f"Not found: {file_id}")
        
        locations = resp.json()['locations']
        shards = []
        
        for loc in locations:
            sid = loc['shard_id']
            nid = loc['node_id']
            checksum = loc['checksum']
            nurl = self._node_url(nid)
            data = self._download_shard(nurl, file_id, sid)
            
            if not self.engine.verify_shard(data, checksum):
                raise Exception(f"Checksum fail: shard {sid}")
            
            shards.append((sid, data))
        
        self.engine.reconstruct_file(shards, output)
        
        elapsed = time.time() - start
        size = Path(output).stat().st_size
        throughput = size / elapsed / (1024*1024)
        logger.info(f"✅ Downloaded: {output}")
        logger.info(f"📊 {elapsed:.2f}s | {throughput:.2f} MB/s")
    
    def _node_url(self, node_id: str) -> str:
        num = int(node_id.split('_')[1])
        return f"http://localhost:{5001 + num}"
    
    def _upload_shard(self, url, fid, sid, data, checksum):
        resp = requests.post(f"{url}/store", 
                           files={'shard': ('shard.dat', data)},
                           data={'file_id': fid, 'shard_id': sid, 'checksum': checksum})
        if resp.status_code != 200:
            raise Exception(f"Upload fail: {resp.text}")
    
    def _download_shard(self, url, fid, sid) -> bytes:
        resp = requests.get(f"{url}/retrieve/{fid}/{sid}")
        if resp.status_code != 200:
            raise Exception(f"Download fail: {resp.text}")
        return resp.content
    
    def get_system_status(self) -> dict:
        return requests.get(f"{self.master_url}/status").json()
