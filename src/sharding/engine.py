"""File sharding and reconstruction"""
import hashlib
from pathlib import Path
from typing import List, Tuple
from utils.logger import setup_logger

logger = setup_logger(__name__)


class ShardingEngine:
    def __init__(self, shard_size=1024*1024):
        self.shard_size = shard_size
    
    def shard_file(self, file_path: str) -> List[Tuple[int, bytes, str]]:
        path = Path(file_path)
        if not path.exists():
            raise FileNotFoundError(f"Not found: {path}")
        
        shards = []
        sid = 0
        
        with open(path, 'rb') as f:
            while chunk := f.read(self.shard_size):
                checksum = hashlib.sha256(chunk).hexdigest()
                shards.append((sid, chunk, checksum))
                sid += 1
        
        logger.info(f"✂️ Split {path.name} into {len(shards)} shards")
        return shards
    
    def reconstruct_file(self, shards: List[Tuple[int, bytes]], output: str):
        shards.sort(key=lambda x: x[0])
        path = Path(output)
        path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(path, 'wb') as f:
            for _, data in shards:
                f.write(data)
        
        logger.info(f"🔧 Reconstructed: {path}")
    
    def verify_shard(self, data: bytes, expected: str) -> bool:
        return hashlib.sha256(data).hexdigest() == expected
