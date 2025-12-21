Set-Content -Path fs_lite_cli.py -Value @'
#!/usr/bin/env python3
"""
FS-Lite CLI - COSMEON Challenge Solution
Team: BASS Blaster
"""

import argparse
import sys
from pathlib import Path
import json
import hashlib
import shutil
import random
import time
from typing import List, Dict, Optional, Tuple
from datetime import datetime

BASE_DIR = Path.cwd() / "fs_lite_data"
NODES_DIR = BASE_DIR / "nodes"
METADATA_FILE = BASE_DIR / "metadata.json"
NODE_STATE_FILE = BASE_DIR / "nodes_state.json"

def ensure_dir(p: Path):
    p.mkdir(parents=True, exist_ok=True)

def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()

def format_bytes(size: int) -> str:
    for unit in ['B', 'KB', 'MB', 'GB']:
        if size < 1024.0:
            return f"{size:.2f} {unit}"
        size /= 1024.0
    return f"{size:.2f} TB"

class Node:
    def __init__(self, node_id: int, base_dir: Path):
        self.id = node_id
        self.name = f"sat_{node_id:02d}"
        self.path = base_dir / self.name
        ensure_dir(self.path)

    def put_chunk(self, chunk_filename: str, data: bytes):
        with open(self.path / chunk_filename, "wb") as f:
            f.write(data)

    def has_chunk(self, chunk_filename: str) -> bool:
        return (self.path / chunk_filename).exists()

    def get_chunk(self, chunk_filename: str) -> bytes:
        with open(self.path / chunk_filename, "rb") as f:
            return f.read()

class FSLite:
    def __init__(self, num_nodes: int = 8):
        self.base = BASE_DIR
        ensure_dir(self.base)
        ensure_dir(NODES_DIR)
        self.num_nodes = max(1, int(num_nodes))
        self.nodes: List[Node] = [Node(i + 1, NODES_DIR) for i in range(self.num_nodes)]
        self.metadata_path = METADATA_FILE
        self.state_path = NODE_STATE_FILE
        self.metadata = self._load_json(self.metadata_path) or {}
        self.node_state = self._load_json(self.state_path) or {n.name: {"online": True} for n in self.nodes}

    def _load_json(self, path: Path) -> Optional[Dict]:
        if path.exists():
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        return None

    def _save_json(self, path: Path, data: Dict):
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)

    def init_nodes(self, num_nodes: int):
        self.num_nodes = int(num_nodes)
        if NODES_DIR.exists():
            shutil.rmtree(NODES_DIR)
        ensure_dir(NODES_DIR)
        self.nodes = [Node(i + 1, NODES_DIR) for i in range(self.num_nodes)]
        self.node_state = {n.name: {"online": True} for n in self.nodes}
        self._save_json(self.state_path, self.node_state)
        print(f"[SUCCESS] Initialized {self.num_nodes} satellite nodes")

    def upload_file(self, file_path: str, chunk_size: int = 1024, strategy: str = "round_robin", replication: int = 2):
        path = Path(file_path)
        if not path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")
        
        fname = path.name
        filesize = path.stat().st_size
        
        print(f"\n[UPLOAD] {fname} ({format_bytes(filesize)})")
        start_time = time.time()
        
        chunks = []
        with open(path, "rb") as f:
            idx = 0
            while data := f.read(chunk_size):
                chunks.append((idx, data))
                idx += 1
        
        print(f"[SHARDING] Split into {len(chunks)} chunks ({format_bytes(chunk_size)} each)")
        
        file_meta = {
            "file_name": fname,
            "size": filesize,
            "chunk_size": chunk_size,
            "total_chunks": len(chunks),
            "strategy": strategy,
            "replication": replication,
            "chunks": {}
        }
        
        for chunk_idx, data in chunks:
            chunk_name = f"{fname}.chunk{chunk_idx}"
            chunk_hash = sha256_bytes(data)
            
            nodes_selected = self._select_nodes(chunk_idx, replication, strategy, fname)
            replicas = []
            
            for node in nodes_selected:
                node.put_chunk(chunk_name, data)
                replicas.append({
                    "node": node.name,
                    "chunk_filename": chunk_name,
                    "hash": chunk_hash
                })
            
            file_meta["chunks"][str(chunk_idx)] = replicas
        
        self.metadata[fname] = file_meta
        self._save_json(self.metadata_path, self.metadata)
        
        elapsed = time.time() - start_time
        throughput = filesize / elapsed / (1024 * 1024) if elapsed > 0 else 0
        print(f"[SUCCESS] Upload complete!")
        print(f"[METRICS] Time: {elapsed:.2f}s | Throughput: {throughput:.2f} MB/s")
        print(f"[CONFIG] Strategy: {strategy} | Replication: {replication}x")

    def _select_nodes(self, chunk_id: int, replication: int, strategy: str, filename: str) -> List[Node]:
        if replication > self.num_nodes:
            replication = self.num_nodes
        
        if strategy == "round_robin":
            start = chunk_id % self.num_nodes
            return [self.nodes[(start + i) % self.num_nodes] for i in range(replication)]
        elif strategy == "random":
            return random.sample(self.nodes, replication)
        elif strategy == "hash":
            key = f"{filename}:{chunk_id}".encode()
            digest = int(hashlib.sha256(key).hexdigest(), 16)
            return [self.nodes[(digest + i) % self.num_nodes] for i in range(replication)]
        else:
            return [self.nodes[chunk_id % self.num_nodes]]

    def download_file(self, file_name: str, out_path: str):
        if file_name not in self.metadata:
            raise FileNotFoundError(f"File not found in metadata: {file_name}")
        
        meta = self.metadata[file_name]
        out_path = Path(out_path)
        ensure_dir(out_path.parent)
        
        print(f"\n[DOWNLOAD] {file_name}")
        start_time = time.time()
        
        with open(out_path, "wb") as out_f:
            for i in range(meta["total_chunks"]):
                replicas = meta["chunks"][str(i)]
                chunk_data = None
                
                for r in replicas:
                    node_name = r["node"]
                    