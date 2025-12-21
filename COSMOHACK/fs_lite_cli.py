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

# Configuration
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
        print(f"✅ Initialized {self.num_nodes} satellite nodes")

    def upload_file(self, file_path: str, chunk_size: int = 1024, strategy: str = "round_robin", replication: int = 2):
        path = Path(file_path)
        if not path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")
        
        fname = path.name
        filesize = path.stat().st_size
        
        print(f"\n📤 Uploading {fname} ({format_bytes(filesize)})")
        start_time = time.time()
        
        chunks = []
        with open(path, "rb") as f:
            idx = 0
            while data := f.read(chunk_size):
                chunks.append((idx, data))
                idx += 1
        
        print(f"✂️  Split into {len(chunks)} chunks ({format_bytes(chunk_size)} each)")
        
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
        throughput = filesize / elapsed / (1024 * 1024)
        print(f"✅ Upload complete!")
        print(f"📊 Time: {elapsed:.2f}s | Throughput: {throughput:.2f} MB/s")
        print(f"📋 Strategy: {strategy} | Replication: {replication}x")

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
        
        print(f"\n📥 Downloading {file_name}")
        start_time = time.time()
        
        with open(out_path, "wb") as out_f:
            for i in range(meta["total_chunks"]):
                replicas = meta["chunks"][str(i)]
                chunk_data = None
                
                for r in replicas:
                    node_name = r["node"]
                    if not self.node_state.get(node_name, {}).get("online", True):
                        continue
                    
                    node = self._node_by_name(node_name)
                    if not node or not node.has_chunk(r["chunk_filename"]):
                        continue
                    
                    data = node.get_chunk(r["chunk_filename"])
                    if sha256_bytes(data) != r["hash"]:
                        print(f"⚠️  Integrity check failed for chunk {i} on {node_name}")
                        continue
                    
                    chunk_data = data
                    print(f"✓ Chunk {i} from {node_name}")
                    break
                
                if chunk_data is None:
                    raise RuntimeError(f"Cannot recover chunk {i} - all replicas failed")
                
                out_f.write(chunk_data)
        
        elapsed = time.time() - start_time
        filesize = out_path.stat().st_size
        throughput = filesize / elapsed / (1024 * 1024)
        
        print(f"✅ Download complete: {out_path}")
        print(f"📊 Time: {elapsed:.2f}s | Throughput: {throughput:.2f} MB/s")

    def _node_by_name(self, node_name: str):
        for n in self.nodes:
            if n.name == node_name:
                return n
        return None

    def node_offline(self, node_name: str):
        if node_name not in self.node_state:
            raise KeyError(f"Node not found: {node_name}")
        self.node_state[node_name]["online"] = False
        self._save_json(self.state_path, self.node_state)
        print(f"💔 {node_name} marked OFFLINE")

    def node_online(self, node_name: str):
        if node_name not in self.node_state:
            raise KeyError(f"Node not found: {node_name}")
        self.node_state[node_name]["online"] = True
        self._save_json(self.state_path, self.node_state)
        print(f"💚 {node_name} marked ONLINE")

    def show_status(self):
        print("\n" + "="*60)
        print("SYSTEM STATUS")
        print("="*60)
        
        online = sum(1 for s in self.node_state.values() if s.get("online", True))
        total = len(self.nodes)
        
        print(f"📊 Nodes: {online}/{total} online")
        print(f"📁 Files: {len(self.metadata)}")
        print(f"\nNode Details:")
        
        for n in self.nodes:
            state = self.node_state.get(n.name, {})
            status = "🟢 ONLINE" if state.get("online", True) else "🔴 OFFLINE"
            chunks = len(list(n.path.glob("*.chunk*"))) if n.path.exists() else 0
            print(f"  {n.name}: {status} | {chunks} chunks")

    def list_files(self):
        if not self.metadata:
            print("(no files uploaded)")
            return
        
        print("\n" + "="*60)
        print("UPLOADED FILES")
        print("="*60)
        for fname, meta in self.metadata.items():
            print(f"\n📄 {fname}")
            print(f"   Size: {format_bytes(meta['size'])}")
            print(f"   Chunks: {meta['total_chunks']}")
            print(f"   Strategy: {meta.get('strategy', 'N/A')}")
            print(f"   Replication: {meta.get('replication', 1)}x")

    def run_demo(self):
        """Automated demo"""
        print("="*60)
        print("FS-LITE AUTOMATED DEMO")
        print("="*60)
        
        # Step 1: Initialize
        print("\n🚀 Step 1: Initializing 8 nodes...")
        self.init_nodes(8)
        
        # Step 2: Create test file
        print("\n📄 Step 2: Creating test file...")
        test_file = Path("demo_test.txt")
        with open(test_file, "w") as f:
            f.write("Hello COSMEON! " * 100)
        print(f"✅ Created {test_file} ({format_bytes(test_file.stat().st_size)})")
        
        # Step 3: Upload
        print("\n📤 Step 3: Uploading with replication=2...")
        self.upload_file(str(test_file), chunk_size=50, strategy="round_robin", replication=2)
        
        # Step 4: Status
        print("\n📊 Step 4: System status...")
        self.show_status()
        
        # Step 5: Simulate failure
        print("\n⚠️  Step 5: Simulating node failure...")
        self.node_offline("sat_02")
        time.sleep(1)
        
        # Step 6: Download
        print("\n📥 Step 6: Downloading file (should work despite failure)...")
        self.download_file("demo_test.txt", "recovered_demo.txt")
        
        # Step 7: Verify
        print("\n✅ Step 7: Verifying integrity...")
        with open(test_file, "rb") as f1, open("recovered_demo.txt", "rb") as f2:
            if f1.read() == f2.read():
                print("🎉 FILE INTEGRITY VERIFIED!")
            else:
                print("❌ Integrity check failed")
        
        # Step 8: Summary
        print("\n" + "="*60)
        print("DEMO SUMMARY")
        print("="*60)
        print("✅ Initialized 8 nodes")
        print("✅ Uploaded file with replication")
        print("✅ Simulated node failure")
        print("✅ Successfully recovered file")
        print("✅ Integrity verified")
        print("="*60)

def build_parser():
    p = argparse.ArgumentParser(
        prog="fs_lite_cli",
        description="FS-Lite - Orbital Sharded Storage CLI"
    )
    sub = p.add_subparsers(dest="command", required=True)
    
    # demo
    sub.add_parser("demo", help="Run automated demo")
    
    # init-nodes
    sp_init = sub.add_parser("init-nodes", help="Initialize nodes")
    sp_init.add_argument("--count", "-c", type=int, default=8)
    
    # upload
    sp_upload = sub.add_parser("upload", help="Upload file")
    sp_upload.add_argument("file")
    sp_upload.add_argument("--chunk-size", "-s", type=int, default=1024)
    sp_upload.add_argument("--strategy", "-t", choices=["round_robin", "random", "hash"], default="round_robin")
    sp_upload.add_argument("--replication", "-r", type=int, default=2)
    
    # download
    sp_download = sub.add_parser("download", help="Download file")
    sp_download.add_argument("file_name")
    sp_download.add_argument("--out", "-o", required=True)
    
    # list
    sub.add_parser("list", help="List files")
    
    # status
    sub.add_parser("status", help="Show status")
    
    # node-offline
    sp_off = sub.add_parser("node-offline", help="Mark node offline")
    sp_off.add_argument("node_name")
    
    # node-online
    sp_on = sub.add_parser("node-online", help="Mark node online")
    sp_on.add_argument("node_name")
    
    return p

def main():
    parser = build_parser()
    args = parser.parse_args()
    
    # Determine node count
    nodes_count = 8
    if NODE_STATE_FILE.exists():
        try:
            with open(NODE_STATE_FILE) as f:
                st = json.load(f)
                nodes_count = len(st.keys()) or 8
        except:
            pass
    
    fs = FSLite(num_nodes=nodes_count)
    
    try:
        if args.command == "demo":
            fs.run_demo()
        elif args.command == "init-nodes":
            fs.init_nodes(args.count)
        elif args.command == "upload":
            fs.upload_file(args.file, args.chunk_size, args.strategy, args.replication)
        elif args.command == "download":
            fs.download_file(args.file_name, args.out)
        elif args.command == "list":
            fs.list_files()
        elif args.command == "status":
            fs.show_status()
        elif args.command == "node-offline":
            fs.node_offline(args.node_name)
        elif args.command == "node-online":
            fs.node_online(args.node_name)
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()