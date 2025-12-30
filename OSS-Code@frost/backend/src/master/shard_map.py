"""Shard Map - SQLite metadata storage"""
import sqlite3
import threading
from typing import List, Dict
from pathlib import Path
from utils.logger import setup_logger

logger = setup_logger(__name__)


class ShardMap:
    def __init__(self, db_path='metadata/master.db'):
        self.db_path = db_path
        self.lock = threading.Lock()
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self._init_db()
    
    def _init_db(self):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS shards (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                file_id TEXT NOT NULL,
                shard_id INTEGER NOT NULL,
                node_id TEXT NOT NULL,
                checksum TEXT NOT NULL,
                size INTEGER NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(file_id, shard_id, node_id)
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS files (
                file_id TEXT PRIMARY KEY,
                original_name TEXT NOT NULL,
                total_size INTEGER NOT NULL,
                num_shards INTEGER NOT NULL,
                strategy TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_file_id ON shards(file_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_node_id ON shards(node_id)')
        
        conn.commit()
        conn.close()
        logger.info(f"💾 Initialized database: {self.db_path}")
    
    def register_file(self, file_id, original_name, total_size, num_shards, strategy):
        with self.lock:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute('''
                INSERT OR REPLACE INTO files VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ''', (file_id, original_name, total_size, num_shards, strategy))
            conn.commit()
            conn.close()
    
    def register_shard(self, file_id, shard_id, node_id, checksum, size):
        with self.lock:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute('''
                INSERT OR REPLACE INTO shards 
                (file_id, shard_id, node_id, checksum, size)
                VALUES (?, ?, ?, ?, ?)
            ''', (file_id, shard_id, node_id, checksum, size))
            conn.commit()
            conn.close()
    
    def get_shard_locations(self, file_id) -> List[Dict]:
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute('''
            SELECT shard_id, node_id, checksum, size
            FROM shards WHERE file_id = ? ORDER BY shard_id
        ''', (file_id,))
        rows = cursor.fetchall()
        conn.close()
        return [{'shard_id': r[0], 'node_id': r[1], 
                 'checksum': r[2], 'size': r[3]} for r in rows]
    
    def get_files_on_node(self, node_id) -> List[str]:
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute('SELECT DISTINCT file_id FROM shards WHERE node_id = ?', (node_id,))
        rows = cursor.fetchall()
        conn.close()
        return [r[0] for r in rows]
    
    def get_file_count(self) -> int:
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute('SELECT COUNT(*) FROM files')
        count = cursor.fetchone()[0]
        conn.close()
        return count
