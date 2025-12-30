"""Sharding tests"""
import pytest
import tempfile
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))
from sharding.engine import ShardingEngine


def test_shard_file():
    engine = ShardingEngine(shard_size=1024)
    with tempfile.NamedTemporaryFile(delete=False) as f:
        f.write(b'A' * 5000)
        test_file = f.name
    
    try:
        shards = engine.shard_file(test_file)
        assert len(shards) == 5
        for sid, data, checksum in shards:
            assert isinstance(sid, int)
            assert isinstance(data, bytes)
            assert len(checksum) == 64
    finally:
        Path(test_file).unlink()


def test_reconstruct():
    engine = ShardingEngine(shard_size=1024)
    with tempfile.NamedTemporaryFile(delete=False) as f:
        test_data = b'B' * 3000
        f.write(test_data)
        test_file = f.name
    
    try:
        shards = engine.shard_file(test_file)
        with tempfile.NamedTemporaryFile(delete=False) as f:
            output = f.name
        
        shards_no_checksum = [(s[0], s[1]) for s in shards]
        engine.reconstruct_file(shards_no_checksum, output)
        
        with open(test_file, 'rb') as f1, open(output, 'rb') as f2:
            assert f1.read() == f2.read()
        
        Path(output).unlink()
    finally:
        Path(test_file).unlink()

