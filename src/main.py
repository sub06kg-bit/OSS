"""
Orbital Sharded Storage (OSS) - Main Entry Point
"""
import sys
import argparse
from pathlib import Path
import time

sys.path.insert(0, str(Path(__file__).parent))

from master.coordinator import MasterNode
from node.satellite import SatelliteNode
from utils.logger import setup_logger

logger = setup_logger(__name__)


def init_system(num_nodes=8, base_port=5000):
    """Initialize OSS with Master and Satellite nodes"""
    logger.info(f"Initializing OSS with {num_nodes} satellite nodes...")
    
    master = MasterNode(port=base_port)
    master.start()
    
    satellites = []
    for i in range(num_nodes):
        node = SatelliteNode(
            node_id=f"sat_{i:02d}",
            port=base_port + i + 1,
            master_url=f"http://localhost:{base_port}"
        )
        node.start()
        satellites.append(node)
        master.register_node(node.node_id, node.url)
    
    logger.info("✅ OSS System initialized successfully")
    return master, satellites


def main():
    parser = argparse.ArgumentParser(description='OSS - Orbital Sharded Storage')
    parser.add_argument('--nodes', type=int, default=8, help='Number of nodes')
    parser.add_argument('--mode', choices=['init', 'demo', 'test'], default='demo')
    args = parser.parse_args()
    
    if args.mode == 'init':
        master, satellites = init_system(num_nodes=args.nodes)
        logger.info("System ready. Press Ctrl+C to stop.")
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info("Shutting down...")
            master.stop()
            for sat in satellites:
                sat.stop()
    
    elif args.mode == 'demo':
        sys.path.insert(0, str(Path(__file__).parent.parent))
        from demos.demo_full import run_full_demo
        run_full_demo(num_nodes=args.nodes)
    
    elif args.mode == 'test':
        import pytest
        pytest.main(['-v', 'tests/'])


if __name__ == '__main__':
    main()

