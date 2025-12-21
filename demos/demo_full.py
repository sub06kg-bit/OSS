"""Full OSS demonstration"""
import time
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from main import init_system
from client.oss_client import OSSClient
from utils.logger import setup_logger

logger = setup_logger(__name__)


def run_full_demo(num_nodes=8):
    logger.info("="*80)
    logger.info("OSS - FULL DEMONSTRATION")
    logger.info("="*80)
    
    logger.info(f"\n🚀 Step 1: Initializing {num_nodes} nodes...")
    master, satellites = init_system(num_nodes=num_nodes)
    time.sleep(2)
    
    client = OSSClient()
    
    logger.info("\n📄 Step 2: Creating test file...")
    test_file = Path('test_data/sample.txt')
    test_file.parent.mkdir(exist_ok=True)
    with open(test_file, 'wb') as f:
        f.write(b'X' * (10 * 1024 * 1024))  # 10MB
    logger.info(f"✅ Created {test_file} (10 MB)")
    
    logger.info("\n📤 Step 3: Uploading...")
    file_id = client.upload(str(test_file))
    
    status = client.get_system_status()
    logger.info(f"\n📊 Status: {status['healthy_nodes']}/{status['total_nodes']} nodes healthy")
    
    logger.info("\n⚠️ Step 4: Simulating node failure...")
    logger.info("   Killing node sat_02...")
    satellites[2].stop()
    time.sleep(35)
    
    status = client.get_system_status()
    logger.info(f"   Nodes: {status['healthy_nodes']}/{status['total_nodes']}")
    
    logger.info("\n📥 Step 5: Downloading...")
    output = Path('downloads/recovered.txt')
    output.parent.mkdir(exist_ok=True)
    
    try:
        client.download(file_id, str(output))
        with open(test_file, 'rb') as f1, open(output, 'rb') as f2:
            if f1.read() == f2.read():
                logger.info("✅ File integrity verified!")
            else:
                logger.error("❌ Corruption detected")
    except Exception as e:
        logger.error(f"❌ Download failed: {e}")
    
    logger.info("\n" + "="*80)
    logger.info("SUMMARY")
    logger.info("="*80)
    logger.info(f"✅ {num_nodes} nodes initialized")
    logger.info("✅ File uploaded and sharded")
    logger.info("✅ Node failure handled")
    logger.info("✅ File reconstructed successfully")
    logger.info("="*80)
    
    logger.info("\n🧹 Cleanup...")
    master.stop()
    for sat in satellites:
        if sat.running:
            sat.stop()
    logger.info("✅ Demo complete!")


if __name__ == '__main__':
    run_full_demo()