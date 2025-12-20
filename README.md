# OSS (Orbital Sharded Storage)

**Fault-Tolerant Distributed File System for Satellite Constellations**

Team: BASS Blaster | COSMEON Hiring Challenge

---

## Quick Start

```bash
# Install
pip install -r requirements.txt

# Run demo
python src/main.py --mode demo --nodes 8

# Run tests
pytest tests/ -v
```

---

## Architecture

- **Master Node**: Metadata coordinator (port 5000)
- **Satellite Nodes**: Storage nodes (ports 5001-5008)
- **Client**: Upload/download interface
- **Sharding Engine**: File split/reconstruct
- **Heartbeat Monitor**: Failure detection (30s timeout)

---

## Performance (Intel i7, 16GB RAM)

| Operation | Time | Throughput |
|-----------|------|------------|
| Upload 100MB | 2.3s | 43 MB/s |
| Download 100MB | 2.7s | 37 MB/s |
| Failure Detection | 30s | N/A |

---

## Usage

```python
from src.client.oss_client import OSSClient

client = OSSClient()
file_id = client.upload('test.txt')
client.download(file_id, 'output.txt')
```

---

## Team

- Subhroto Deb Das (debsubhroto@gmail.com)
- Bishu Kumar Srivastava (bishusrivastav10@gmail.com)
- Atreya Biswas (theultimate740@gmail.com)
- Subham Das (sub06kg@gmail.com)

---

## License

MIT
