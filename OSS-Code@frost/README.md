# OSS - Orbital Sharded Storage

**Fault-Tolerant Distributed File System for Satellite Constellations**

[![Python](https://img.shields.io/badge/Python-3.8%2B-blue)](https://python.org)
[![License](https://img.shields.io/badge/License-MIT-green)](LICENSE)

Team: BASS Blaster | COSMEON Hiring Challenge - Problem Statement 3

---

## 🚀 Quick Start
```bash
# Clone repository
git clone https://github.com/sub06kg-bit/OSS.git
cd OSS

# Run automated demo (no installation needed!)
python fs_lite_cli.py demo
```

---

## 🧩 Key Capabilities

### Core Storage Engine
- File sharding (1MB default)
- SHA‑256 integrity verification
- Round‑Robin & Erasure Coding (6+3)
- Heartbeat‑based failure detection
- Automatic recovery hooks
- SQLite metadata (ACID)

### Platform Layer (New)
- API‑driven architecture
- OSS → Event → Metric pipeline
- Ready for user analytics & rewards

### Frontend Dashboard (New)
- File upload & download
- Node health visualization
- Shard‑to‑node graph (D3)
- Reconstruct & verify UI
- GSAP motion feedback

---

## 🏗️ Architecture

```txt
frontend (Next.js)
     ↓
platform API (FastAPI)
     ↓
OSS Core (Flask)
     ↓
Master → Satellite Nodes
     ↓
Local shard storage
🚀 Quick Start (Recommended – Docker)
bash
Copy code
docker compose up --build
Access:

UI → http://localhost:3000

API → http://localhost:8000

Master → http://localhost:5000/status

⚙️ Manual Run (Dev)
bash
Copy code
pip install -r requirements.txt
python src/main.py --mode demo
🧪 Testing
bash
Copy code
pytest -v --cov=src
📊 Performance (Localhost)
Operation	Throughput
Upload	~43 MB/s
Download	~37 MB/s
Recovery	~45 sec



## 📋 Features

✅ **Multiple Distribution Strategies**
- Round-Robin (default)
- Hash-based
- Random

✅ **Configurable Replication** (2-3x default)

✅ **Node Failure Simulation** with automatic recovery

✅ **SHA-256 Integrity Verification**

✅ **Persistent Metadata** (JSON-based)

✅ **Performance Metrics** (throughput tracking)

---

## 📖 Usage Examples

### Initialize System
```bash
python fs_lite_cli.py init-nodes --count 8
```

### Upload File
```bash
python fs_lite_cli.py upload myfile.txt --chunk-size 1024 --replication 2
```

### Simulate Node Failure
```bash
python fs_lite_cli.py node-offline sat_02
```

### Download File
```bash
python fs_lite_cli.py download myfile.txt --out recovered.txt
```

### Check System Status
```bash
python fs_lite_cli.py status
```

---

## 🎬 Demo Video

[▶️ Watch Demo on YouTube](https://www.youtube.com/watch?v=DT6ajf4hz-A&feature=youtu.be)

---

## 📊 Performance

Tested on Intel i7, 16GB RAM:

| Operation | Throughput |
|-----------|------------|
| Upload | 45 MB/s |
| Download | 40 MB/s |
| Recovery | 35 MB/s |

---

## 🏗️ Architecture
```
[Client]
    ↓
[Master/Coordinator]
    ↓
[Satellite Nodes: sat_01 to sat_08]
    ↓
[Local Storage]
```

### Key Components:
- **Sharding Engine**: Splits files into chunks
- **Distribution Logic**: Round-robin, hash, random
- **Metadata Manager**: Tracks chunk locations
- **Recovery System**: Handles node failures

---

## 📚 Research Foundation

Based on:
- Ghemawat et al. (2003): *Google File System*
- Plank & Xu (2006): *Reed-Solomon Erasure Coding*
- Karger et al. (1997): *Consistent Hashing*

---

## 🧪 Testing
```bash
# Run all commands
python fs_lite_cli.py demo
```

---

## 🤝 Team

- **Subhroto Deb Das** - debsubhroto@gmail.com
- **Bishu Kumar Srivastava** - bishusrivastav10@gmail.com
- **Atreya Biswas** - theultimate740@gmail.com
- **Subham Das** - sub06kg@gmail.com

---

## 📄 License

MIT License - see [LICENSE](LICENSE) file

---

## 🙏 Acknowledgments

- COSMEON Hiring Challenge
- Distributed Systems Research Community
- ISRO for orbital computing inspiration

---

## 📧 Contact

Questions? Open an issue or email: debsubhroto@gmail.com
