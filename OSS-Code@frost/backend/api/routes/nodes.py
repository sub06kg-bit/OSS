from fastapi import APIRouter
import requests

router = APIRouter()

MASTER_URL = "http://master:5000"

@router.get("/health")
def node_health():
    return requests.get(f"{MASTER_URL}/status").json()
