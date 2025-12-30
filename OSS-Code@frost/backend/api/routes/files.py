from fastapi import APIRouter
from client.oss_client import OSSClient

router = APIRouter()
client = OSSClient()

@router.get("/")
def list_files():
    return client.list_files()

@router.get("/{file_id}")
def get_file(file_id: str):
    return client.get_file_info(file_id)
