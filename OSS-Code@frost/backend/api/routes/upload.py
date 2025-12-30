from fastapi import APIRouter, UploadFile
from client.oss_client import OSSClient

router = APIRouter()
client = OSSClient()

@router.post("/")
def upload_file(file: UploadFile):
    path = f"/tmp/{file.filename}"
    with open(path, "wb") as f:
        f.write(file.file.read())

    file_id = client.upload(path)
    return {"file_id": file_id}
