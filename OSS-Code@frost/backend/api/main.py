from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from routes import upload, nodes, files

app = FastAPI(title="OSS Platform API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(upload.router, prefix="/upload")
app.include_router(nodes.router, prefix="/nodes")
app.include_router(files.router, prefix="/files")
