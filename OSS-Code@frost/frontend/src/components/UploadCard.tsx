import { useState } from "react";
import { api } from "@/lib/api";

export default function UploadCard() {
  const [file, setFile] = useState<File | null>(null);
  const [status, setStatus] = useState<string>("");

  async function upload() {
    if (!file) return;

    const form = new FormData();
    form.append("file", file);

    setStatus("Uploading...");
    await api.post("/upload", form);
    setStatus("Uploaded ✅");
  }

  return (
    <div>
      <h3>Upload File</h3>
      <input type="file" onChange={e => setFile(e.target.files?.[0] || null)} />
      <button onClick={upload}>Upload</button>
      <p>{status}</p>
    </div>
  );
}
