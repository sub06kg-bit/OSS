import { useEffect, useState } from "react";
import { api } from "@/lib/api";

export default function FileList() {
  const [files, setFiles] = useState<any[]>([]);

  useEffect(() => {
    api.get("/files").then(r => setFiles(r.data.files));
  }, []);

  return (
    <>
      <h3>Stored Files</h3>
      <ul>
        {files.map(f => (
          <li key={f.file_id}>
            {f.file_id}
            <button
              onClick={() =>
                window.open(
                  `${process.env.NEXT_PUBLIC_API_BASE}/download/${f.file_id}`
                )
              }
            >
              Download
            </button>
          </li>
        ))}
      </ul>
    </>
  );
}
