import { useEffect, useState } from "react";
import { api } from "@/lib/api";
import { BarChart, Bar, XAxis, YAxis, Tooltip } from "recharts";

export default function NodeHealth() {
  const [nodes, setNodes] = useState<any[]>([]);

  useEffect(() => {
    api.get("/status").then(res => {
      const healthy = res.data.healthy_nodes;
      const failed = res.data.failed_nodes;
      setNodes([
        { name: "Healthy", count: healthy },
        { name: "Failed", count: failed },
      ]);
    });
  }, []);

  return (
    <>
      <h3>Node Health</h3>
      <BarChart width={300} height={200} data={nodes}>
        <XAxis dataKey="name" />
        <YAxis />
        <Tooltip />
        <Bar dataKey="count" fill="#4ade80" />
      </BarChart>
    </>
  );
}
