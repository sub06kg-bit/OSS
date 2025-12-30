import UploadCard from "@/components/UploadCard";
import NodeHealth from "@/components/NodeHealth";
import FileList from "@/components/FileList";
import ShardMap from "@/components/ShardMap";

export default function Home() {
  return (
    <main style={{ padding: 32 }}>
      <h1>OSS Dashboard</h1>
      <UploadCard />
      <NodeHealth />
      <ShardMap />
      <FileList />
    </main>
  );
}
