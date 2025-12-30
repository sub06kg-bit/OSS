import { useEffect, useRef } from "react";
import * as d3 from "d3";
import { api } from "@/lib/api";

export default function ShardMap() {
  const ref = useRef<SVGSVGElement>(null);

  useEffect(() => {
    api.get("/shard-map").then(res => {
      const data = res.data;

      const svg = d3.select(ref.current);
      svg.selectAll("*").remove();

      const nodes = data.nodes;
      const links = data.links;

      const sim = d3.forceSimulation(nodes)
        .force("link", d3.forceLink(links).id(d => d.id))
        .force("charge", d3.forceManyBody())
        .force("center", d3.forceCenter(200, 150));

      const link = svg.selectAll("line")
        .data(links)
        .enter()
        .append("line")
        .attr("stroke", "#999");

      const node = svg.selectAll("circle")
        .data(nodes)
        .enter()
        .append("circle")
        .attr("r", 6)
        .attr("fill", "#60a5fa");

      sim.on("tick", () => {
        link
          .attr("x1", d => d.source.x)
          .attr("y1", d => d.source.y)
          .attr("x2", d => d.target.x)
          .attr("y2", d => d.target.y);

        node
          .attr("cx", d => d.x)
          .attr("cy", d => d.y);
      });
    });
  }, []);

  return (
    <>
      <h3>Shard Distribution</h3>
      <svg ref={ref} width={400} height={300} />
    </>
  );
}
