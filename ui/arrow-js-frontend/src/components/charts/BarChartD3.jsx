import { useEffect, useRef } from "react";
import * as d3 from "d3";

export default function BarChartD3({ data, design }) {
  const svgRef = useRef();
  const tooltipRef = useRef();

  useEffect(() => {
    const svg = d3.select(svgRef.current);
    svg.selectAll("*").remove();

    const tooltip = d3.select(tooltipRef.current);
    const svgWidth = design.width || 800;
    const svgHeight = design.height || 400;
    const margin = { top: 50, right: 50, bottom: 70, left: 60 };
    const width = svgWidth - margin.left - margin.right;
    const height = svgHeight - margin.top - margin.bottom;

    svg.attr("width", svgWidth).attr("height", svgHeight);

    const chartGroup = svg.append("g")
      .attr("transform", `translate(${margin.left}, ${margin.top})`);

    const xGroupField = design.xAxisField;
    const xSubgroupField = "dataset";  
    const yValueField = "value";

    const xGroups = [...new Set(data.map(d => d[xGroupField]))];
    const subGroups = [...new Set(data.map(d => d[xSubgroupField]))];

    const x0 = d3.scaleBand()
      .domain(xGroups)
      .range([0, width])
      .padding(0.2);

    const x1 = d3.scaleBand()
      .domain(subGroups)
      .range([0, x0.bandwidth()])
      .padding(0.05);

    const yMax = d3.max(data, d => d[yValueField]) || 0;
    const y = d3.scaleLinear()
      .domain([0, yMax])
      .nice()
      .range([height, 0]);

    const color = d3.scaleOrdinal(d3.schemeSet2);

    if (design.showGrid) {
      chartGroup.append("g")
        .attr("class", "grid")
        .call(d3.axisLeft(y).tickSize(-width).tickFormat(""))
        .selectAll(".grid line")
        .attr("stroke", "#e0e0e0")
        .attr("stroke-opacity", 0.7);
    }

    chartGroup.append("g").call(d3.axisLeft(y));
    chartGroup.append("text")
      .attr("x", -margin.left + 10)
      .attr("y", -10)
      .attr("fill", "black")
      .text(design.yAxisLabel);

    chartGroup.append("g")
      .attr("transform", `translate(0, ${height})`)
      .call(d3.axisBottom(x0));

    chartGroup.append("text")
      .attr("x", width / 2)
      .attr("y", height + 50)
      .attr("fill", "black")
      .text(design.xAxisLabel);

    const groupedData = d3.group(data, d => d[xGroupField]);

    chartGroup.selectAll("g.group")
      .data(xGroups)
      .enter()
      .append("g")
      .attr("transform", d => `translate(${x0(d)},0)`)
      .selectAll("rect")
      .data(d => groupedData.get(d))
      .enter()
      .append("rect")
      .attr("x", d => x1(d[xSubgroupField]))
      .attr("y", d => y(d[yValueField]))
      .attr("width", x1.bandwidth())
      .attr("height", d => height - y(d[yValueField]))
      .attr("fill", d => color(d[xSubgroupField]))
      .on("mouseover", (event, d) => {
        tooltip
          .style("display", "block")
          .html(`
            <strong>${xGroupField}:</strong> ${d[xGroupField]}<br/>
            <strong>${d.dataset}:</strong> ${d.value}
          `);
      })
      .on("mousemove", (event) => {
        const bounds = svgRef.current.getBoundingClientRect();
        tooltip
          .style("left", `${event.clientX - bounds.left + 10}px`)
          .style("top", `${event.clientY - bounds.top - 30}px`);
      })
      .on("mouseout", () => {
        tooltip.style("display", "none");
      });

  }, [data, design]);

  return (
    <div className="relative w-full">
      <div
        ref={tooltipRef}
        className="absolute pointer-events-none bg-white text-sm text-gray-800 px-3 py-1 rounded shadow border border-gray-300 z-10"
        style={{ display: "none" }}
      ></div>
      <svg ref={svgRef}></svg>
    </div>
  );
}
