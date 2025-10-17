import { useRef, useEffect } from 'react';
import * as d3 from 'd3';

function PieChartD3({ data, design }) {
  const svgRef = useRef();
  const tooltipRef = useRef();

  useEffect(() => {
    const width = design.width;
    const height = design.height;
    const radius = Math.min(width, height) / 2;

    const svg = d3.select(svgRef.current);
    svg.selectAll('*').remove();
    const tooltip = d3.select(tooltipRef.current);

    const chartGroup = svg
      .append('g')
      .attr('transform', `translate(${width / 2}, ${height / 2})`);

    const groupedByDataset = d3.group(data, d => d.dataset);

    const color = d3.scaleOrdinal(d3.schemeCategory10);

    Array.from(groupedByDataset.entries()).forEach(([dataset, entries], index) => {
      const pie = d3.pie().value(d => d.value)(entries);

      const arc = d3.arc()
        .innerRadius(2)
        .outerRadius(radius - index * 30);

      chartGroup.selectAll(`.arc-${dataset}`)
        .data(pie)
        .enter()
        .append('path')
        .attr('class', `arc-${dataset}`)
        .attr('d', arc)
        .attr('fill', (d, i) => color(i))
        .attr('stroke', 'white')
        .attr('stroke-width', 1)
        .on('mouseover', (event, d) => {
          tooltip
            .style('display', 'block')
            .html(`
              <strong>${design.xAxisField}:</strong> ${d.data[design.xAxisField]}<br/>
               <strong> ${d.data.dataset}:</strong> ${d.value}
            `);
        })
        .on('mousemove', (event) => {
          const bounds = svgRef.current.getBoundingClientRect();
          tooltip
            .style('left', `${event.clientX - bounds.left + 10}px`)
            .style('top', `${event.clientY - bounds.top - 30}px`);
        })
        .on('mouseout', () => {
          tooltip.style('display', 'none');
        });
    });

  }, [data, design]);

  return (
    <div className="relative">
      <div
        ref={tooltipRef}
        className="absolute pointer-events-none bg-white text-sm px-3 py-1 rounded shadow border z-10"
        style={{ display: 'none' }}
      />
      <svg ref={svgRef} width={design.width} height={design.height}></svg>
    </div>
  );
}

export default PieChartD3;












/*
// Normal data formate for pie chart
import { useRef, useEffect } from 'react';
import * as d3 from 'd3';

function PieChartD3({ data, design }) {
  const svgRef = useRef();
  const tooltipRef = useRef();

  useEffect(() => {
    const width = design.width;
    const height = design.height;
    const radius = Math.min(width, height) / 2;

    const svg = d3.select(svgRef.current);
    svg.selectAll('*').remove();
    const tooltip = d3.select(tooltipRef.current);

    const chartGroup = svg
      .append('g')
      .attr('transform', `translate(${width / 2}, ${height / 2})`);

    // Pie layout with values
    const pie = d3.pie().value(d => d.value)(data);

    // Arc generator
    const arc = d3.arc()
      .innerRadius(0)
      .outerRadius(radius);

    // Color scale
    const color = d3.scaleOrdinal(d3.schemeCategory10);

    // Draw the pie
    chartGroup.selectAll('path')
      .data(pie)
      .enter()
      .append('path')
      .attr('d', arc)
      .attr('fill', (d, i) => color(i))
      .attr('stroke', 'white')
      .attr('stroke-width', 1)
      .on('mouseover', (event, d) => {
        tooltip
          .style('display', 'block')
          .html(`
            <strong>${design.xAxisField}:</strong> ${d.data[design.xAxisField]}<br/>
            <strong>${d.data.dataset}</strong> ${d.data.value}
          `);
      })
      .on('mousemove', (event) => {
        const bounds = svgRef.current.getBoundingClientRect();
        tooltip
          .style('left', `${event.clientX - bounds.left + 10}px`)
          .style('top', `${event.clientY - bounds.top - 30}px`);
      })
      .on('mouseout', () => {
        tooltip.style('display', 'none');
      });

  }, [data, design]);

  return (
    <div className="relative">
      <div
        ref={tooltipRef}
        className="absolute pointer-events-none bg-white text-sm px-3 py-1 rounded shadow border z-10"
        style={{ display: 'none' }}
      />
      <svg ref={svgRef} width={design.width} height={design.height}></svg>
    </div>
  );
}

export default PieChartD3;
*/