import { useRef, useEffect } from 'react';
import * as d3 from 'd3';

function AreaChartD3({ data, design }) {
  const svgRef = useRef();
  const tooltipRef = useRef();

  useEffect(() => {
    const width = design.width;
    const height = design.height;
    const margin = { top: 50, right: 50, bottom: 50, left: 60 };

    const svgElement = d3.select(svgRef.current);
    svgElement.selectAll('*').remove();
    svgElement.attr('width', width).attr('height', height);

    const tooltip = d3.select(tooltipRef.current);
    const innerWidth = width - margin.left - margin.right;
    const innerHeight = height - margin.top - margin.bottom;

    const chartGroup = svgElement
      .append('g')
      .attr('transform', `translate(${margin.left},${margin.top})`);

    const groupedByDataset = d3.group(data, d => d.dataset);

    const xField = design.xAxisField;
    const xDomain = [...new Set(data.map(d => d[xField]))];
    const xScale = d3.scalePoint()
      .domain(xDomain)
      .range([0, innerWidth])
      .padding(0.5);

    const yMax = d3.max(data, d => d.value) || 0;
    const yScale = d3.scaleLinear()
      .domain([0, yMax])
      .nice()
      .range([innerHeight, 0]);

    // Optional grid
    if (design.showGrid) {
      chartGroup.append('g')
        .attr('class', 'grid')
        .call(d3.axisLeft(yScale).tickSize(-innerWidth).tickFormat(''));
    }

    // Axes
    chartGroup.append('g')
      .attr('transform', `translate(0,${innerHeight})`)
      .call(d3.axisBottom(xScale));

    chartGroup.append('g').call(d3.axisLeft(yScale));

    const color = d3.scaleOrdinal(d3.schemeTableau10);

    // Plot one area per dataset
    Array.from(groupedByDataset.entries()).forEach(([dataset, values], idx) => {
      const area = d3.area()
        .x(d => xScale(d[xField]))
        .y0(innerHeight)
        .y1(d => yScale(d.value))
        .curve(d3.curveMonotoneX);

      chartGroup.append('path')
        .datum(values)
        .attr('fill', color(idx))
        .attr('fill-opacity', 0.5)
        .attr('stroke', color(idx))
        .attr('stroke-width', 1.5)
        .attr('d', area);

      chartGroup.selectAll(`.dot-${dataset}`)
        .data(values)
        .enter()
        .append('circle')
        .attr('class', `dot-${dataset}`)
        .attr('cx', d => xScale(d[xField]))
        .attr('cy', d => yScale(d.value))
        .attr('r', 4)
        .attr('fill', color(idx))
        .on('mouseover', (event, d) => {
          tooltip
            .style('display', 'block')
            .html(`
              <strong>${design.xAxisLabel}:</strong> ${d[xField]}<br/>
              <strong>${d.dataset}:</strong> ${d.value}<br/>
            `);
        })
        .on('mousemove', (event) => {
          const bounds = svgRef.current.getBoundingClientRect();
          tooltip
            .style('left', `${event.clientX - bounds.left + 10}px`)
            .style('top', `${event.clientY - bounds.top + 10}px`);
        })
        .on('mouseout', () => tooltip.style('display', 'none'));
    });

    // Axis Labels
    chartGroup.append('text')
      .attr('x', innerWidth / 2)
      .attr('y', innerHeight + 40)
      .attr('text-anchor', 'middle')
      .text(design.xAxisLabel);

    chartGroup.append('text')
      .attr('x', -40)
      .attr('y', -20)
      .text(design.yAxisLabel);
  }, [data, design]);

  return (
    <div className="relative">
      <div
        ref={tooltipRef}
        className="absolute pointer-events-none bg-white text-sm px-3 py-1 rounded shadow border z-10"
        style={{ display: 'none' }}
      />
      <svg ref={svgRef}></svg>
    </div>
  );
}

export default AreaChartD3;
