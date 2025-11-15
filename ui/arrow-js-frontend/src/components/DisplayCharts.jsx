import LineChartD3 from "./charts/LineChartD3";
import BarChartD3 from "./charts/BarChartD3";
import PieChartD3 from "./charts/PieChartD3";
import { formatPossibleDate } from "./utils/DateNormalizer";
/**
 * Universal chart display component
 * Supports Line, Bar, and Pie charts.
 * Auto-detects numeric, timestamp, and label columns.
 * 
 * Props:
 *  - logs: Array of objects (query results)
 *  - view: "line" | "bar" | "pie"
 *  - width (optional): number → chart width
 *  - height (optional): number → chart height
 */
export default function DisplayCharts({ logs, view, width, height }) {
  if (!logs || logs.length === 0)
    return <p className="text-gray-500 text-center p-6">No data to visualize.</p>;

  const keys = Object.keys(logs[0]);
  if (keys.length === 0)
    return <p className="text-gray-500">No valid data columns.</p>;

  // Use only first 3 columns for visualization (ignore the rest)
  const usedKeys = keys.slice(0, 3);
  const [col1, col2, col3] = usedKeys;

  // Base chart design (with optional width/height override)
  let chartData = [];
  let design = {
    width: width || 1200,
    height: height || 430,
    xAxisField: "x",
    yAxisField: "value",
    xAxisLabel: col1,
    yAxisLabel: col2,
    showGrid: true,
    legendPosition: "top-right",
    fontFamily: "Inter, Helvetica, Arial",
    animation: true,
  };

  // -------------------------------------------------
  // PIE CHART (always uses first 2 columns)
  // -------------------------------------------------
  if (view === "pie") {
    const [k1, k2] = keys.slice(0, 2); // only first two columns used

    // Helper: check if something is a timestamp
    const isTimestamp = (val) => {
      if (val == null) return false;
      if (typeof val === "string" && /^\d{10,16}$/.test(val)) return true;
      if (typeof val === "number" && val > 1000000000) return true;
      if (typeof val === "string" && /\d{4}-\d{2}-\d{2}/.test(val)) return true;
      return false;
    };

    // Helper: check if column is numeric (NOT timestamps!)
    const isNumericColumn = (key) => {
      return logs.every(row => {
        const v = row[key];
        if (v == null || v === "") return true;
        if (isTimestamp(v)) return false;
        return !isNaN(Number(v));
      });
    };

    let numericKey = null;
    let labelKey = null;

    const k1IsNum = isNumericColumn(k1);
    const k2IsNum = isNumericColumn(k2);

    if (k1IsNum && !k2IsNum) {
      numericKey = k1;
      labelKey = k2;
    } else if (k2IsNum && !k1IsNum) {
      numericKey = k2;
      labelKey = k1;
    } else {
      // fallback: use second as numeric
      numericKey = k2;
      labelKey = k1;
    }

    const pieData = logs.map(row => ({
      label: String(formatPossibleDate(row[labelKey])),
      value: Number(row[numericKey]) || 0,
    }));

    return (
      <PieChartD3
        data={pieData}
        design={{
          width: width || 600,
          height: height || 400,
        }}
      />
    );
  }

  // -------------------------------------------------
  // LINE / BAR charts (3 columns or less)
  // -------------------------------------------------
  const xKey = col1;
  let yKey = col2;
  let zKey = col3;

  // Between col2 & col3 → detect numeric for Y axis
  const sample = logs.find(r => r[col2] !== undefined && r[col3] !== undefined);
  if (sample) {
    const isCol2Numeric = !isNaN(Number(sample[col2]));
    const isCol3Numeric = !isNaN(Number(sample[col3]));
    if (isCol3Numeric && !isCol2Numeric) {
      yKey = col3;
      zKey = col2;
    }
  }

  // Normalize data for D3 charts
  chartData = logs.map(row => {
    const xValue = formatPossibleDate(row[xKey]);
    const yValue = Number(row[yKey]) || 0;
    const zValue = zKey ? String(formatPossibleDate(row[zKey])) : "Series";

    return {
      x: xValue,
      value: yValue,
      dataset: zValue,
      label: zValue,
    };
  });

  // Update chart design
  design = {
    ...design,
    width: width || design.width,
    height: height || design.height,
    xAxisField: "x",
    yAxisField: "value",
    xAxisLabel: xKey,
    yAxisLabel: yKey,
    legendPosition: "top-right",
  };

  // -------------------------------------------------
  // Render chart
  // -------------------------------------------------
  if (view === "line") return <LineChartD3 data={chartData} design={design} />;
  if (view === "bar") return <BarChartD3 data={chartData} design={design} />;
  if (view === "pie")
    return <p className="text-gray-500">Pie chart requires at least 2 columns.</p>;

  return <p className="text-gray-500">Unsupported visualization</p>;
}
