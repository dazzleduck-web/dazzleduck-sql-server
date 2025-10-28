import LineChartD3 from "./charts/LineChartD3";
import AreaChartD3 from "./charts/AreaChartD3";
import BarChartD3 from "./charts/BarChartD3";
import PieChartD3 from "./charts/PieChartD3";

export default function DisplayCharts({ logs, view }) {
    if (!logs || logs.length === 0) {
        return <p className="text-gray-500 text-center p-6">No data to visualize.</p>;
    }

    const keys = Object.keys(logs[0] || {});
    const xKey = keys[0];
    let yKey = keys.find((k) => typeof logs[0][k] === "number");
    if (!yKey && keys.length > 1) yKey = keys[1];
    if (!yKey) yKey = keys[0]; 

    const groupKey = keys.find((k) => ["dataset", "group", "series"].includes(k)) || null;


    const chartData = [];
    if (groupKey) {
        logs.forEach((row) =>
            chartData.push({
                x: row[xKey],
                value: Number(row[yKey]) || 0,
                dataset: String(row[groupKey]),
                __raw: row,
            })
        );
    } else {
        logs.forEach((row, idx) =>
            chartData.push({
                x: row[xKey],
                value: Number(row[yKey]) || 0,
                dataset: "series",
                __raw: row,
            })
        );
    }

    const design = {
        width: 900,
        height: 360,
        xAxisField: "x",
        yAxisField: "value",
        xAxisLabel: xKey,
        yAxisLabel: yKey,
        showGrid: true,
        legendPosition: "top",
        fontFamily: "Inter, Helvetica, Arial",
        animation: true,
    };

    
    if (view === "pie") {
        const map = new Map();
        chartData.forEach((d) => {
            const key = d.x ?? "unknown";
            map.set(key, (map.get(key) || 0) + (Number(d.value) || 0));
        });
        const pieData = Array.from(map.entries()).map(([label, value]) => ({ label, value }));
        return <PieChartD3 data={pieData} design={{ width: 600, height: 400 }} />;
    }

    if (view === "line") return <LineChartD3 data={chartData} design={design} />;
    if (view === "area") return <AreaChartD3 data={chartData} design={design} />;
    if (view === "bar") return <BarChartD3 data={chartData} design={design} />;

    return <p className="text-gray-500">Unsupported visualization</p>;
}
