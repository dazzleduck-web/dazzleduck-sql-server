import React from "react";
import DisplayCharts from "../DisplayCharts";
import { formatPossibleDate } from "../utils/DateNormalizer";

const QueryResults = ({ logs, loading, error, view, isConnected }) => {
    if (loading) {
        return (
            <div className="border overflow-auto max-h-[450px] bg-white rounded-lg scrollbar-custom">
                <p className="text-gray-500 text-center p-10">Loading results...</p>
            </div>
        );
    }

    if (error) {
        return (
            <div className="border overflow-auto max-h-[450px] bg-white rounded-lg scrollbar-custom">
                <pre className="text-red-600 text-center font-medium p-5 whitespace-pre-wrap break-all text-md">
                    {error}
                </pre>
            </div>
        );
    }

    if (logs.length === 0) {
        return (
            <div className="border overflow-auto max-h-[450px] bg-white rounded-lg scrollbar-custom">
                {!isConnected ? (
                    <p className="text-gray-700 text-center p-10">
                        Connect first to run queries. Click <b>Connect</b> in the Connection Settings.
                    </p>
                ) : (
                    <p className="text-gray-500 text-center p-10">
                        No results yet. Run a query to view result data.
                    </p>
                )}
            </div>
        );
    }

    if (view === "table") {
        return (
            <div className="border overflow-auto max-h-[450px] bg-white rounded-lg scrollbar-custom">
                <table className="w-full text-sm text-left border-separate border-spacing-0">
                    <thead className="sticky top-0 z-10 shadow-md custom-font">
                        <tr className="bg-blue-100 text-gray-800">
                            {Object.keys(logs[0]).map((key) => (
                                <th
                                    key={key}
                                    className="p-2 border border-gray-400 capitalize text-center"
                                >
                                    {key}
                                </th>
                            ))}
                        </tr>
                    </thead>
                    <tbody>
                        {logs.map((r, i) => (
                            <tr
                                key={i}
                                className="hover:bg-gray-100 transition duration-300"
                            >
                                {Object.entries(r).map(([key, val], j) => (
                                    <td
                                        key={j}
                                        className="p-2 border border-gray-300 text-center"
                                    >
                                        {String(formatPossibleDate(val))}
                                    </td>
                                ))}
                            </tr>
                        ))}
                    </tbody>
                </table>
            </div>
        );
    }

    return (
        <div className="border overflow-auto max-h-[450px] bg-white rounded-lg scrollbar-custom">
            <div className="p-2">
                <DisplayCharts logs={logs} view={view} />
            </div>
        </div>
    );
};

export default QueryResults;