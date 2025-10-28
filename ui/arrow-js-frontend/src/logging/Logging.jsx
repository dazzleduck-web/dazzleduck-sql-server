import React, { useState, useRef } from "react";
import "../App.css";
import { useLogging } from "../context/LoggingContext";
import { v4 as uuidv4 } from "uuid";
import DisplayCharts from "../components/DisplayCharts";
import {
    HiOutlineChevronDoubleUp,
    HiOutlineChevronDoubleDown,
    HiOutlineChevronDoubleRight,
    HiOutlineChevronDoubleLeft,
    HiOutlineX,
} from "react-icons/hi";
const Logging = () => {
    const DEFAULT_VIEW = "table";
    const { executeQuery } = useLogging();

    // connection config
    const [connection, setConnection] = useState({
        url: "",
        username: "",
        password: "",
        splitSize: 0,
    });

    const [showConnection, setShowConnection] = useState(true);

    // Query rows
    const [rows, setRows] = useState([
        { id: "1-" + uuidv4(), showPanel: true, query: "", view: DEFAULT_VIEW },
    ]);

    const [results, setResults] = useState({});
    const nextId = useRef(2);

    const addRow = () => {
        const newId = nextId.current++ + "-" + uuidv4();
        setRows((prev) => [
            ...prev,
            { id: newId, showPanel: true, query: "", view: DEFAULT_VIEW },
        ]);
    };

    const removeRow = (id) => {
        if (rows.length === 1) return;
        setRows((prev) => prev.filter((r) => r.id !== id));
        setResults((prev) => {
            const copy = { ...prev };
            delete copy[id];
            return copy;
        });
    };

    const updateRow = (id, key, value) => {
        setRows((prev) =>
            prev.map((r) => (r.id === id ? { ...r, [key]: value } : r))
        );
    };

    const handleRunQuery = async (row) => {
        const { url, username, password, splitSize } = connection;
        const id = row.id;
        const query = row.query;

        setResults((prev) => ({
            ...prev,
            [id]: { logs: [], loading: true, error: null },
        }));

        try {
            const logsData = await executeQuery(
                url,
                username,
                password,
                query,
                splitSize
            );

            // Normalize into array of objects
            let normalized = [];
            if (Array.isArray(logsData)) {
                normalized = logsData;
            } else if (Array.isArray(logsData?.data)) {
                normalized = logsData.data;
            } else if (Array.isArray(logsData?.logs)) {
                normalized = logsData.logs;
            } else if (typeof logsData === "object" && logsData !== null) {
                normalized = [logsData];
            }

            setResults((prev) => ({
                ...prev,
                [id]: { logs: normalized, loading: false, error: null },
            }));
        } catch (err) {
            setResults((prev) => ({
                ...prev,
                [id]: {
                    logs: [],
                    loading: false,
                    error: err.message || "Query failed",
                },
            }));
        }
    };

    const clearRowLogs = (id) => {
        setResults((prev) => ({
            ...prev,
            [id]: { logs: [], loading: false, error: null },
        }));
    };

    return (
        <div className="relative min-h-screen bg-gradient-to-br from-gray-50 to-gray-200 p-8 space-y-10">
            {/* === Connection Panel Toggle === */}
            <button
                onClick={() => setShowConnection(!showConnection)}
                className="fixed top-30.5 left-[1.5px] z-30 text-xl bg-gray-600 text-white p-1 rounded-full shadow hover:bg-gray-700 transition cursor-pointer">
                {showConnection ? <HiOutlineX /> : <HiOutlineChevronDoubleRight />}
            </button>

            {/* === Connection Panel === */}
            <div
                className={`fixed top-30 left-8 w-80 bg-white shadow-2xl rounded-2xl border border-gray-600 transform transition-transform duration-300 ease-in-out p-6 z-20 ${showConnection ? "translate-x-0" : "-translate-x-full"
                    }`}>
                <h2 className="text-2xl font-semibold text-gray-800 mb-4">
                    Connection Settings
                </h2>
                <div className="space-y-4">
                    {["url", "username", "password"].map((field) => (
                        <div key={field}>
                            <label className="block text-sm font-medium text-gray-700 mb-1">
                                {field === "url"
                                    ? "Server URL"
                                    : field.charAt(0).toUpperCase() + field.slice(1)}
                            </label>
                            <input
                                type={field === "password" ? "password" : "text"}
                                value={connection[field]}
                                onChange={(e) =>
                                    setConnection({ ...connection, [field]: e.target.value })
                                }
                                placeholder={
                                    field === "url" ? "http://localhost:8080" : `Enter ${field}`
                                }
                                className="w-full border border-gray-400 rounded-lg p-2"
                            />
                        </div>
                    ))}

                    <div>
                        <label className="text-sm font-medium text-gray-600 mb-1 mr-2">
                            Split Size:
                        </label>
                        <input
                            type="number"
                            min="0"
                            value={connection.splitSize}
                            onChange={(e) => {
                                const value = Number(e.target.value);
                                if (value >= 0 || e.target.value === "") {
                                    setConnection({ ...connection, splitSize: e.target.value });
                                }
                            }}
                            className="w-40 border border-gray-400 rounded-lg p-1"
                        />
                        <p className="text-xs text-gray-500 mt-1">
                            0 = /query; &gt;0 = plan/split
                        </p>
                    </div>
                </div>
            </div>

            {/* === Query Rows === */}
            {rows.map((row) => {
                const result = results[row.id] || {
                    logs: [],
                    loading: false,
                    error: null,
                };
                const { logs, loading, error } = result;

                return (
                    <div
                        key={row.id}
                        className={`relative max-w-[85dvw] mx-auto bg-white rounded-2xl shadow-2xl p-8 border border-gray-300 flex flex-col overflow-hidden`}>
                        {/* Collapse toggle */}
                        <div
                            className={`absolute top-0 left-0 w-full h-2.5 bg-gray-800 z-10`}>
                            <div
                                className={`absolute -bottom-3.5 ${row.showPanel ? "-bottom-4.5" : ""
                                    } left-1/2 -translate-x-1/2 w-50 h-13 bg-gray-800 cursor-pointer rounded-b-full z-10 transition-all duration-300`}
                                onClick={() => updateRow(row.id, "showPanel", !row.showPanel)}>
                                <button
                                    onClick={() => updateRow(row.id, "showPanel", !row.showPanel)}
                                    className="absolute top-7 left-1/2 -translate-x-1/2 z-20 rounded-full text-white h-6 cursor-pointer">
                                    {row.showPanel ? (
                                        <HiOutlineChevronDoubleUp className="text-lg" />
                                    ) : (
                                        <HiOutlineChevronDoubleDown className="text-lg" />
                                    )}
                                </button>
                            </div>
                        </div>

                        {/* Remove button */}
                        <button
                            onClick={() => removeRow(row.id)}
                            disabled={rows.length === 1}
                            className="absolute -right-5 hover:right-0 top-4 w-12 border p-1 rounded-full rounded-r-lg text-sm font-medium hover:bg-red-500 hover:text-white transition-all duration-300 disabled:opacity-50 cursor-pointer">
                            <HiOutlineX className="text-xl" />
                        </button>

                        {/* Query Editor */}
                        <div
                            className={`overflow-hidden mt-5 shadow-lg rounded-xl transition-all duration-500 ease-in-out ${row.showPanel
                                ? "max-h-[600px] opacity-100"
                                : "max-h-0 opacity-0"
                                }`}>
                            <div className="flex flex-col flex-1 bg-gray-50 rounded-xl border border-gray-300 p-6">
                                <h2 className="text-2xl font-semibold text-gray-800 mb-2">
                                    SQL Query #{row.id.split("-")[0]}
                                </h2>
                                <textarea
                                    value={row.query}
                                    onChange={(e) => updateRow(row.id, "query", e.target.value)}
                                    placeholder="e.g. SELECT * FROM read_arrow('/your/file.arrow');"
                                    rows={5}
                                    className="w-full border border-gray-400 rounded-lg p-3 text-sm h-35 scrollbar-custom"
                                />
                                <div className="flex justify-end items-center gap-3 mt-3">
                                    <button
                                        onClick={() => clearRowLogs(row.id)}
                                        disabled={loading || logs.length === 0}
                                        className="bg-gray-400 hover:bg-gray-500 text-white font-medium py-2 px-4 rounded-md transition disabled:opacity-50 cursor-pointer">
                                        Clear
                                    </button>
                                    <button
                                        onClick={() => handleRunQuery(row)}
                                        disabled={loading}
                                        className="bg-blue-600 hover:bg-blue-700 text-white font-medium py-2 px-4 rounded-md transition disabled:opacity-50 cursor-pointer">
                                        {loading ? "Running..." : "Run Query"}
                                    </button>
                                </div>
                            </div>
                        </div>

                        {/* === Results === */}
                        <div
                            className={`flex flex-col flex-1 transition-all duration-500 ease-in-out ${row.showPanel ? "mt-6" : "mt-0"
                                }`}>
                            <div className="flex justify-between items-center mb-2">
                                <h2 className="text-2xl font-semibold text-gray-800">
                                    Results
                                </h2>
                                {/* View selection radio buttons */}
                                <div className="flex items-center gap-5">
                                    {["table", "line", "area", "bar", "pie"].map((v) => (
                                        <label
                                            key={v}
                                            className="flex items-center text-sm cursor-pointer">
                                            <input
                                                type="radio"
                                                name={`view-${row.id}`}
                                                value={v}
                                                checked={row.view === v}
                                                onChange={() => updateRow(row.id, "view", v)}
                                                className="mr-1 cursor-pointer"
                                            />
                                            {v.charAt(0).toUpperCase() + v.slice(1)}
                                        </label>
                                    ))}
                                </div>
                            </div>

                            <div className="border overflow-auto max-h-[450px] bg-white rounded-lg scrollbar-custom">
                                {loading ? (
                                    <p className="text-gray-500 text-center p-10">
                                        Loading results...
                                    </p>
                                ) : error ? (
                                    <p className="text-red-600 text-center font-medium p-10">
                                        {error}
                                    </p>
                                ) : logs.length === 0 ? (
                                    <p className="text-gray-500 text-center p-10">
                                        No results yet. Run a query to view log data.
                                    </p>
                                ) : row.view === "table" ? (
                                    <table className="w-full text-sm text-left border-separate border-spacing-0">
                                        <thead className="sticky top-0 z-10 shadow-md custom-font">
                                            <tr className="bg-blue-100 text-gray-800">
                                                {Object.keys(logs[0]).map((key) => (
                                                    <th
                                                        key={key}
                                                        className="p-2 border border-gray-400 capitalize text-center">
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
                                                  {key === "dt" ? new Date(val).toLocaleString() : String(val)}
                                                </td>
                                              ))}
                                            </tr>
                                          ))}
                                        </tbody>

                                    </table>
                                ) : (
                                    <div className="p-2">
                                        <DisplayCharts
                                            logs={logs}
                                            view={row.view}
                                        />
                                    </div>
                                )}
                            </div>
                        </div>
                    </div>
                );
            })}

            {/* Add Row Button */}
            <div className="flex justify-center mt-10">
                <button
                    onClick={addRow}
                    className="bg-green-600 hover:bg-green-700 text-white font-semibold py-3 px-6 rounded-md transition duration-300 cursor-pointer">
                    Add New Query Row
                </button>
            </div>
        </div>
    );
};

export default Logging;
