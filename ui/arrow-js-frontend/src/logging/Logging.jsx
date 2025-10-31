import React, { useState, useRef, useEffect } from "react";
import "../App.css";
import { useLogging } from "../context/LoggingContext";
import { v4 as uuidv4 } from "uuid";
import DisplayCharts from "../components/DisplayCharts";
import { useForm } from 'react-hook-form';
import {
    HiOutlineChevronDoubleUp,
    HiOutlineChevronDoubleDown,
    HiOutlineChevronDoubleRight,
    HiOutlineChevronDoubleLeft,
    HiOutlineX,
} from "react-icons/hi";

const Logging = () => {
    const DEFAULT_VIEW = "table";
    const { executeQuery, login } = useLogging();

    // react-hook-form with defaultValues
    const {
        register,
        handleSubmit,
        watch,
        formState: { errors, isSubmitted, isSubmitting }
    } = useForm({
        defaultValues: {
            url: "",
            username: "",
            password: "",
            splitSize: 0,
        },
    });
    // watch all fields
    const watchedFields = watch();

    // connection config stored for executeQuery usage
    const [connection, setConnection] = useState({
        url: "",
        username: "",
        password: "",
        splitSize: 0,
    });

    // whether we successfully connected
    const [isConnected, setIsConnected] = useState(false);
    const [loginError, setLoginError] = useState(null);

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

    // Helper: normalize response to array
    const normalizeLogs = (logsData) => {
        if (Array.isArray(logsData)) return logsData;
        if (Array.isArray(logsData?.data)) return logsData.data;
        if (Array.isArray(logsData?.logs)) return logsData.logs;
        if (logsData && typeof logsData === "object") return [logsData];
        return [];
    };

    // Core executor for one query row
    const runQueryForRow = async (row) => {
        const { url, username, password, splitSize } = connection;

        // Not connected
        if (!isConnected) {return { logs: [], error: "Not connected — click Connect" };}
        // Empty query
        if (!row.query?.trim()){return { logs: [], error: "Empty query — skipped" };}

        try {
            const data = await executeQuery(url, username, password, row.query, splitSize);
            return { logs: normalizeLogs(data), error: null };
        } catch (err) {
            return { logs: [], error: err?.message || "Query failed" };
        }
    };

    // Run a single query
    const handleRunQuery = async (row) => {
        const id = row.id;
        setResults(prev => ({ ...prev, [id]: { logs: [], loading: true, error: null } }));

        const result = await runQueryForRow(row);

        setResults(prev => ({
            ...prev,
            [id]: { ...result, loading: false },
        }));
    };

    // Run all queries (sequential)
    const runAllQuerys = async ({ delayMs = 100 } = {}) => {
        if (!isConnected) {
            const updated = Object.fromEntries(
                rows.map(r => [r.id, { logs: [], loading: false, error: "Not connected — click Connect" }])
            );
            setResults(updated);
            return;
        }

        // Mark all rows loading
        setResults(prev => ({
            ...prev,
            ...Object.fromEntries(rows.map(r => [r.id, { logs: [], loading: true, error: null }])),
        }));

        for (const row of rows) {
            const result = await runQueryForRow(row);
            setResults(prev => ({
                ...prev,
                [row.id]: { ...result, loading: false },
            }));

            if (delayMs > 0) await new Promise(res => setTimeout(res, delayMs));
        }
    };

    const clearRowLogs = (id) => {
        setResults((prev) => ({
            ...prev,
            [id]: { logs: [], loading: false, error: null },
        }));
    };

    // Handle form submission (Connect)
    const onSubmit = async (data) => {
        // clear previous login error
        setLoginError(null);
        // set connection state (so UI has it)
        setConnection({
            url: data.url,
            username: data.username,
            password: data.password,
            splitSize: data.splitSize ?? 0,
        });

        try {
            await login(data.url, data.username, data.password);
            setIsConnected(true);
            setLoginError(null);
        } catch (err) {
            setIsConnected(false);
            setLoginError(err?.message || "Login failed");
        }
    };

    // whenever any field changes in connection settings we will disconnect.
    useEffect(() => {
        setIsConnected(false);
    }, [watchedFields.url, watchedFields.username, watchedFields.password, watchedFields.splitSize]);

    return (
        <div className="relative min-h-screen bg-gradient-to-br from-gray-50 to-gray-200 p-8 space-y-10">
            {/* === Connection Panel Toggle === */}
            <button
                onClick={() => setShowConnection(!showConnection)}
                className="fixed top-30.5 left-[1.5px] z-30 text-xl bg-gray-600 text-white p-1 rounded-full shadow hover:bg-gray-700 transition cursor-pointer"
            >
                {showConnection ? <HiOutlineX /> : <HiOutlineChevronDoubleRight />}
            </button>

            {/* === Connection Panel === */}
            <div
                className={`fixed top-30 left-8 w-80 bg-white shadow-2xl rounded-2xl border border-gray-600 transform transition-transform duration-300 ease-in-out p-6 z-20 ${showConnection ? "translate-x-0" : "-translate-x-full"
                    }`}
            >
                <h2 className="text-2xl font-semibold text-gray-800 mb-4">
                    Connection Settings
                </h2>

                {/* show a tiny status */}
                <div className="mb-2">
                    {isConnected ? (
                        <p className="text-sm text-green-700">Connected</p>
                    ) : (
                        <p className="text-sm text-gray-600">Not connected</p>
                    )}
                </div>

                <form className="space-y-4" onSubmit={handleSubmit(onSubmit)}>
                    <div>
                        <label className="block text-sm font-medium text-gray-700 mb-1">
                            Server URL
                        </label>
                        <input
                            type="text"
                            {...register("url", { required: "Server URL is required" })}
                            placeholder="http://localhost:8080"
                            className="w-full border border-gray-400 rounded-lg p-2"
                        />
                        {/* show error only after submit attempt */}
                        {isSubmitted && errors.url && (
                            <p className="text-red-500 text-sm">{errors.url.message}</p>
                        )}
                    </div>

                    <div>
                        <label className="block text-sm font-medium text-gray-700 mb-1">
                            Username
                        </label>
                        <input
                            type="text"
                            {...register("username", { required: "Username is required" })}
                            placeholder="Enter username"
                            className="w-full border border-gray-400 rounded-lg p-2"
                        />
                        {isSubmitted && errors.username && (
                            <p className="text-red-500 text-sm">{errors.username.message}</p>
                        )}
                    </div>

                    <div>
                        <label className="block text-sm font-medium text-gray-700 mb-1">
                            Password
                        </label>
                        <input
                            type="password"
                            {...register("password", { required: "Password is required" })}
                            placeholder="Enter password"
                            className="w-full border border-gray-400 rounded-lg p-2"
                        />
                        {isSubmitted && errors.password && (
                            <p className="text-red-500 text-sm">{errors.password.message}</p>
                        )}
                    </div>

                    <div>
                        <label className="text-sm font-medium text-gray-600 mb-1 mr-2">
                            Split Size:
                        </label>
                        {/* remove any direct value binding; use RHF default value */}
                        <input
                            type="number"
                            min="0"
                            {...register("splitSize", {
                                valueAsNumber: true,
                                validate: (v) => (v >= 0) || "Split size must be 0 or greater",
                            })}
                            className="w-40 border border-gray-400 rounded-lg p-1"
                        />
                        {isSubmitted && errors.splitSize && (
                            <p className="text-red-500 text-xs mt-1">{errors.splitSize.message}</p>
                        )}
                        <p className="text-xs text-gray-500 mt-1">0 = /query; &gt;0 = plan/split</p>
                    </div>

                    {/* show login error if any */}
                    {loginError && (
                        <p className="text-red-600 text-sm">{loginError}</p>
                    )}

                    <button
                        type="submit"
                        className={`w-full ${isConnected ? "bg-blue-300 hover:bg-blue-400" : "bg-blue-600 hover:bg-blue-700 cursor-pointer"} text-white font-medium py-2 rounded-lg mt-4 transition`}
                        disabled={(isSubmitting, isConnected)}
                    >
                        {isSubmitting ? "Connecting..." : "Connect"}
                    </button>
                </form>
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
                        className={`relative max-w-[85dvw] mx-auto bg-white rounded-2xl shadow-2xl p-8 border border-gray-300 flex flex-col overflow-hidden`}
                    >
                        {/* Collapse toggle */}
                        <div className={`absolute top-0 left-0 w-full h-2.5 bg-gray-800 z-10`}>
                            <div
                                className={`absolute -bottom-3.5 ${row.showPanel ? "-bottom-4.5" : ""
                                    } left-1/2 -translate-x-1/2 w-50 h-13 bg-gray-800 cursor-pointer rounded-b-full z-10 transition-all duration-300`}
                                onClick={() => updateRow(row.id, "showPanel", !row.showPanel)}
                            >
                                <button
                                    onClick={() => updateRow(row.id, "showPanel", !row.showPanel)}
                                    className="absolute top-7 left-1/2 -translate-x-1/2 z-20 rounded-full text-white h-6 cursor-pointer"
                                >
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
                            className="absolute -right-5 hover:right-0 top-4 w-12 border p-1 rounded-full rounded-r-lg text-sm font-medium hover:bg-red-500 hover:text-white transition-all duration-300 disabled:opacity-50 cursor-pointer"
                        >
                            <HiOutlineX className="text-xl" />
                        </button>

                        {/* Query Editor */}
                        <div
                            className={`overflow-hidden mt-5 shadow-lg rounded-xl transition-all duration-500 ease-in-out ${row.showPanel
                                ? "max-h-[600px] opacity-100"
                                : "max-h-0 opacity-0"
                                }`}
                        >
                            <div className="flex flex-col flex-1 bg-gray-50 rounded-xl border border-gray-300 p-6">
                                <h2 className="text-2xl font-semibold text-gray-800 mb-2">
                                    SQL Query #{row.id.split("-")[0]}
                                </h2>
                                <textarea
                                    value={row.query}
                                    onChange={(e) => updateRow(row.id, "query", e.target.value)}
                                    onKeyDown={(e) => {
                                            if (e.key === "Enter" && e.shiftKey && isConnected) {
                                                e.preventDefault();
                                                handleRunQuery(row);
                                            }
                                        }}
                                    placeholder="e.g. SELECT * FROM read_arrow('/your/file.arrow');"
                                    rows={5}
                                    className="w-full border border-gray-400 rounded-lg p-3 text-sm h-35 scrollbar-custom"
                                />
                                <div className="flex justify-end items-center gap-3 mt-3">
                                    <button
                                        onClick={() => clearRowLogs(row.id)}
                                        disabled={loading || logs.length === 0}
                                        className="bg-gray-400 hover:bg-gray-500 text-white font-medium py-2 px-4 rounded-md transition disabled:opacity-50 cursor-pointer"
                                    >
                                        Clear
                                    </button>
                                    <button
                                        onClick={() => handleRunQuery(row)}
                                        disabled={loading || !isConnected}
                                        className="bg-blue-600 hover:bg-blue-700 text-white font-medium py-2 px-4 rounded-md transition disabled:opacity-50 cursor-pointer"
                                    >
                                        {loading ? "Running..." : "Run"}
                                    </button>
                                </div>
                            </div>
                        </div>

                        {/* === Results === */}
                        <div
                            className={`flex flex-col flex-1 transition-all duration-500 ease-in-out ${row.showPanel ? "mt-6" : "mt-0"
                                }`}
                        >
                            <div className="flex justify-between items-center mb-2">
                                <h2 className="text-2xl font-semibold text-gray-800">
                                    Results
                                </h2>

                                {/* View selection radio buttons */}
                                <div className="flex items-center gap-5">
                                    {["table", "line", "area", "bar", "pie"].map((v) => (
                                        <label
                                            key={v}
                                            className="flex items-center text-sm cursor-pointer"
                                        >
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
                                    // if not connected, show Connect-first message
                                    !isConnected ? (
                                        <p className="text-gray-700 text-center p-10">
                                            Connect first to run queries. Click <b>Connect</b> in the Connection Settings.
                                        </p>
                                    ) : (
                                        <p className="text-gray-500 text-center p-10">
                                            No results yet. Run a query to view log data.
                                        </p>
                                    )
                                ) : row.view === "table" ? (
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
                                                            {key === "dt" ? new Date(val).toLocaleString() : String(val)}
                                                        </td>
                                                    ))}
                                                </tr>
                                            ))}
                                        </tbody>
                                    </table>
                                ) : (
                                    <div className="p-2">
                                        <DisplayCharts logs={logs} view={row.view} />
                                    </div>
                                )}
                            </div>
                        </div>
                    </div>
                );
            })}

            {/* Add Row and Run all quries Button */}
            <div className="flex justify-evenly mt-10 gap-5">
                <button
                    onClick={addRow}
                    className="bg-green-600 hover:bg-green-700 text-white font-semibold py-3 px-6 rounded-md transition duration-300 cursor-pointer"
                >
                    Add New Query Row
                </button>

                <button
                    onClick={runAllQuerys}
                       disabled={!isConnected}
                    className="bg-blue-600 hover:bg-blue-700 text-white font-semibold py-3 px-6 rounded-md disabled:opacity-50 transition duration-300 cursor-pointer"
                >
                    Run Queries
                </button>
            </div>
        </div>
    );
};

export default Logging;
