import React, { useState, useRef, useEffect } from "react";
import "../App.css";
import { useLogging } from "../context/LoggingContext";
import { v4 as uuidv4 } from "uuid";
import DisplayCharts from "../components/DisplayCharts";
import { useForm } from 'react-hook-form';
import { HiOutlineChevronDoubleUp, HiOutlineChevronDoubleDown, HiOutlineChevronDoubleRight, HiOutlineX } from "react-icons/hi";
import { formatPossibleDate } from "../components/utils/DateNormalizer";
import { AiFillCloseCircle, AiFillCheckCircle } from "react-icons/ai";

const Logging = () => {
    const DEFAULT_VIEW = "table";
    const { executeQuery, login, cancelQuery } = useLogging();
    const [showAdvanced, setShowAdvanced] = useState(false);
    const [claims, setClaims] = useState([{ key: "", value: "" }]);

    // State to track query IDs and cancellation status
    const [queryIds, setQueryIds] = useState({}); // { rowId: numeric queryId }
    const [cancellingQueries, setCancellingQueries] = useState({}); // { rowId: boolean }
    // generating numeric query IDs
    const queryIdCounter = useRef(1);

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
            cluster: "",
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
        claims: { cluster: "" },
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
        // Clean up queryId tracking
        setQueryIds((prev) => {
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

    const addClaim = () => {
        setClaims(prev => [...prev, { key: "", value: "" }]);
    };

    const removeClaim = (index) => {
        setClaims(prev => prev.filter((_, i) => i !== index));
    };

    const updateClaim = (index, field, value) => {
        setClaims(prev =>
            prev.map((item, i) =>
                i === index ? { ...item, [field]: value } : item
            )
        );
    };

    // Generate a numeric query ID
    const generateQueryId = () => {
        return queryIdCounter.current++;
    };

    // Core executor for one query row
    const runQueryForRow = async (row) => {
        const { url, username, password, splitSize } = connection;

        // Not connected
        if (!isConnected) {
            return { logs: [], error: "Not connected — click Connect", queryId: null };
        }
        // Empty query
        if (!row.query?.trim()) {
            return { logs: [], error: "Empty query — skipped", queryId: null };
        }

        // Generate numeric query ID BEFORE executing
        const queryId = generateQueryId();

        // Store it immediately so cancel can use it
        setQueryIds(prev => ({ ...prev, [row.id]: queryId }));

        try {
            const result = await executeQuery(url, row.query, splitSize, null, queryId);
            return { logs: result.data, error: null, queryId: result.queryId };
        } catch (err) {
            return { logs: [], error: err?.message || "Query failed", queryId: null };
        }
    };

    // Run a single query
    const handleRunQuery = async (row) => {
        const id = row.id;
        setResults(prev => ({ ...prev, [id]: { logs: [], loading: true, error: null } }));

        const result = await runQueryForRow(row);

        setResults(prev => ({
            ...prev,
            [id]: { logs: result.logs, loading: false, error: result.error },
        }));
    };

    // Cancel a query
    const handleCancelQuery = async (row) => {
        const id = row.id;
        const queryId = queryIds[id];

        if (!queryId) {
            console.warn("No query ID found for row:", id);
            setResults(prev => ({
                ...prev,
                [id]: {
                    ...prev[id],
                    error: "No active query to cancel for this row"
                },
            }));
            return;
        }

        if (!isConnected) {
            setResults(prev => ({
                ...prev,
                [id]: { ...prev[id], error: "Not connected — cannot cancel" },
            }));
            return;
        }

        setCancellingQueries(prev => ({ ...prev, [id]: true }));

        try {
            const { url } = connection;
            const cleanUrl = url.trim();
            const result = await cancelQuery(cleanUrl, row.query, queryId);

            if (result.success) {
                setResults(prev => ({
                    ...prev,
                    [id]: {
                        logs: prev[id]?.logs || [],
                        loading: false,
                        error: `Query cancelled (status: ${result.status})`,
                    },
                }));
            } else {
                setResults(prev => ({
                    ...prev,
                    [id]: {
                        ...prev[id],
                        error: `Cancel failed with status: ${result.status}`,
                    },
                }));
            }

            // Clean up the queryId after successful cancel
            setQueryIds(prev => {
                const updated = { ...prev };
                delete updated[id];
                return updated;
            });
        } catch (err) {
            setResults(prev => ({
                ...prev,
                [id]: {
                    ...prev[id],
                    error: `Cancel error: ${err?.message || "Unknown error"}`,
                },
            }));
        } finally {
            setCancellingQueries(prev => ({ ...prev, [id]: false }));
        }
    };

    // Run all queries (parallel)
    const runAllQueries = async () => {
        if (!isConnected) return;

        const loadingState = {};
        rows.forEach(row => {
            loadingState[row.id] = { logs: [], loading: true, error: null };
        });
        setResults(prev => ({ ...prev, ...loadingState }));

        rows.forEach(async (row) => {
            const id = row.id;
            try {
                const result = await runQueryForRow(row);
                setResults(prev => ({
                    ...prev,
                    [id]: {
                        logs: result.logs,
                        loading: false,
                        error: result.error
                    }
                }));
            } catch (err) {
                setResults(prev => ({
                    ...prev,
                    [id]: {
                        logs: [],
                        loading: false,
                        error: err?.message || "Query failed"
                    }
                }));
            }
        });
    };

    const clearRowLogs = (id) => {
        setResults((prev) => ({
            ...prev,
            [id]: { logs: [], loading: false, error: null },
        }));
    };

    // Handle form submission (Connect)
    const onSubmit = async (data) => {
        setLoginError(null);

        // convert claims list → object
        const claimsObject = {};
        claims.forEach(c => {
            if (c.key.trim() !== "") {
                claimsObject[c.key.trim()] = c.value.trim();
            }
        });

        const newConnection = {
            url: data.url,
            username: data.username,
            password: data.password,
            claims: claimsObject,
            splitSize: data.splitSize ?? 0,
        };

        setConnection(newConnection);

        try {
            await login(data.url, data.username, data.password, claimsObject);
            setIsConnected(true);
        } catch (err) {
            setIsConnected(false);
            setLoginError(err?.message || "Login failed");
        }
    };

    // whenever any field changes in connection settings we will disconnect.
    useEffect(() => {
        setIsConnected(false);
    }, [watchedFields.url, watchedFields.username, watchedFields.password, watchedFields.splitSize, claims]);

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
                className={`fixed top-30 left-8 w-90 bg-white shadow-2xl rounded-2xl border border-gray-600 transform transition-transform duration-300 ease-in-out px-6 py-4 z-20 max-h-[80vh] overflow-y-auto scrollbar-hidden ${showConnection ? "translate-x-0" : "-translate-x-full"}`}
            >
                <div className="flex items-center gap-2 mb-3">
                    <h2 className="text-2xl font-semibold text-gray-800">
                        Connection Settings
                    </h2>

                    {/* Status circle */}
                    <div className="mt-2">
                        {isConnected ? (
                            <AiFillCheckCircle className="text-green-600" size={20} />
                        ) : (
                            <AiFillCloseCircle className="text-red-600" size={20} />
                        )}
                    </div>
                </div>

                <form className="space-y-4" onSubmit={handleSubmit(onSubmit)}>
                    <div>
                        <label className="block text-sm font-medium text-gray-700 mb-1">
                            Server URL
                        </label>
                        <input
                            type="text"
                            {...register("url", { required: "Server URL is required" })}
                            placeholder="Enter Server URL"
                            className="w-full border border-gray-400 rounded-lg py-1 px-2"
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
                            className="w-full border border-gray-400 rounded-lg py-1 px-2"
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
                            className="w-full border border-gray-400 rounded-lg py-1 px-2"
                        />
                        {isSubmitted && errors.password && (
                            <p className="text-red-500 text-sm">{errors.password.message}</p>
                        )}
                    </div>

                    {/* === Advanced Settings === */}
                    <div className="">
                        <button
                            type="button"
                            onClick={() => setShowAdvanced(!showAdvanced)}
                            className="flex justify-between w-full text-left text-gray-800 font-medium"
                        >
                            <span>Advanced Settings</span>
                            <span className="text-sm">{showAdvanced ? "▲" : "▼"}</span>
                        </button>

                        {/* Collapsible Content */}
                        <div
                            className={`transition-all duration-300 overflow-hidden ${showAdvanced ? "max-h-96 opacity-100" : "max-h-0 opacity-0"
                                }`}
                        >
                            <div className="mt-2 space-y-4">

                                {/* === Claims Section === */}
                                <div>
                                    <label className="block text-sm font-medium text-gray-700 mb-1">
                                        Claims
                                    </label>

                                    {claims.map((item, index) => {
                                        const allowedKeys = ["cluster", "orgId", "database", "schema", "table", "path", "function",];
                                        const isValidKey = item.key.trim() === "" || allowedKeys.includes(item.key.trim());

                                        // Suggestions appear only when partial typing AND no exact match
                                        const filteredSuggestions =
                                            item.key.trim() &&
                                                !allowedKeys.includes(item.key.trim())
                                                ? allowedKeys.filter((k) =>
                                                    k.toLowerCase().includes(item.key.toLowerCase())
                                                ) : [];

                                        return (
                                            <div key={index} className="relative mb-2">
                                                <div className="flex items-center gap-2">
                                                    {/* Key Input */}
                                                    <div className="w-1/2 relative">
                                                        <input
                                                            type="text"
                                                            placeholder="key"
                                                            value={item.key}
                                                            onChange={(e) => updateClaim(index, "key", e.target.value)}
                                                            className={`w-full border rounded-lg py-1 px-2 ${item.key.trim() === "" ? "border-gray-400" : isValidKey ? "border-green-500" : "border-red-500"}`}
                                                        />

                                                        {/* Suggestions */}
                                                        {filteredSuggestions.length > 0 && (
                                                            <div className="absolute left-0 right-0 bg-white border border-gray-300 rounded shadow-lg max-h-40 overflow-y-auto z-20">
                                                                {filteredSuggestions.map((suggest, i) => (
                                                                    <div
                                                                        key={i}
                                                                        className="px-2 py-1 hover:bg-gray-100 cursor-pointer"
                                                                        onClick={() => {
                                                                            updateClaim(index, "key", suggest);
                                                                        }}
                                                                    >
                                                                        {suggest}
                                                                    </div>
                                                                ))}
                                                            </div>
                                                        )}
                                                    </div>

                                                    {/* Value Input */}
                                                    <input
                                                        type="text"
                                                        placeholder="value"
                                                        value={item.value}
                                                        onChange={(e) => updateClaim(index, "value", e.target.value)}
                                                        className="w-1/2 border border-gray-400 rounded-lg py-1 px-2"
                                                    />

                                                    {/* Remove */}
                                                    <button
                                                        type="button"
                                                        onClick={() => removeClaim(index)}
                                                        className="text-red-600 font-bold"
                                                    >
                                                        ✕
                                                    </button>
                                                </div>
                                            </div>
                                        );
                                    })}

                                    {/* Add Row */}
                                    <button
                                        type="button"
                                        onClick={addClaim}
                                        className="text-blue-600 font-semibold mt-1"
                                    >
                                        + Add Claim
                                    </button>
                                </div>

                                {/* === Split Size (unchanged) === */}
                                <div>
                                    <label className="block text-sm font-medium text-gray-700 mb-1">
                                        Split Size
                                    </label>
                                    <input
                                        type="number"
                                        min="0"
                                        {...register("splitSize", {
                                            valueAsNumber: true,
                                            validate: (v) =>
                                                v >= 0 || "Split size must be 0 or greater",
                                        })}
                                        className="w-40 border border-gray-400 rounded-lg p-1"
                                    />
                                    {isSubmitted && errors.splitSize && (
                                        <p className="text-red-500 text-xs mt-1">
                                            {errors.splitSize.message}
                                        </p>
                                    )}
                                    <p className="text-xs text-gray-500 mt-1">
                                        0 = /query; &gt;0 = plan/split
                                    </p>
                                </div>
                            </div>
                        </div>
                    </div>

                    {/* show login error if any */}
                    {loginError && (
                        <p className="text-red-600 text-sm">{loginError}</p>
                    )}

                    <button
                        type="submit"
                        className={`w-full ${isConnected ? "bg-blue-300 hover:bg-blue-400" : "bg-blue-600 hover:bg-blue-700 cursor-pointer"} text-white font-medium py-2 rounded-lg mt-1 transition`}
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
                                    {queryIds[row.id] && (
                                        <span className="text-sm text-gray-500 ml-2">
                                            (Query ID: {queryIds[row.id]})
                                        </span>
                                    )}
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
                                        onClick={() => handleRunQuery(row)}
                                        disabled={loading || !isConnected}
                                        className="bg-blue-600 hover:bg-blue-700 text-white font-medium py-2 px-4 rounded-md transition disabled:opacity-50 cursor-pointer"
                                    >
                                        {loading ? "Running..." : "Run"}
                                    </button>
                                    <button
                                        onClick={() => handleCancelQuery(row)}
                                        disabled={!isConnected || cancellingQueries[row.id] || !queryIds[row.id]}
                                        className="bg-red-600 hover:bg-red-700 text-white font-medium py-2 px-4 rounded-md transition disabled:opacity-50 cursor-pointer"
                                    >
                                        {cancellingQueries[row.id] ? "Canceling..." : "Cancel"}
                                    </button>
                                    <button
                                        onClick={() => clearRowLogs(row.id)}
                                        disabled={loading || logs.length === 0}
                                        className="bg-gray-400 hover:bg-gray-500 text-white font-medium py-2 px-4 rounded-md transition disabled:opacity-50 cursor-pointer"
                                    >
                                        Clear
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
                                    {["table", "line", "bar", "pie"].map((v) => (
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
                                            No results yet. Run a query to view result data.
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
                                                            {String(formatPossibleDate(val))}
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
                    onClick={runAllQueries}
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
