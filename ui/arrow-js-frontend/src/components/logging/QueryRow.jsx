import React from "react";
import { HiOutlineChevronDoubleUp, HiOutlineChevronDoubleDown, HiOutlineX } from "react-icons/hi";
import QueryResults from "./QueryResults";
import VariableManager from "./VariableManager";

const QueryRow = ({
    row,
    result,
    queryId,
    isConnected,
    isCancelling,
    totalRows,
    updateRow,
    removeRow,
    handleRunQuery,
    handleCancelQuery,
    clearRowLogs
}) => {
    const { logs = [], loading = false, error = null } = result || {};
    const variables = row.variables || {};

    // Handle updating variables for this row
    const handleUpdateVariables = (updatedVariables) => {
        updateRow(row.id, "variables", updatedVariables);
    };

    return (
        <div className="relative max-w-[85dvw] mx-auto bg-white rounded-2xl shadow-2xl p-8 border border-gray-300 flex flex-col overflow-hidden">
            {/* Collapse toggle */}
            <div className="absolute top-0 left-0 w-full h-2.5 bg-gray-800 z-10">
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
                disabled={totalRows === 1}
                className="absolute -right-5 hover:right-0 top-4 w-12 border p-1 rounded-full rounded-r-lg text-sm font-medium hover:bg-red-500 hover:text-white transition-all duration-300 disabled:opacity-50 cursor-pointer"
            >
                <HiOutlineX className="text-xl" />
            </button>

            {/* Query Editor */}
            <div
                className={`overflow-hidden mt-5 shadow-lg rounded-xl transition-all duration-500 ease-in-out ${row.showPanel
                    ? "max-h-150 opacity-100"
                    : "max-h-0 opacity-0"
                    }`}
            >
                <div className="flex flex-col flex-1 bg-gray-50 rounded-xl border border-gray-300 p-6">
                    <div className="flex justify-between items-start mb-2">
                        <h2 className="text-2xl w-full font-semibold text-gray-800">
                            SQL Query #{row.id.split("-")[0]}
                            {queryId && (
                                <span className="text-sm text-gray-500 ml-2">
                                    (Query ID: {queryId})
                                </span>
                            )}
                        </h2>
                        {/* Variable Manager - top right */}
                        <VariableManager
                            query={row.query}
                            variables={variables}
                            onUpdateVariables={handleUpdateVariables}
                        />
                    </div>
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
                            disabled={!isConnected || isCancelling || !queryId || result?.loading === false}
                            className="bg-red-600 hover:bg-red-700 text-white font-medium py-2 px-4 rounded-md transition disabled:opacity-50 cursor-pointer"
                        >
                            {isCancelling ? "Canceling..." : "Cancel"}
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

            {/* Results */}
            <div
                className={`flex flex-col flex-1 transition-all duration-500 ease-in-out ${row.showPanel ? "mt-6" : "mt-0"
                    }`}
            >
                <div className="flex justify-between items-center mb-2">
                    <h2 className="text-2xl font-semibold text-gray-800">Results</h2>

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

                <QueryResults
                    logs={logs}
                    loading={loading}
                    error={error}
                    view={row.view}
                    isConnected={isConnected}
                />
            </div>
        </div>
    );
};

export default QueryRow;