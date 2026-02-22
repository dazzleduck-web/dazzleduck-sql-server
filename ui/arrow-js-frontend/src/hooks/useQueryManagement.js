import { useState, useRef } from "react";
import { v4 as uuidv4 } from "uuid";

const DEFAULT_VIEW = "table";

export const useQueryManagement = (executeQuery, cancelQuery, isConnected, connection) => {
    const [rows, setRows] = useState([
        { id: "1-" + uuidv4(), showPanel: true, query: "", view: DEFAULT_VIEW, variables: {} },
    ]);
    const [results, setResults] = useState({});
    const [queryIds, setQueryIds] = useState({});
    const [cancellingQueries, setCancellingQueries] = useState({});
    const [isRunningAll, setIsRunningAll] = useState(false);
    const [searchData, setSearchData] = useState([]);
    const [searchLoading, setSearchLoading] = useState(false);
    const [searchError, setSearchError] = useState("");
    const [searchQuery, setSearchQuery] = useState("");

    const nextId = useRef(2);
    const queryIdCounter = useRef(1);

    const generateQueryId = () => {
        return queryIdCounter.current++;
    };

    // Substitute variables in the query (also strips default values like {variable:default})
    const substituteVariables = (query, variables) => {
        let substitutedQuery = query;
        Object.entries(variables).forEach(([name, value]) => {
            // Skip empty variable names (to avoid replacing {})
            if (!name || name.trim() === "") {
                return;
            }
            // Replace {variable_name} or {variable_name:default} with the value
            // Escape special regex characters in variable name
            const escapedName = name.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
            // Match {name} or {name:default} patterns that are NOT preceded by \
            const pattern = new RegExp(`(?<!\\\\)\\{${escapedName}(?::[^}]*)?\\}`, 'g');
            substitutedQuery = substitutedQuery.replace(pattern, value || '');
        });
        // Convert escaped braces back to literal braces (\{ -> {, \} -> })
        substitutedQuery = substitutedQuery.replace(/\\{/g, '{').replace(/\\}/g, '}');
        return substitutedQuery;
    };

    const addRow = () => {
        const newId = nextId.current++ + "-" + uuidv4();
        setRows((prev) => [
            ...prev,
            { id: newId, showPanel: true, query: "", view: DEFAULT_VIEW, variables: {} },
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

    const runQueryForRow = async (row) => {
        const { url, splitSize } = connection;

        if (!isConnected) {
            return { logs: [], error: "Not connected — click Connect", queryId: null };
        }
        if (!row.query?.trim()) {
            return { logs: [], error: "Empty query — skipped", queryId: null };
        }

        // Substitute variables in the query
        const queryWithVars = substituteVariables(row.query, row.variables || {});

        const queryId = generateQueryId();
        setQueryIds(prev => ({ ...prev, [row.id]: queryId }));

        try {
            const result = await executeQuery(url, queryWithVars, splitSize, null, queryId);
            return { logs: result.data, error: null, queryId: result.queryId };
        } catch (err) {
            return { logs: [], error: err?.message || "Query failed", queryId: null };
        }
    };

    const handleRunQuery = async (row) => {
        const id = row.id;
        setResults(prev => ({ ...prev, [id]: { logs: [], loading: true, error: null } }));

        const result = await runQueryForRow(row);

        setResults(prev => ({
            ...prev,
            [id]: { logs: result.logs, loading: false, error: result.error },
        }));
    };

    const handleCancelQuery = async (row) => {
        const id = row.id;
        const queryId = queryIds[id];

        if (!queryId) {
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

    const runAllQueries = async () => {
        if (!isConnected || isRunningAll) return;
        setIsRunningAll(true);
        const loadingState = {};

        rows.forEach(row => {
            loadingState[row.id] = { logs: [], loading: true, error: null };
        });
        setResults(prev => ({ ...prev, ...loadingState }));

        const promises = rows.map(async (row) => {
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
        await Promise.all(promises);
        setIsRunningAll(false);
    };

    const runSearchQuery = async (query) => {
        setSearchError("");
        if (!isConnected) {
            setSearchData([]);
            setSearchError("Not connected — Connect first to search");
            return;
        }
        if (!query?.trim()) {
            setSearchData([]);
            setSearchError("Enter a query to search");
            return;
        }
        try {
            setSearchLoading(true);
            const { url } = connection;
            const result = await executeQuery(url, query, 0, null, null);
            const rows = Array.isArray(result?.data) ? result.data : [];
            setSearchData(rows);

            if (rows.length === 0) {
                setSearchError("No results found");
            }
        } catch (err) {
            setSearchData([]);
            setSearchError(err?.message || "Search query failed");
        } finally {
            setSearchLoading(false);
        }
    };

    const clearRowLogs = (id) => {
        setResults((prev) => ({
            ...prev,
            [id]: { logs: [], loading: false, error: null },
        }));
    };

    const resetRows = () => {
        setRows([{ id: "1-" + uuidv4(), showPanel: true, query: "", view: DEFAULT_VIEW, variables: {} }]);
        setResults({});
        setQueryIds({});
        nextId.current = 2;
    };

    const restoreRows = (queries) => {
        if (queries && queries.length > 0) {
            const restoredRows = queries.map((q, index) => ({
                id: (index + 1) + "-" + uuidv4(),
                showPanel: true,
                query: q.query,
                view: DEFAULT_VIEW,
                variables: q.variables || {}
            }));

            setRows(restoredRows);
            nextId.current = queries.length + 1;
            setResults({});
        }
    };

    return {
        rows,
        results,
        queryIds,
        cancellingQueries,
        isRunningAll,
        addRow,
        removeRow,
        updateRow,
        handleRunQuery,
        handleCancelQuery,
        runAllQueries,
        clearRowLogs,
        resetRows,
        restoreRows,
        searchData,
        searchLoading,
        searchError,
        searchQuery,
        setSearchQuery,
        runSearchQuery,
    };
};