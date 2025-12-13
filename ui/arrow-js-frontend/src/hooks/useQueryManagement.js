import { useState, useRef } from "react";
import { v4 as uuidv4 } from "uuid";

const DEFAULT_VIEW = "table";

export const useQueryManagement = (executeQuery, cancelQuery, isConnected, connection) => {
    const [rows, setRows] = useState([
        { id: "1-" + uuidv4(), showPanel: true, query: "", view: DEFAULT_VIEW },
    ]);
    const [results, setResults] = useState({});
    const [queryIds, setQueryIds] = useState({});
    const [cancellingQueries, setCancellingQueries] = useState({});
    const [isRunningAll, setIsRunningAll] = useState(false);

    const nextId = useRef(2);
    const queryIdCounter = useRef(1);

    const generateQueryId = () => {
        return queryIdCounter.current++;
    };

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

        const queryId = generateQueryId();
        setQueryIds(prev => ({ ...prev, [row.id]: queryId }));

        try {
            const result = await executeQuery(url, row.query, splitSize, null, queryId);
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

    const clearRowLogs = (id) => {
        setResults((prev) => ({
            ...prev,
            [id]: { logs: [], loading: false, error: null },
        }));
    };

    const resetRows = () => {
        setRows([{ id: "1-" + uuidv4(), showPanel: true, query: "", view: DEFAULT_VIEW }]);
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
                view: DEFAULT_VIEW
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
    };
};