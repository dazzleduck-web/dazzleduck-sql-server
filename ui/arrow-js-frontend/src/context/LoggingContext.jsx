import React, { createContext, useContext, useCallback, useState, useEffect } from "react";
import axios from "axios";
import { tableFromIPC } from "apache-arrow";
import Cookies from "js-cookie";

const LoggingContext = createContext();

export const LoggingProvider = ({ children }) => {
    const FIVE_MINUTES_MS = 300000;

    // Session state
    const [connectionInfo, setConnectionInfo] = useState(null);

    // Load connection info from cookies on mount
    useEffect(() => {
        const savedConnection = Cookies.get("connectionInfo");
        if (savedConnection) {
            try {
                setConnectionInfo(JSON.parse(savedConnection));
            } catch (e) {
                console.error("Failed to parse saved connection info:", e);
            }
        }
    }, []);

    // --- Normalization / Parsing ---
    const parseResponseData = (data) => {
        let tableData = [];
        try {
            if (data?.payloadBase64 || data?.binary || data?.base64) {
                const base64 = data.payloadBase64 || data.base64 || data.binary;
                const binary = Uint8Array.from(atob(base64), (c) => c.charCodeAt(0));

                const table = tableFromIPC(binary);
                const rows = Array.from(table.toArray());
                const columns = table.schema.fields.map((f) => f.name);

                tableData = rows.map((row) => {
                    const obj = {};
                    columns.forEach((col) => (obj[col] = row[col]));
                    return obj;
                });
            } else if (Array.isArray(data?.logs)) {
                tableData = data.logs;
            } else if (Array.isArray(data?.data)) {
                tableData = data.data;
            } else if (Array.isArray(data)) {
                tableData = data;
            } else if (typeof data === "object" && data !== null) {
                tableData = [data];
            }
        } catch (e) {
            throw new Error("Error decoding response: " + e.message);
        }
        return tableData;
    };

    // --- Login ---
    const login = async (serverUrl, username, password, splitSize, claims) => {
        try {
            const response = await axios.post(`${serverUrl.trim()}/login`, {
                username,
                password,
                claims: claims,
            });
            const jwt = `${response.data.tokenType} ${response.data.accessToken}`;

            // Save JWT with expiration
            Cookies.set("jwtToken", jwt, { path: "/", secure: true });

            // Save connection info (without password for security in cookies)
            const connInfo = {
                serverUrl: serverUrl.trim(),
                username,
                claims,
                splitSize, // Default, will be updated when executing queries
                loginTime: new Date().toISOString()
            };

            Cookies.set("connectionInfo", JSON.stringify(connInfo), { path: "/" });

            setConnectionInfo(connInfo);

            return jwt;
        } catch (err) {
            if (err.response) {
                const code = err.response.status;
                const text = err.response.statusText || "Upstream server error";
                throw new Error(`Failed with response ${code} ${text}`);
            }
            throw err;
        }
    };

    // --- Logout ---
    const logout = useCallback(() => {
        Cookies.remove("jwtToken", { path: "/" });
        Cookies.remove("connectionInfo", { path: "/" });
        setConnectionInfo(null);
    }, []);


    // --- Helper: Check jwt and logout ---
    const requireJwtOrLogout = (jwt) => {
        const token = Cookies.get("jwtToken");
        if (!token && !jwt) {
            logout();
            throw new Error("Session expired. Please login again.");
        }
        return token;
    };

    // --- Helper: detect JSON ---
    const looksLikeJson = (buf) => {
        try {
            const s = new TextDecoder("utf-8").decode(buf.slice(0, 256)).trim();
            return s.startsWith("{") || s.startsWith("[") || s.startsWith('"');
        } catch {
            return false;
        }
    };

    // --- Core forwarder ---
    const forwardToDazzleDuck = async (serverUrl, query, jwt, queryId = null) => {
        if (!/^https?:\/\//i.test(serverUrl.trim())) {
            throw new Error("Server URL must start with http:// or https://");
        }

        const token = requireJwtOrLogout(jwt);

        const headers = {
            "Content-Type": "application/json",
            Accept: "application/json, application/vnd.apache.arrow.stream",
            Authorization: token ? token : jwt,
        };

        // Prepare request body with queryId (as number)
        let requestBody;
        if (typeof query === "string") {
            requestBody = queryId !== null ? { query, id: queryId } : { query };
        } else {
            requestBody = queryId !== null ? { ...query, id: queryId } : query;
        }

        try {
            const response = await axios.post(
                serverUrl,
                requestBody,
                {
                    responseType: "arraybuffer",
                    headers,
                    timeout: FIVE_MINUTES_MS,
                    maxContentLength: 50 * 1024 * 1024,
                }
            );

            const contentType = (response.headers["content-type"] || "").toLowerCase();
            let buffer;

            if (response.data instanceof ArrayBuffer) {
                buffer = new Uint8Array(response.data);
            } else if (ArrayBuffer.isView(response.data)) {
                buffer = new Uint8Array(response.data.buffer);
            } else if (typeof response.data === "string") {
                buffer = new TextEncoder().encode(response.data);
            } else {
                throw new Error("Unexpected response data type");
            }

            const safeBuffer = buffer.slice(0);

            if (contentType.includes("application/json") || looksLikeJson(safeBuffer)) {
                const jsonText = new TextDecoder("utf-8").decode(safeBuffer);
                const parsed = JSON.parse(jsonText);
                return { type: "json", data: parsed };
            }

            const base64 = btoa(String.fromCharCode(...safeBuffer));
            return { type: "binary", contentType, base64 };
        } catch (err) {
            throw err;
        }
    };

    // --- Execute Query ---
    const executeQuery = useCallback(async (serverUrl, query, splitSize, jwt, queryId) => {
        const cleanUrl = serverUrl.trim();
        if (!serverUrl || !query) {
            throw new Error("Please fill in all fields before running the query.");
        }

        const numericSplitSize = Number(splitSize);
        let finalResults = [];

        try {
            if (numericSplitSize <= 0) {
                let url = cleanUrl.endsWith("/query")
                    ? cleanUrl
                    : cleanUrl.replace(/\/+$/, "") + "/query";

                const result = await forwardToDazzleDuck(url, query, jwt, queryId);
                finalResults = result.type === "json"
                    ? parseResponseData(result.data)
                    : parseResponseData({
                        binary: true,
                        contentType: result.contentType,
                        payloadBase64: result.base64,
                    });
            } else {
                let planUrl = serverUrl.endsWith("/plan")
                    ? serverUrl
                    : serverUrl.replace(/\/+$/, "") + "/plan";
                planUrl += `?split_size=${numericSplitSize}`;

                const planResult = await forwardToDazzleDuck(planUrl, { query }, jwt, queryId);
                const splits = Array.isArray(planResult.data) ? planResult.data : [];

                if (splits.length === 0) throw new Error("No splits returned from /plan endpoint.");

                for (const split of splits) {
                    const sql = split.query || split.sql;
                    if (!sql) continue;

                    const splitUrl = serverUrl.endsWith("/query")
                        ? serverUrl
                        : serverUrl.replace(/\/+$/, "") + "/query";

                    const splitResult = await forwardToDazzleDuck(splitUrl, sql, jwt, queryId);
                    const normalized = splitResult.type === "json"
                        ? parseResponseData(splitResult.data)
                        : parseResponseData({
                            binary: true,
                            contentType: splitResult.contentType,
                            payloadBase64: splitResult.base64,
                        });

                    finalResults.push(...normalized);
                }
            }

            // Normalize again (for safety)
            const normalized = Array.isArray(finalResults)
                ? finalResults
                : parseResponseData(finalResults);

            // Update connection info with splitSize
            if (connectionInfo) {
                const updated = { ...connectionInfo, splitSize: numericSplitSize };
                setConnectionInfo(updated);
                Cookies.set("connectionInfo", JSON.stringify(updated), { path: "/" });
            }

            return { data: normalized, queryId };
        } catch (err) {
            const msg = err?.message || String(err);
            throw new Error(`Query execution failed: ${msg}`);
        }
    }, [connectionInfo]);

    // --- Cancel Query ---
    const cancelQuery = useCallback(async (serverUrl, query, queryId) => {
        if (!serverUrl || !queryId) {
            throw new Error("Server URL and Query ID are required to cancel.");
        }

        const token = requireJwtOrLogout();

        const headers = {
            "Content-Type": "application/json",
            Accept: "application/json",
            Authorization: token,
        };

        try {
            let cancelUrl = serverUrl.endsWith("/cancel")
                ? serverUrl
                : serverUrl.replace(/\/+$/, "") + "/cancel";

            const response = await axios.post(
                cancelUrl,
                { query, id: queryId },
                {
                    headers,
                    timeout: 5000,
                }
            );

            // Accept status codes 200, 202, 409 as success (matching the test)
            if ([200, 202, 409].includes(response.status)) {
                return { success: true, status: response.status };
            }

            return { success: false, status: response.status };
        } catch (err) {
            const msg = err?.message || String(err);
            throw new Error(`Cancel request failed: ${msg}`);
        }
    }, []);

    // --- Save Session ---
    const saveSession = useCallback((currentQueries = []) => {
        if (!connectionInfo) {
            throw new Error("No active connection to save");
        }

        return {
            version: "1.0",
            savedAt: new Date().toISOString(),
            connection: {
                serverUrl: connectionInfo.serverUrl,
                username: connectionInfo.username,
                claims: connectionInfo.claims,
                splitSize: connectionInfo.splitSize || 0
            },
            queries: currentQueries.map(q => ({
                query: q.query
            }))
        };
    }, [connectionInfo]);

    // --- Load Session ---
    const loadSession = useCallback((file) => {
        return new Promise((resolve, reject) => {
            const reader = new FileReader();

            reader.onload = (e) => {
                try {
                    const sessionData = JSON.parse(e.target.result);

                    // Validate session data structure
                    if (!sessionData.connection || !sessionData.queries) {
                        throw new Error("Invalid session file format");
                    }

                    resolve(sessionData);
                } catch (err) {
                    reject(new Error("Failed to parse session file: " + err.message));
                }
            };

            reader.onerror = () => {
                reject(new Error("Failed to read session file"));
            };

            reader.readAsText(file);
        });
    }, []);

    // --- Restore Session ---
    const restoreSession = useCallback(async (sessionData) => {
        const { connection, queries } = sessionData;

        // Just restore UI state, NOT authentication
        const connInfo = {
            serverUrl: connection.serverUrl,
            username: connection.username,
            claims: connection.claims,
            splitSize: connection.splitSize,
            loginTime: new Date().toISOString()
        };

        setConnectionInfo(connInfo);
        Cookies.set("connectionInfo", JSON.stringify(connInfo), { path: "/" });

        return {
            connection: connInfo,
            queriesRestored: queries?.length || 0
        };
    }, []);

    return (
        <LoggingContext.Provider
            value={{
                executeQuery,
                login,
                logout,
                cancelQuery,
                saveSession,
                loadSession,
                restoreSession,
                connectionInfo,
            }}>
            {children}
        </LoggingContext.Provider>
    );
};

export const useLogging = () => useContext(LoggingContext);