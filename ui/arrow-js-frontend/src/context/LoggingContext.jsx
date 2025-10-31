import React, { createContext, useContext, useCallback } from "react";
import axios from "axios";
import { tableFromIPC } from "apache-arrow";
import Cookies from "js-cookie";

const LoggingContext = createContext();

export const LoggingProvider = ({ children }) => {
    /**
     * Decode Arrow payload or JSON into usable JS objects
     */
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
            } else if (Array.isArray(data.logs)) {
                tableData = data.logs;
            } else if (Array.isArray(data.data)) {
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

    /**
     * Login to get JWT
     */
    const login = async (serverUrl, username, password) => {
        try {
            const response = await axios.post(`${serverUrl}/login`, {
                username,
                password,
                claims: { org: "1" },
            });
            const jwt = response.data;
            Cookies.set("jwtToken", jwt, { path: "/", secure: true });
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

    /**
     * Generic forwarder
     */
    const forwardToDazzleDuck = async (serverUrl, username, password, query) => {
        if (!/^https?:\/\//i.test(serverUrl)) {
            throw new Error("Server URL must start with http:// or https://");
        }

        // Get token from cookies
        const token = Cookies.get("jwtToken");

        // Build headers
        const headers = {
            "Content-Type": "application/json",
            Accept: "application/json, application/vnd.apache.arrow.stream",
            Authorization: `Bearer ${token}`,
        };

        try {
            const response = await axios.post(
                serverUrl,
                typeof query === "string" ? { query } : query,
                {
                    responseType: "arraybuffer", // handles both JSON and Arrow
                    headers,
                    timeout: 20000,
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

            // Try JSON first
            if (contentType.includes("application/json") || looksLikeJson(safeBuffer)) {
                const jsonText = new TextDecoder("utf-8").decode(safeBuffer);
                const parsed = JSON.parse(jsonText);
                return { type: "json", data: parsed };
            }

            // Otherwise, it's Arrow binary
            const base64 = btoa(String.fromCharCode(...safeBuffer));
            return { type: "binary", contentType, base64 };
        } catch (err) {
            if (err.response) {
                const code = err.response.status;
                const text = err.response.statusText || "Upstream server error";
                throw new Error(`DazzleDuck responded ${code} ${text}`);
            }
            if (err.code === "ECONNABORTED") {
                throw new Error("DazzleDuck request timed out");
            }
            throw err;
        }
    };

    const looksLikeJson = (buf) => {
        try {
            const s = new TextDecoder("utf-8").decode(buf.slice(0, 256)).trim();
            return s.startsWith("{") || s.startsWith("[") || s.startsWith('"');
        } catch {
            return false;
        }
    };

    /**
     * Executes a single /query
     */
    const runQuery = async (serverUrl, username, password, query) => {
        const result = await forwardToDazzleDuck(serverUrl, username, password, query);

        if (result.type === "json") {
            return parseResponseData(result.data);
        }

        if (result.type === "binary") {
            return parseResponseData({
                binary: true,
                contentType: result.contentType,
                payloadBase64: result.base64,
            });
        }

        return [];
    };

    /**
     * Executes /query or /plan (based on splitSize)
     */
    const executeQuery = useCallback(
        async (serverUrl, username, password, query, splitSize) => {
            if (!serverUrl || !username || !password || !query) {
                throw new Error("Please fill in all fields before running the query.");
            }

            const numericSplitSize = Number(splitSize);
            let finalResults = [];

            try {
                // === /query ===
                if (numericSplitSize <= 0) {
                    let url = serverUrl.endsWith("/query")
                        ? serverUrl
                        : serverUrl.replace(/\/+$/, "") + "/query";

                    finalResults = await runQuery(url, username, password, query);
                }

                // === /plan and split queries ===
                else {
                    let planUrl = serverUrl.endsWith("/plan")
                        ? serverUrl
                        : serverUrl.replace(/\/+$/, "") + "/plan";

                    // Append ?split_size param
                    planUrl += `?split_size=${numericSplitSize}`;

                    const planResult = await forwardToDazzleDuck(planUrl, username, password, { query });
                    const splits = Array.isArray(planResult.data) ? planResult.data : [];

                    if (splits.length === 0) {
                        throw new Error("No splits returned from /plan endpoint.");
                    }

                    for (const split of splits) {
                        const sql = split.query || split.sql;
                        if (!sql) continue;

                        const splitUrl = serverUrl.endsWith("/query") ? serverUrl : serverUrl.replace(/\/+$/, "") + "/query";

                        const splitResult = await runQuery(splitUrl, username, password, sql);
                        finalResults.push(...splitResult);
                    }
                }
            } catch (err) {
                const msg = err && err.message ? err.message : String(err);
                throw new Error(`Query execution failed: ${msg}`);
            }

            return finalResults;
        }, []);

    return (
        <LoggingContext.Provider
            value={{
                executeQuery,
                login,
            }}>
            {children}
        </LoggingContext.Provider>
    );
};

export const useLogging = () => useContext(LoggingContext);
