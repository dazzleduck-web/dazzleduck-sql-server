import React, { createContext, useContext, useCallback } from "react";
import axios from "axios";
import { tableFromIPC } from "apache-arrow";
import Cookies from "js-cookie";

const LoggingContext = createContext();

export const LoggingProvider = ({ children }) => {
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
    const login = async (serverUrl, username, password) => {
        try {
            const response = await axios.post(`${serverUrl}/login`, {
                username,
                password,
                claims: { org: "1" },
            });
            const jwt = `${response.data.tokenType} ${response.data.accessToken}`;
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
    const forwardToDazzleDuck = async (serverUrl, query, jwt) => {
        if (!/^https?:\/\//i.test(serverUrl)) {
            throw new Error("Server URL must start with http:// or https://");
        }

        const token = Cookies.get("jwtToken");

        const headers = {
            "Content-Type": "application/json",
            Accept: "application/json, application/vnd.apache.arrow.stream",
            Authorization: token !== undefined || token !== null ? `Bearer ${token}` : jwt,
        };

        try {
            const response = await axios.post(
                serverUrl,
                typeof query === "string" ? { query } : query,
                {
                    responseType: "arraybuffer",
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

            if (contentType.includes("application/json") || looksLikeJson(safeBuffer)) {
                const jsonText = new TextDecoder("utf-8").decode(safeBuffer);
                const parsed = JSON.parse(jsonText);
                return { type: "json", data: parsed };
            }

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

    // --- Execute Query ---
    const executeQuery = useCallback(async (serverUrl, query, splitSize, jwt) => {
        if (!serverUrl || !query) {
            throw new Error("Please fill in all fields before running the query.");
        }

        const numericSplitSize = Number(splitSize);
        let finalResults = [];

        try {
            if (numericSplitSize <= 0) {
                let url = serverUrl.endsWith("/query")
                    ? serverUrl
                    : serverUrl.replace(/\/+$/, "") + "/query";

                const result = await forwardToDazzleDuck(url, query, jwt);
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

                const planResult = await forwardToDazzleDuck(planUrl, { query }, jwt);
                const splits = Array.isArray(planResult.data) ? planResult.data : [];

                if (splits.length === 0) throw new Error("No splits returned from /plan endpoint.");

                for (const split of splits) {
                    const sql = split.query || split.sql;
                    if (!sql) continue;

                    const splitUrl = serverUrl.endsWith("/query")
                        ? serverUrl
                        : serverUrl.replace(/\/+$/, "") + "/query";

                    const splitResult = await forwardToDazzleDuck(splitUrl, sql, jwt);
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

            return normalized;
        } catch (err) {
            const msg = err?.message || String(err);
            throw new Error(`Query execution failed: ${msg}`);
        }
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
