// --- Universal Timestamp Normalizer ---
// Detects and converts numeric, string, ISO, Arrow Long timestamps automatically
export function formatPossibleDate(val) {
    if (val == null || val === "") return val;

    // Arrow Long (DuckDB / Arrow timestamps)
    if (typeof val === "object" && val.low !== undefined && val.high !== undefined) {
        try {
            const millis = val.low + val.high * 4294967296;
            return new Date(millis).toLocaleString();
        } catch {
            return val;
        }
    }

    // Numeric string timestamps ("1735689600000", "1735689600")
    if (typeof val === "string" && /^\d{10,16}$/.test(val.trim())) {
        const num = Number(val);
        if (!isNaN(num)) {
            const ms = val.length === 10 ? num * 1000 : num;
            return new Date(ms).toLocaleString();
        }
    }

    // Numeric timestamps (1735689600000)
    if (typeof val === "number" && val > 1000000000) {
        const ms = val < 100000000000 ? val * 1000 : val;
        try {
            return new Date(ms).toLocaleString();
        } catch {
            return val;
        }
    }

    // ISO or date-like string ("2025-01-01", "2025-01-01T00:00:00")
    if (typeof val === "string" && /\d{4}-\d{2}-\d{2}/.test(val)) {
        try {
            return new Date(val).toLocaleString();
        } catch {
            return val;
        }
    }

    return val;
}
