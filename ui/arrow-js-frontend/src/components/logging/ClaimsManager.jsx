import React from "react";

const ClaimsManager = ({ claims, addClaim, removeClaim, updateClaim }) => {
    const allowedKeys = ["cluster", "orgId", "database", "schema", "table", "path", "function"];

    return (
        <div>
            <label className="block text-sm font-medium text-gray-700 mb-1">Claims</label>

            {claims.map((item, index) => {
                const isValidKey = item.key.trim() === "" || allowedKeys.includes(item.key.trim());

                const filteredSuggestions =
                    item.key.trim() && !allowedKeys.includes(item.key.trim())
                        ? allowedKeys.filter((k) =>
                            k.toLowerCase().includes(item.key.toLowerCase())
                        )
                        : [];

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
                                    className={`w-full border rounded-lg py-1 px-2 ${item.key.trim() === "" ? "border-gray-400" : isValidKey ? "border-green-500" : "border-red-500"}`} />

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
    );
};

export default ClaimsManager;