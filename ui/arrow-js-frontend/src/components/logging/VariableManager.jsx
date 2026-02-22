import React, { useState, useEffect, useRef } from "react";
import { HiOutlineVariable, HiOutlinePencil, HiCheck, HiX, HiTrash } from "react-icons/hi";
import { MdTipsAndUpdates } from "react-icons/md";

const VariableManager = ({ query, variables, onUpdateVariables }) => {
    const [showModal, setShowModal] = useState(false);
    const [showTip, setShowTip] = useState(false);
    const [editingVariable, setEditingVariable] = useState(null);
    const [tempValue, setTempValue] = useState("");
    const [defaultValues, setDefaultValues] = useState({}); // Track default values from query

    const dropdownRef = useRef(null);
    const tipRef = useRef(null);

    // Close dropdown when clicking outside
    useEffect(() => {
        const handleClickOutside = (event) => {
            if (dropdownRef.current && !dropdownRef.current.contains(event.target)) {
                setShowModal(false);
            }
            if (tipRef.current && !tipRef.current.contains(event.target)) {
                setShowTip(false);
            }
        };

        document.addEventListener("mousedown", handleClickOutside);
        return () => {
            document.removeEventListener("mousedown", handleClickOutside);
        };
    }, []);

    // Regex to find variables in format {variable_name} or {variable_name:default_value}
    // Must have at least one character inside the braces, excludes {} used for string literals
    // Skips escaped braces: \{ and \}
    const variablePattern = /(?<!\\)\{([a-zA-Z_][a-zA-Z0-9_]*)(?::([^}]*))?\}(?!\})/g;

    // Extract all unique variable names from the query with default values
    const extractVariables = () => {
        const matches = query.matchAll(variablePattern);
        const foundVars = [];
        for (const match of matches) {
            const varName = match[1];
            const defaultValue = match[2] || ""; // Default value after : or empty string
            // Skip empty variable names (e.g., {} used for string literals)
            if (varName && varName.trim() !== "") {
                if (!foundVars.some(v => v.name === varName)) {
                    foundVars.push({ name: varName, defaultValue });
                }
            }
        }
        return foundVars;
    };

    // Automatically create/update variables when query changes
    useEffect(() => {
        const foundVars = extractVariables();
        if (foundVars.length === 0) {
            setDefaultValues({});
            return;
        }

        // Update default values from query
        const newDefaults = {};
        foundVars.forEach(({ name, defaultValue }) => {
            newDefaults[name] = defaultValue;
        });
        setDefaultValues(newDefaults);

        const updatedVars = { ...variables };
        let hasChanges = false;

        foundVars.forEach(({ name, defaultValue }) => {
            if (!(name in variables)) {
                // New variable - use default value if provided, otherwise empty string
                updatedVars[name] = defaultValue;
                hasChanges = true;
            } else {
                // Existing variable - only update if the value is the same as the old default
                // This means the user hasn't explicitly set a different value
                const oldDefault = defaultValues[name] || "";
                if (variables[name] === oldDefault) {
                    // User value matches old default, so update to new default
                    updatedVars[name] = defaultValue;
                    hasChanges = true;
                }
                // If user has set a different value, keep it
            }
        });

        // Remove variables that are no longer in the query
        Object.keys(variables).forEach(varName => {
            if (!foundVars.some(v => v.name === varName)) {
                delete updatedVars[varName];
                hasChanges = true;
            }
        });

        if (hasChanges) {
            onUpdateVariables(updatedVars);
        }
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [query]); // Only depend on query, not variables

    // Start editing a variable
    const startEdit = (varName, currentValue) => {
        setEditingVariable(varName);
        setTempValue(currentValue);
    };

    // Save edited variable value
    const saveEdit = () => {
        if (editingVariable) {
            onUpdateVariables({
                ...variables,
                [editingVariable]: tempValue
            });
        }
        setEditingVariable(null);
        setTempValue("");
    };

    // Cancel editing
    const cancelEdit = () => {
        setEditingVariable(null);
        setTempValue("");
    };

    // Delete a variable
    const deleteVariable = (varName) => {
        const updatedVars = { ...variables };
        delete updatedVars[varName];
        onUpdateVariables(updatedVars);
    };

    // Update variable value on key press (Enter to save, Escape to cancel)
    const handleKeyDown = (e) => {
        if (e.key === "Enter") {
            saveEdit();
        } else if (e.key === "Escape") {
            cancelEdit();
        }
    };

    const variableCount = Object.keys(variables).length;
    const foundVars = extractVariables();

    return (
        <>
            <div className="flex items-center justify-end gap-3 w-full relative">
                {/* Tip bulb icon - toggles tip on click */}
                <button
                    ref={tipRef}
                    onClick={() => setShowTip(!showTip)}
                    className="p-2 rounded-full hover:bg-gray-200 transition ease-in-out duration-300 cursor-pointer"
                    title="Show variable tips"
                >
                    {showTip ? (
                        <HiX className="text-xl text-yellow-500" />
                    ) : (
                        <MdTipsAndUpdates className="text-xl text-yellow-500" />
                    )}
                </button>

                {/* Tip popup - appears when bulb icon is clicked */}
                {showTip && (
                    <div className="absolute right-20 top-8 mt-2 bg-gray-800 text-white text-xs px-3 py-2 rounded-lg shadow-lg z-20 max-w-xs">
                        <p className="mb-1">
                            Use <code className="bg-gray-700 px-1 rounded text-yellow-400">{"{variable}"}</code> or{" "}
                            <code className="bg-gray-700 px-1 rounded text-yellow-400">{"{variable:default}"}</code>
                        </p>
                        <p className="text-gray-300">
                            Escape with <code className="bg-gray-700 px-1 rounded text-yellow-400">{"\\{variable\\}"}</code> for literal
                        </p>
                    </div>
                )}

                {/* Variables Count Indicator - button always visible */}
                <button
                    onClick={() => variableCount > 0 && setShowModal(true)}
                    disabled={variableCount === 0}
                    className={`flex items-center gap-1 text-sm font-medium py-2 px-3 rounded-md transition-all ${variableCount > 0
                        ? "bg-green-600 hover:bg-green-700 text-white cursor-pointer"
                        : "bg-gray-300 text-gray-500 cursor-not-allowed"
                        }`}
                >
                    <HiOutlineVariable className="text-base" />
                    Variables ({variableCount})
                </button>
            </div>

            {/* Variables Dropdown Panel - appears beside the button */}
            {showModal && (
                <div ref={dropdownRef} className="absolute right-13 top-29 bg-white rounded-xl shadow-2xl p-4 w-80 max-h-96 overflow-y-auto z-30 border border-gray-200">
                    <div className="flex justify-between items-center mb-2">
                        <h3 className="text-lg font-semibold text-gray-800">
                            Query Variables
                        </h3>
                        <button
                            onClick={() => setShowModal(false)}
                            className="text-gray-500 hover:text-gray-700 transition-all cursor-pointer"
                        >
                            <HiX className="text-xl" />
                        </button>
                    </div>

                    {variableCount === 0 ? (
                        <div className="text-center py-4 text-gray-500">
                            <HiOutlineVariable className="text-3xl mx-auto mb-2" />
                            <p className="text-sm">No variables defined yet.</p>
                            <p className="text-xs mt-2 text-gray-400">
                                Add variables to your query using format:
                                <code className="bg-gray-100 px-1 rounded ml-1">{"{variable_name}"}</code>
                                or
                                <code className="bg-gray-100 px-1 rounded ml-1">{"{variable_name:default}"}</code>
                            </p>
                            <p className="text-xs mt-1 text-gray-400">
                                Use {"\\{"} and {"\\}"} to escape literal braces.
                            </p>
                        </div>
                    ) : (
                        <div className="space-y-2">
                            {Object.entries(variables).map(([name, value]) => (
                                <div
                                    key={name}
                                    className="flex items-center gap-2 p-2 bg-gray-50 rounded-lg border border-gray-200"
                                >
                                    {/* Variable Name */}
                                    <div className="shrink-0 font-mono text-xs font-medium text-gray-700">
                                        {"{" + name + "}"}
                                    </div>

                                    {/* Variable Value - Display or Edit */}
                                    {editingVariable === name ? (
                                        <div className="flex-1 flex items-center gap-1">
                                            <input
                                                type="text"
                                                value={tempValue}
                                                onChange={(e) => setTempValue(e.target.value)}
                                                onKeyDown={handleKeyDown}
                                                placeholder="Enter value..."
                                                className="flex-1 px-2 py-1 text-xs border border-gray-300 rounded focus:outline-none focus:ring-1 focus:ring-blue-500"
                                                autoFocus
                                            />
                                            <button
                                                onClick={saveEdit}
                                                className="p-0.5 text-green-600 hover:text-green-700 transition"
                                                title="Save"
                                            >
                                                <HiCheck className="text-base" />
                                            </button>
                                            <button
                                                onClick={cancelEdit}
                                                className="p-0.5 text-red-600 hover:text-red-700 transition"
                                                title="Cancel"
                                            >
                                                <HiX className="text-base" />
                                            </button>
                                        </div>
                                    ) : (
                                        <div className="flex-1 flex items-center gap-1">
                                            <div
                                                className="flex-1 px-2 py-1 text-xs bg-white border border-gray-200 rounded min-h-6 flex items-center cursor-text hover:bg-gray-50 transition"
                                                title={value || "No value set"}
                                                onDoubleClick={() => startEdit(name, value)}
                                            >
                                                <span className={value ? "text-gray-800" : "text-gray-400 italic"}>
                                                    {value || "No value set"}
                                                </span>
                                            </div>
                                            <button
                                                onClick={() => startEdit(name, value)}
                                                className="p-0.5 text-blue-600 hover:text-blue-700 transition"
                                                title="Edit"
                                            >
                                                <HiOutlinePencil className="text-base" />
                                            </button>
                                            <button
                                                onClick={() => deleteVariable(name)}
                                                className="p-0.5 text-red-600 hover:text-red-700 transition"
                                                title="Delete"
                                            >
                                                <HiTrash className="text-base" />
                                            </button>
                                        </div>
                                    )}
                                </div>
                            ))}
                        </div>
                    )}
                </div>
            )}
        </>
    );
};

export default VariableManager;
