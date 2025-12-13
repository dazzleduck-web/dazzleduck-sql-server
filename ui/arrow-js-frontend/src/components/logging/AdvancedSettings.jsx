import React from "react";
import ClaimsManager from "./ClaimsManager";

const AdvancedSettings = ({
    showAdvanced,
    setShowAdvanced,
    claims,
    addClaim,
    removeClaim,
    updateClaim,
    register,
    errors,
    isSubmitted
}) => {
    return (
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
                    {/* Claims Section */}
                    <ClaimsManager
                        claims={claims}
                        addClaim={addClaim}
                        removeClaim={removeClaim}
                        updateClaim={updateClaim}
                    />

                    {/* Split Size */}
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
    );
};

export default AdvancedSettings;