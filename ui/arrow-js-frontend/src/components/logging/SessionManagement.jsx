import React from "react";

const SessionManagement = ({
    isConnected,
    fileInputRef,
    handleSaveSession,
    handleOpenSession,
    openFileDialog
}) => {
    return (
        <div className="mt-4 pt-4 border-t border-gray-300 space-y-2">
            <h3 className="text-sm font-semibold text-gray-700 mb-2">Session Management</h3>

            <button
                onClick={handleSaveSession}
                disabled={!isConnected}
                className="w-full bg-green-600 hover:bg-green-700 text-white font-medium py-2 rounded-lg transition disabled:opacity-50 disabled:cursor-not-allowed cursor-pointer"
            >
                Save Session
            </button>

            <input
                ref={fileInputRef}
                type="file"
                accept=".json"
                onChange={handleOpenSession}
                className="hidden"
            />

            <button
                onClick={openFileDialog}
                className="w-full bg-purple-600 hover:bg-purple-700 text-white font-medium py-2 rounded-lg transition cursor-pointer"
            >
                Open Session
            </button>
        </div>
    );
};

export default SessionManagement;