import React, { useState } from "react";
import "../App.css";
import { useLogging } from "../context/LoggingContext";
import { useQueryManagement } from "../hooks/useQueryManagement";
import { useConnectionForm } from "../hooks/useConnectionForm";
import { useSessionManagement } from "../hooks/useSessionManagement";
import ConnectionPanel from "../components/logging/ConnectionPanel";
import QueryRow from "../components/logging/QueryRow";
import PopupMessage from "../components/utils/PopupMessage";

const Logging = () => {
    const {
        executeQuery,
        login,
        logout,
        cancelQuery,
        saveSession,
        loadSession,
        restoreSession,
        connectionInfo,
    } = useLogging();

    const [popup, setPopup] = useState({
        message: "",
        type: "",
        visible: false,
    });

    const showPopup = (message, type = "success") => {
        setPopup({
            message,
            type,
            visible: true,
        });
    };

    const [showConnection, setShowConnection] = useState(true);

    // Connection form management
    const connectionForm = useConnectionForm(login, logout, connectionInfo);

    // Query management
    const queryManagement = useQueryManagement(
        executeQuery,
        cancelQuery,
        connectionForm.isConnected,
        connectionForm.connection
    );

    // Session management
    const sessionManagement = useSessionManagement(
        saveSession,
        loadSession,
        restoreSession,
        queryManagement.rows,
        connectionForm.populateConnectionData,
        queryManagement.restoreRows,
        showPopup
    );

    // Handle logout with query reset
    const handleLogout = () => {
        connectionForm.handleLogout();
        queryManagement.resetRows();
    };

    return (
        <div className="relative min-h-screen bg-gradient-to-br from-gray-50 to-gray-200 p-8 space-y-10">
            {/* Connection Panel */}
            <ConnectionPanel
                showConnection={showConnection}
                setShowConnection={setShowConnection}
                isConnected={connectionForm.isConnected}
                register={connectionForm.register}
                handleSubmit={connectionForm.handleSubmit}
                onSubmit={connectionForm.onSubmit}
                errors={connectionForm.errors}
                isSubmitted={connectionForm.isSubmitted}
                isSubmitting={connectionForm.isSubmitting}
                loginError={connectionForm.loginError}
                handleLogout={handleLogout}
                showAdvanced={connectionForm.showAdvanced}
                setShowAdvanced={connectionForm.setShowAdvanced}
                claims={connectionForm.claims}
                addClaim={connectionForm.addClaim}
                removeClaim={connectionForm.removeClaim}
                updateClaim={connectionForm.updateClaim}
                fileInputRef={sessionManagement.fileInputRef}
                handleSaveSession={sessionManagement.handleSaveSession}
                handleOpenSession={sessionManagement.handleOpenSession}
                openFileDialog={sessionManagement.openFileDialog}
            />

            {/* Query Rows */}
            {queryManagement.rows.map((row) => (
                <QueryRow
                    key={row.id}
                    row={row}
                    result={queryManagement.results[row.id]}
                    queryId={queryManagement.queryIds[row.id]}
                    isConnected={connectionForm.isConnected}
                    isCancelling={queryManagement.cancellingQueries[row.id]}
                    totalRows={queryManagement.rows.length}
                    updateRow={queryManagement.updateRow}
                    removeRow={queryManagement.removeRow}
                    handleRunQuery={queryManagement.handleRunQuery}
                    handleCancelQuery={queryManagement.handleCancelQuery}
                    clearRowLogs={queryManagement.clearRowLogs}
                />
            ))}

            {/* Add Row and Run All Queries Buttons */}
            <div className="flex justify-evenly mt-10 gap-5">
                <button
                    onClick={queryManagement.addRow}
                    className="bg-green-600 hover:bg-green-700 text-white font-semibold py-3 px-6 rounded-md transition duration-300 cursor-pointer"
                >
                    Add New Query Row
                </button>

                <button
                    onClick={queryManagement.runAllQueries}
                    disabled={!connectionForm.isConnected || queryManagement.isRunningAll}
                    className="bg-blue-600 hover:bg-blue-700 text-white font-semibold py-3 px-6 rounded-md disabled:opacity-50 transition duration-300 cursor-pointer"
                >
                    {queryManagement.isRunningAll ? "Running..." : "Run Queries"}
                </button>
            </div>

            <PopupMessage
                message={popup.message}
                type={popup.type}
                visible={popup.visible}
                onClose={() =>
                    setPopup({ message: "", type: "", visible: false })
                }
            />
        </div>
    );
};

export default Logging;