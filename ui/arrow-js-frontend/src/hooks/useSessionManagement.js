import { useRef } from "react";

export const useSessionManagement = (
    saveSession,
    loadSession,
    restoreSession,
    rows,
    populateConnectionData,
    restoreRows,
    showPopup
) => {
    const fileInputRef = useRef(null);

    const handleSaveSession = async () => {
        try {
            const currentQueries = rows.map(row => ({ query: row.query }));
            const sessionData = saveSession(currentQueries);
            const json = JSON.stringify(sessionData, null, 2);

            // Use File System Access API if available (modern browsers)
            if (window.showSaveFilePicker) {
                const fileHandle = await window.showSaveFilePicker({
                    suggestedName: `dazzleduck-session-${new Date().toISOString().split('T')[0]}.json`,
                    types: [
                        {
                            description: "Dazzleduck-UI Session File",
                            accept: { "application/json": [".json"] },
                        },
                    ],
                });

                const writable = await fileHandle.createWritable();
                await writable.write(json);
                await writable.close();
            } else {
                // Fallback for other browsers: create a download link
                const blob = new Blob([json], { type: "application/json" });
                const url = URL.createObjectURL(blob);

                const a = document.createElement("a");
                a.href = url;
                a.download = `dazzleduck-session-${new Date().toISOString().split('T')[0]}.json`;
                document.body.appendChild(a);
                a.click();
                document.body.removeChild(a);

                URL.revokeObjectURL(url);
            }

            showPopup("Session saved successfully!", "success");
        } catch (err) {
            if (err?.name === "AbortError") return;
            showPopup("Failed to save session: " + err.message, "error");
        }
    };

    const handleOpenSession = async (event) => {
        const file = event.target.files?.[0];
        if (!file) return;

        try {
            const sessionData = await loadSession(file);
            await restoreSession(sessionData);

            populateConnectionData(sessionData);
            restoreRows(sessionData.queries || []);

            showPopup(`Session loaded. Please enter password and click Connect.`, "success");

            event.target.value = '';
        } catch (err) {
            showPopup("Failed to load session: " + err.message, "error");
            event.target.value = '';
        }
    };

    const openFileDialog = () => {
        fileInputRef.current?.click();
    };

    return {
        fileInputRef,
        handleSaveSession,
        handleOpenSession,
        openFileDialog,
    };
};
