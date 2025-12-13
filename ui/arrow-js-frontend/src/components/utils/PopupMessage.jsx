import React, { useEffect } from "react";

const PopupMessage = ({ message, type, visible, onClose }) => {
    useEffect(() => {
        if (visible) {
            const timer = setTimeout(() => {
                onClose(); // Auto-close after 2 seconds
            }, 4000);
            return () => clearTimeout(timer);
        }
    }, [visible, onClose]);

    if (!visible) return null;

    return (
        <div
            className={`fixed top-5 left-1/2 transform -translate-x-1/2 z-50 px-4 py-2 rounded shadow-lg text-sm font-medium transition-all duration-300 ${type === "success"
                    ? "bg-green-100 text-green-800 border border-green-300"
                    : "bg-red-100 text-red-800 border border-red-300"
                }`}
        >
            <p>{message}</p>
        </div>
    );
};

export default PopupMessage;