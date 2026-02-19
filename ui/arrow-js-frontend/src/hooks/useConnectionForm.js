import { useState, useEffect } from "react";
import { useForm } from 'react-hook-form';

export const useConnectionForm = (login, logout, connectionInfo) => {
    const [claims, setClaims] = useState([{ key: "", value: "" }]);
    const [connection, setConnection] = useState({
        url: "",
        username: "",
        password: "",
        claims: { cluster: "" },
        splitSize: 0,
    });
    const [isConnected, setIsConnected] = useState(false);
    const [loginError, setLoginError] = useState(null);
    const [showAdvanced, setShowAdvanced] = useState(false);

    const {
        register,
        handleSubmit,
        watch,
        setValue,
        formState: { errors, isSubmitted, isSubmitting }
    } = useForm({
        defaultValues: {
            url: "",
            username: "",
            password: "",
            cluster: "",
            splitSize: 0,
        },
    });

    const watchedFields = watch();

    // Load connection info on mount
    useEffect(() => {
        if (connectionInfo) {
            setValue("url", connectionInfo.serverUrl);
            setValue("username", connectionInfo.username);
            setValue("splitSize", connectionInfo.splitSize || 0);

            if (connectionInfo.claims) {
                const claimsArray = Object.entries(connectionInfo.claims).map(([key, value]) => ({
                    key,
                    value
                }));
                setClaims(claimsArray.length > 0 ? claimsArray : [{ key: "", value: "" }]);
            }

            setConnection({
                url: connectionInfo.serverUrl,
                username: connectionInfo.username,
                password: "",
                claims: connectionInfo.claims,
                splitSize: connectionInfo.splitSize || 0,
            });
        }
    }, [connectionInfo, setValue]);

    // Disconnect when connection settings change
    useEffect(() => {
        if (connectionInfo &&
            watchedFields.url === connectionInfo.serverUrl &&
            watchedFields.username === connectionInfo.username &&
            watchedFields.splitSize === connectionInfo.splitSize) {

            const currentClaimsStr = JSON.stringify(claims.filter(c => c.key.trim() !== ""));
            const savedClaimsStr = JSON.stringify(
                Object.entries(connectionInfo.claims || {}).map(([key, value]) => ({ key, value }))
            );
            if (currentClaimsStr === savedClaimsStr) {
                return;
            }
        }
        setIsConnected(false);
    }, [watchedFields.url, watchedFields.username, watchedFields.password, watchedFields.splitSize, claims, connectionInfo]);

    const addClaim = () => {
        setClaims(prev => [...prev, { key: "", value: "" }]);
    };

    const removeClaim = (index) => {
        setClaims(prev => prev.filter((_, i) => i !== index));
    };

    const updateClaim = (index, field, value) => {
        setClaims(prev =>
            prev.map((item, i) =>
                i === index ? { ...item, [field]: value } : item
            )
        );
    };

    const onSubmit = async (data) => {
        setLoginError(null);

        const claimsObject = {};
        claims.forEach(c => {
            if (c.key.trim() !== "") {
                claimsObject[c.key.trim()] = c.value.trim();
            }
        });

        const newConnection = {
            url: data.url,
            username: data.username,
            password: data.password,
            claims: claimsObject,
            splitSize: data.splitSize ?? 0,
        };

        setConnection(newConnection);

        try {
            await login(data.url, data.username, data.password, data.splitSize, claimsObject);
            setIsConnected(true);
        } catch (err) {
            setIsConnected(false);
            setLoginError(err?.message || "Login failed");
        }
    };

    const handleLogout = () => {
        logout();
        setIsConnected(false);
    };

    const populateConnectionData = (sessionData) => {
        setIsConnected(false);
        setValue("url", sessionData.connection.serverUrl);
        setValue("username", sessionData.connection.username);
        setValue("password", "");
        setValue("splitSize", sessionData.connection.splitSize || 0);

        if (sessionData.connection.claims) {
            const claimsArray = Object.entries(sessionData.connection.claims).map(([key, value]) => ({
                key,
                value
            }));
            setClaims(claimsArray.length > 0 ? claimsArray : [{ key: "", value: "" }]);
        }
    };

    return {
        // Form
        register,
        handleSubmit,
        setValue,
        errors,
        isSubmitted,
        isSubmitting,
        watchedFields,

        // Claims
        claims,
        addClaim,
        removeClaim,
        updateClaim,

        // Connection
        connection,
        isConnected,
        loginError,

        // Advanced settings
        showAdvanced,
        setShowAdvanced,

        // Actions
        onSubmit,
        handleLogout,
        populateConnectionData,
    };
};