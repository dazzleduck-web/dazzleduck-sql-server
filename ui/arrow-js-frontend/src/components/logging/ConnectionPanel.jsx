import React from "react";
import { HiOutlineX, HiOutlineChevronDoubleRight } from "react-icons/hi";
import { AiFillCloseCircle, AiFillCheckCircle } from "react-icons/ai";
import AdvancedSettings from "./AdvancedSettings";
import SessionManagement from "./SessionManagement";

const ConnectionPanel = ({
    showConnection,
    setShowConnection,
    isConnected,
    register,
    handleSubmit,
    onSubmit,
    errors,
    isSubmitted,
    isSubmitting,
    loginError,
    handleLogout,
    showAdvanced,
    setShowAdvanced,
    claims,
    addClaim,
    removeClaim,
    updateClaim,
    fileInputRef,
    handleSaveSession,
    handleOpenSession,
    openFileDialog,
    disableCompression,
    setDisableCompression
}) => {
    return (
        <>
            {/* Toggle Button */}
            <button
                onClick={() => setShowConnection(!showConnection)}
                className="fixed top-30.5 left-[1.5px] z-30 text-xl bg-gray-600 text-white p-1 rounded-full shadow hover:bg-gray-700 transition cursor-pointer"
            >
                {showConnection ? <HiOutlineX /> : <HiOutlineChevronDoubleRight />}
            </button>

            {/* Connection Panel */}
            <div
                className={`fixed top-30 left-8 w-90 bg-white shadow-2xl rounded-2xl border border-gray-600 transform transition-transform duration-300 ease-in-out px-6 py-4 z-20 max-h-[80vh] overflow-y-auto scrollbar-hidden ${showConnection ? "translate-x-0" : "-translate-x-full"
                    }`}
            >
                <div className="flex items-center gap-2 mb-3">
                    <h2 className="text-2xl font-semibold text-gray-800">
                        Connection Settings
                    </h2>

                    {/* Status circle */}
                    <div className="mt-2">
                        {isConnected ? (
                            <AiFillCheckCircle className="text-green-600" size={20} />
                        ) : (
                            <AiFillCloseCircle className="text-red-600" size={20} />
                        )}
                    </div>
                </div>

                <form className="space-y-4" onSubmit={handleSubmit(onSubmit)}>
                    {/* Server URL */}
                    <div>
                        <label className="block text-sm font-medium text-gray-700 mb-1">
                            Server URL
                        </label>
                        <input
                            type="text"
                            {...register("url", { required: "Server URL is required" })}
                            placeholder="Enter Server URL"
                            className="w-full border border-gray-400 rounded-lg py-1 px-2"
                        />
                        {isSubmitted && errors.url && (
                            <p className="text-red-500 text-sm">{errors.url.message}</p>
                        )}
                    </div>

                    {/* Username */}
                    <div>
                        <label className="block text-sm font-medium text-gray-700 mb-1">
                            Username
                        </label>
                        <input
                            type="text"
                            {...register("username", { required: "Username is required" })}
                            placeholder="Enter username"
                            className="w-full border border-gray-400 rounded-lg py-1 px-2"
                        />
                        {isSubmitted && errors.username && (
                            <p className="text-red-500 text-sm">{errors.username.message}</p>
                        )}
                    </div>

                    {/* Password */}
                    <div>
                        <label className="block text-sm font-medium text-gray-700 mb-1">
                            Password
                        </label>
                        <input
                            type="password"
                            {...register("password", { required: "Password is required" })}
                            placeholder="Enter password"
                            className="w-full border border-gray-400 rounded-lg py-1 px-2"
                        />
                        {isSubmitted && errors.password && (
                            <p className="text-red-500 text-sm">{errors.password.message}</p>
                        )}
                    </div>

                    {/* Advanced Settings */}
                    <AdvancedSettings
                        showAdvanced={showAdvanced}
                        setShowAdvanced={setShowAdvanced}
                        claims={claims}
                        addClaim={addClaim}
                        removeClaim={removeClaim}
                        updateClaim={updateClaim}
                        register={register}
                        errors={errors}
                        isSubmitted={isSubmitted}
                        disableCompression={disableCompression}
                        setDisableCompression={setDisableCompression}
                    />

                    {/* Login Error */}
                    {loginError && (
                        <p className="text-red-600 text-sm">{loginError}</p>
                    )}

                    {/* Connect/Logout Buttons */}
                    <div className="flex gap-2">
                        <button
                            type="submit"
                            className={`flex-1 ${isConnected ? "bg-blue-300 hover:bg-blue-400" : "bg-blue-600 hover:bg-blue-700 cursor-pointer"} text-white font-medium py-2 rounded-lg mt-1 transition`}
                            disabled={isSubmitting || isConnected}
                        >
                            {isSubmitting ? "Connecting..." : "Connect"}
                        </button>

                        {isConnected && (
                            <button
                                type="button"
                                onClick={handleLogout}
                                className="flex-1 bg-red-600 hover:bg-red-700 text-white font-medium py-2 rounded-lg mt-1 transition cursor-pointer"
                            >
                                Logout
                            </button>
                        )}
                    </div>
                </form>

                {/* Session Management */}
                <SessionManagement
                    isConnected={isConnected}
                    fileInputRef={fileInputRef}
                    handleSaveSession={handleSaveSession}
                    handleOpenSession={handleOpenSession}
                    openFileDialog={openFileDialog}
                />
            </div>
        </>
    );
};

export default ConnectionPanel;