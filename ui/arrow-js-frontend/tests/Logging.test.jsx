import { describe, it, expect, beforeAll } from "vitest";
import { render, screen, fireEvent, waitFor, act } from "@testing-library/react";
import "@testing-library/jest-dom";
import React from "react";
import Cookies from "js-cookie";
import { LoggingProvider } from "../src/context/LoggingContext";
import Logging from "../src/logging/Logging";

const SERVER_URL = "http://localhost:8081";
const USERNAME = "admin";
const PASSWORD = "admin";

describe("Logging Component Integration Tests", () => {
    beforeAll(() => {
        // Clear cookies before test run
        Cookies.remove("jwtToken");
        Cookies.remove("connectionInfo");
    });

    const setup = () =>
        render(
            <LoggingProvider>
                <Logging />
            </LoggingProvider>
        );

    it("should render all core UI elements", () => {
        setup();

        // Check tabs
        expect(screen.getByRole("button", { name: /analytics/i })).toBeInTheDocument();
        expect(screen.getByRole("button", { name: /search/i })).toBeInTheDocument();

        // Check query management buttons
        expect(screen.getByRole("button", { name: /add new query row/i })).toBeInTheDocument();
        expect(screen.getByRole("button", { name: /run queries/i })).toBeInTheDocument();

        // Check view radio buttons in first query row
        expect(screen.getByRole("radio", { name: /table/i })).toBeInTheDocument();
        expect(screen.getByRole("radio", { name: /line/i })).toBeInTheDocument();
        expect(screen.getByRole("radio", { name: /bar/i })).toBeInTheDocument();
        expect(screen.getByRole("radio", { name: /pie/i })).toBeInTheDocument();

        // Check connection panel elements
        expect(screen.getByRole("button", { name: /connect/i })).toBeInTheDocument();
        expect(screen.getByPlaceholderText(/enter server url/i)).toBeInTheDocument();
        expect(screen.getByPlaceholderText(/enter username/i)).toBeInTheDocument();
        expect(screen.getByPlaceholderText(/enter password/i)).toBeInTheDocument();
    });

    it("should show validation errors if required fields are missing", async () => {
        setup();

        const connectBtn = screen.getByRole("button", { name: /connect/i });
        await fireEvent.click(connectBtn);

        await waitFor(() => {
            expect(screen.getByText(/server url is required/i)).toBeInTheDocument();
            expect(screen.getByText(/username is required/i)).toBeInTheDocument();
            expect(screen.getByText(/password is required/i)).toBeInTheDocument();
        });
    });

    it("should connect successfully when valid credentials are provided", async () => {
        setup();

        const urlInput = screen.getByPlaceholderText(/enter server url/i);
        const usernameInput = screen.getByPlaceholderText(/enter username/i);
        const passwordInput = screen.getByPlaceholderText(/enter password/i);

        fireEvent.change(urlInput, { target: { value: SERVER_URL } });
        fireEvent.change(usernameInput, { target: { value: USERNAME } });
        fireEvent.change(passwordInput, { target: { value: PASSWORD } });

        const connectBtn = screen.getByRole("button", { name: /connect/i });
        await fireEvent.click(connectBtn);

        await waitFor(() => {
            const token = Cookies.get("jwtToken");
            const connectionInfo = Cookies.get("connectionInfo");
            expect(token).toBeDefined();
            expect(connectionInfo).toBeDefined();
        }, { timeout: 10000 });
    });

    it("should add and remove query rows", async () => {
        setup();

        const addRowBtn = screen.getByRole("button", { name: /add new query row/i });
        await fireEvent.click(addRowBtn);

        const queryTextareas = screen.getAllByPlaceholderText(/e\.g\. select \* from read_arrow/i);
        expect(queryTextareas.length).toBeGreaterThan(1);

        // Get remove buttons (they have HiOutlineX icon, no explicit name)
        const removeButtons = screen.getAllByRole("button").filter(btn =>
            btn.querySelector("svg") && !btn.textContent.trim()
        );

        if (removeButtons.length > 1) {
            fireEvent.click(removeButtons[1]); // remove second row
        }
    });

    it("should execute a real query after connecting", async () => {
        const { container } = setup();

        // Step 1: Open Advanced Settings and disable ZSTD compression
        const advancedSettingsBtn = screen.getByRole("button", {
            name: /advanced settings/i,
        });
        await act(async () => {
            fireEvent.click(advancedSettingsBtn);
        });

        // Wait for advanced settings to expand, then check the checkbox
        await waitFor(() => {
            expect(
                screen.getByText(/disable zstd compression/i)
            ).toBeInTheDocument();
        });

        const compressionCheckbox = screen.getByRole("checkbox");
        await act(async () => {
            fireEvent.click(compressionCheckbox);
        });

        // Step 2: Fill connection fields
        fireEvent.change(screen.getByPlaceholderText(/enter server url/i), {
            target: { value: SERVER_URL },
        });
        fireEvent.change(screen.getByPlaceholderText(/enter username/i), {
            target: { value: USERNAME },
        });
        fireEvent.change(screen.getByPlaceholderText(/enter password/i), {
            target: { value: PASSWORD },
        });

        // Step 3: Connect
        await act(async () => {
            fireEvent.click(screen.getByRole("button", { name: /connect/i }));
        });

        await waitFor(
            () => {
                expect(Cookies.get("jwtToken")).toBeDefined();
            },
            { timeout: 10000 }
        );

        await act(async () => {
            await new Promise((r) => setTimeout(r, 2000));
        });

        // Step 4: Enter query
        const queryTextarea = screen.getAllByPlaceholderText(
            /e\.g\. select \* from read_arrow/i
        )[0];

        await act(async () => {
            fireEvent.change(queryTextarea, {
                target: { value: "select 2" },
            });
        });

        // Step 5: Find and click Run
        const runBtn = await waitFor(() => {
            const btns = screen
                .getAllByRole("button")
                .filter((btn) => btn.textContent.trim() === "Run");
            expect(btns[0]).not.toBeDisabled();
            return btns[0];
        });

        await act(async () => {
            fireEvent.click(runBtn);
        });

        // Step 6: Wait for result "2" to appear in a table cell
        await waitFor(
            () => {
                const cells = container.querySelectorAll("td");
                const cellTexts = Array.from(cells).map((c) =>
                    c.textContent.trim()
                );
                expect(cellTexts).toContain("2");
            },
            { timeout: 15000 }
        );
    }, 30000);

    it("should switch between Analytics and Search tabs", async () => {
        setup();

        const analyticsTab = screen.getByRole("button", { name: /analytics/i });
        const searchTab = screen.getByRole("button", { name: /search/i });

        // Verify Analytics tab is active by default
        expect(analyticsTab).toHaveClass(/bg-gray-300/i);

        // Click Search tab
        await fireEvent.click(searchTab);

        await waitFor(() => {
            expect(searchTab).toHaveClass(/bg-gray-300/i);
            expect(analyticsTab).not.toHaveClass(/bg-gray-300/i);
        });

        // Click Analytics tab again
        await fireEvent.click(analyticsTab);

        await waitFor(() => {
            expect(analyticsTab).toHaveClass(/bg-gray-300/i);
            expect(searchTab).not.toHaveClass(/bg-gray-300/i);
        });
    });

    it("should show advanced settings when clicked", async () => {
        setup();

        const advancedSettingsBtn = screen.getByRole("button", { name: /advanced settings/i });
        await fireEvent.click(advancedSettingsBtn);

        await waitFor(() => {
            // Check for advanced settings elements
            expect(screen.getByText(/split size/i)).toBeInTheDocument();
            expect(screen.getByText(/disable zstd compression/i)).toBeInTheDocument();
        });
    });

    it("should show session management buttons", async () => {
        setup();

        expect(screen.getByRole("button", { name: /save session/i })).toBeInTheDocument();
        expect(screen.getByRole("button", { name: /open session/i })).toBeInTheDocument();
    });

    it("should have cancel and clear buttons in query row", async () => {
        setup();

        // Find query action buttons
        const buttons = screen.getAllByRole("button");

        // Look for Cancel button
        const cancelBtn = buttons.find(btn =>
            btn.textContent.trim() === "Cancel" || btn.textContent.trim() === "Canceling..."
        );
        expect(cancelBtn).toBeInTheDocument();

        // Look for Clear button
        const clearBtn = buttons.find(btn => btn.textContent.trim() === "Clear");
        expect(clearBtn).toBeInTheDocument();
    });
});
