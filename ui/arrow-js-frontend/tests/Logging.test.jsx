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

    it("should connect successfully when valid credentials are provided", async () => {
        setup();

        const urlInput = screen.getByPlaceholderText(/http:\/\/localhost:8081/i);
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

    it("should execute a real query after connecting", async () => {
        const { container } = setup();

        // Step 1: Open Advanced Settings (no longer has checkbox - just split size)
        const advancedSettingsBtn = screen.getByText(/advanced settings/i);
        await act(async () => {
            fireEvent.click(advancedSettingsBtn);
        });

        // Wait for advanced settings to expand
        await waitFor(() => {
            expect(
                screen.getByText(/split size/i)
            ).toBeInTheDocument();
        });

        // Step 2: Fill connection fields
        fireEvent.change(screen.getByPlaceholderText(/http:\/\/localhost:8081/i), {
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
                target: { value: "select 2 as result" },
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

        // Step 6: Wait for results table to appear and contain data
        await waitFor(
            () => {
                // First check that a table exists
                const tables = container.querySelectorAll("table");
                expect(tables.length).toBeGreaterThan(0);

                // Then check for the result value in any element
                const allText = container.textContent;
                expect(allText).toContain("2");
            },
            { timeout: 15000 }
        );
    }, 30000);

    it("should handle variable substitution in queries", async () => {
        const { container } = setup();

        // Step 1: Connect
        fireEvent.change(screen.getByPlaceholderText(/http:\/\/localhost:8081/i), {
            target: { value: SERVER_URL },
        });
        fireEvent.change(screen.getByPlaceholderText(/enter username/i), {
            target: { value: USERNAME },
        });
        fireEvent.change(screen.getByPlaceholderText(/enter password/i), {
            target: { value: PASSWORD },
        });

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

        // Step 2: Enter query with variable
        const queryTextarea = screen.getAllByPlaceholderText(
            /e\.g\. select \* from read_arrow/i
        )[0];

        await act(async () => {
            fireEvent.change(queryTextarea, {
                target: { value: "select {value} as result" },
            });
        });

        // Step 3: Verify Variables button appears
        await waitFor(() => {
            const variablesBtn = screen.getByText(/variables \(1\)/i);
            expect(variablesBtn).toBeInTheDocument();
            expect(variablesBtn).not.toBeDisabled();
        });

        // Step 4: Open Variables dropdown
        const variablesBtn = screen.getByText(/variables \(1\)/i);
        await act(async () => {
            fireEvent.click(variablesBtn);
        });

        // Step 5: Verify variable is displayed and edit it
        await waitFor(() => {
            const variableDisplay = screen.getByText(/no value set/i);
            expect(variableDisplay).toBeInTheDocument();
        });

        // Double-click to edit variable
        const variableDisplay = screen.getByText(/no value set/i);
        await act(async () => {
            fireEvent.doubleClick(variableDisplay);
        });

        // Wait for input to appear and set value
        await waitFor(() => {
            const input = container.querySelector('input[placeholder="Enter value..."]');
            expect(input).toBeInTheDocument();
            fireEvent.change(input, { target: { value: "42" } });
        });

        // Press Enter to save
        await act(async () => {
            const input = container.querySelector('input[placeholder="Enter value..."]');
            fireEvent.keyDown(input, { key: "Enter" });
        });

        // Step 6: Run query
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

        // Step 7: Wait for results with substituted value
        await waitFor(
            () => {
                const allText = container.textContent;
                expect(allText).toContain("42");
            },
            { timeout: 15000 }
        );
    }, 30000);
});
