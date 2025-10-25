import { describe, it, expect, beforeAll, vi } from "vitest";
import { render, screen, fireEvent, waitFor } from "@testing-library/react";
import "@testing-library/jest-dom";
import React from "react";
import Cookies from "js-cookie";
import { LoggingProvider } from "../src/context/LoggingContext";
import Logging from "../src/logging/Logging";

const SERVER_URL = "http://localhost:8080";
const USERNAME = "admin";
const PASSWORD = "admin";

describe("Logging Component Integration Tests", () => {
    beforeAll(() => {
        // Clear cookies before test run
        Cookies.remove("jwtToken");
    });

    const setup = () =>
        render(
            <LoggingProvider>
                <Logging />
            </LoggingProvider>
        );

    it("should render all core UI elements", () => {
        setup();

        expect(
            screen.getByRole("button", { name: /connect/i })
        ).toBeInTheDocument();

        expect(
            screen.getByRole("button", { name: /add new query row/i })
        ).toBeInTheDocument();

        expect(
            screen.getByRole("button", { name: /Run Query/i })
        ).toBeInTheDocument();

        expect(
            screen.getByRole("radio", { name: /Table/i })
        ).toBeInTheDocument();

        expect(
            screen.getByRole("radio", { name: /Line/i })
        ).toBeInTheDocument();
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
        render(
            <LoggingProvider>
                <Logging />
            </LoggingProvider>
        );

        fireEvent.change(screen.getByPlaceholderText("http://localhost:8080"), {
            target: { value: "http://localhost:8080" },
        });
        fireEvent.change(screen.getByPlaceholderText("Enter username"), {
            target: { value: "admin" },
        });
        fireEvent.change(screen.getByPlaceholderText("Enter password"), {
            target: { value: "admin" },
        });

        await fireEvent.click(screen.getByRole("button", { name: /connect/i }));

        await waitFor(() => {
            expect(screen.getByText(/connected/i)).toBeInTheDocument();
            const token = Cookies.get("jwtToken");
            expect(token).toBeDefined();
        });
    });

    it("should add and remove query rows", async () => {
        setup();

        const addRowBtn = screen.getByRole("button", {
            name: /add new query row/i,
        });
        await fireEvent.click(addRowBtn);

        const queryEditors = screen.getAllByPlaceholderText(
            /select \* from read_arrow/i
        );
        expect(queryEditors.length).toBeGreaterThan(1);

        const removeButtons = screen.getAllByRole("button", { name: "" });
        fireEvent.click(removeButtons[1]); // remove second row
    });

    it("should show an error if trying to run query while disconnected", async () => {
        setup();

        const runBtn = screen.getByRole("button", { name: /run query/i });
        fireEvent.click(runBtn);

        await waitFor(() => {
            expect(
                screen.getByText(/connect first to run queries/i)
            ).toBeInTheDocument();
        });
    });

    it("should execute a real query after connecting", async () => {
        setup();

        fireEvent.change(screen.getByPlaceholderText(/http:\/\/localhost:8080/i), {
            target: { value: SERVER_URL },
        });
        fireEvent.change(screen.getByPlaceholderText(/enter username/i), {
            target: { value: USERNAME },
        });
        fireEvent.change(screen.getByPlaceholderText(/enter password/i), {
            target: { value: PASSWORD },
        });

        fireEvent.click(screen.getByRole("button", { name: /connect/i }));

        await screen.findByText(/connected/i);

        fireEvent.change(
            screen.getByPlaceholderText(/select \* from read_arrow/i),
            { target: { value: "select 1+1 as sum" } }
        );

        await waitFor(() => {
          expect(screen.getByRole("button", { name: /Run Query/i })).not.toBeDisabled();
        });

        fireEvent.click(screen.getByRole("button", { name: /Run Query/i }));
        // Wait for results to render
        await screen.findByText(/sum/i);
        await screen.findByText("2");
    });

});
