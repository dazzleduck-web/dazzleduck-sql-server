import "@testing-library/jest-dom";
import { vi } from "vitest";

// Set up global test environment
global.fetch = fetch; // Use native fetch in Node.js

// Increase test timeout for integration tests (30 seconds)
vi.setConfig({ testTimeout: 30000, hookTimeout: 30000 });
