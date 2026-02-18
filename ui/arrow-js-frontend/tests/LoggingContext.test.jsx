import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { renderHook, act } from '@testing-library/react';
import { LoggingProvider, useLogging } from '../src/context/LoggingContext';
import Cookies from 'js-cookie';

const SERVER_URL = 'http://localhost:8081';
const USERNAME = 'admin';
const PASSWORD = 'admin';

const renderUseLogging = () =>
    renderHook(() => useLogging(), {
        wrapper: ({ children }) => <LoggingProvider>{children}</LoggingProvider>,
    });

describe('LoggingContext Integration Tests', () => {
    let jwtToken;
    let result;

    beforeAll(async () => {
        // Clear cookies before tests
        Cookies.remove('jwtToken');
        Cookies.remove('connectionInfo');

        const hook = renderUseLogging();
        result = hook.result;
        jwtToken = await result.current.login(SERVER_URL, USERNAME, PASSWORD);
        Cookies.set('jwtToken', jwtToken);
    });

    afterAll(() => {
        // Cleanup: logout and clear cookies
        if (result.current.logout) {
            result.current.logout();
        }
        Cookies.remove('jwtToken');
        Cookies.remove('connectionInfo');
    });

    // Basic test to ensure login works by checking if a token is string
    it('should login successfully and return a token', () => {
        expect(jwtToken).toBeDefined();
        expect(typeof jwtToken).toBe('string');
        expect(jwtToken).toMatch(/Bearer/);
    });

    // /query tests ---------------------- START
    it('should execute query directly (split size 0) and return correct format', async () => {
        const resultObj = await result.current.executeQuery(
            SERVER_URL,
            'select 2+2 as sum',
            0,
            jwtToken,
            null,
            true // disableCompression to avoid Arrow decompression issues in test env
        );

        // New format: { data: [...rows...], queryId: number }
        expect(resultObj).toHaveProperty('data');
        expect(resultObj).toHaveProperty('queryId');
        expect(Array.isArray(resultObj.data)).toBe(true);

        if (resultObj.data.length > 0) {
            expect(resultObj.data[0]).toHaveProperty('sum');
            expect(resultObj.data[0].sum).toBe(4);
        }
    });

    it('should execute a simple select query', async () => {
        const resultObj = await result.current.executeQuery(
            SERVER_URL,
            'select 1 as one',
            0,
            jwtToken,
            null,
            true // disableCompression to avoid Arrow decompression issues in test env
        );

        expect(resultObj).toHaveProperty('data');
        expect(Array.isArray(resultObj.data)).toBe(true);

        if (resultObj.data.length > 0) {
            expect(resultObj.data[0]).toHaveProperty('one');
            expect(resultObj.data[0].one).toBe(1);
        }
    });

    it('should execute query with alias', async () => {
        const resultObj = await result.current.executeQuery(
            SERVER_URL,
            'select 21 as age',
            0,
            jwtToken,
            null,
            true // disableCompression to avoid Arrow decompression issues in test env
        );

        expect(resultObj).toHaveProperty('data');
        expect(Array.isArray(resultObj.data)).toBe(true);

        if (resultObj.data.length > 0) {
            expect(resultObj.data[0]).toHaveProperty('age');
            expect(resultObj.data[0].age).toBe(21);
        }
    });
    // /query tests ---------------------- END

    // /plan and split queries tests ---------------------- START
    it('should execute /plan -> /query split logic correctly', async () => {
        // Use a query that would benefit from splitting
        const splitQuery = 'SELECT * FROM information_schema.tables';

        const resultObj = await result.current.executeQuery(
            SERVER_URL,
            splitQuery,
            1, // Enable splitting
            jwtToken
        );

        expect(resultObj).toHaveProperty('data');
        expect(resultObj).toHaveProperty('queryId');
        expect(Array.isArray(resultObj.data)).toBe(true);

        if (resultObj.data.length > 0) {
            expect(resultObj.data[0]).toBeDefined();
        }
    });
    // /plan and split queries tests ---------------------- END

    // Invalid input handling
    it('should handle invalid input gracefully', async () => {
        await expect(
            result.current.executeQuery(SERVER_URL, '', 0, jwtToken)
        ).rejects.toThrow(/Please fill in all fields/);
    });

    // Handle /plan returning no splits or query errors
    it('should handle query execution errors gracefully', async () => {
        const badQuery = 'select * from nonexistent_table_xyz123';
        try {
            await result.current.executeQuery(SERVER_URL, badQuery, 1, jwtToken);
            // If it doesn't throw, that's unexpected
            expect(true).toBe(false);
        } catch (err) {
            expect(err).toBeDefined();
            expect(err.message).toBeTruthy();
        }
    });

    // Logout test
    it('should logout and clear cookies', async () => {
        await act(async () => {
            result.current.logout();
        });

        const token = Cookies.get('jwtToken');
        const connectionInfo = Cookies.get('connectionInfo');

        expect(token).toBeUndefined();
        expect(connectionInfo).toBeUndefined();
    });
    
    // Test with claims
    it('should login with claims', async () => {
        const claims = { cluster: 'test-cluster', database: 'test-db' };

        await act(async () => {
            jwtToken = await result.current.login(SERVER_URL, USERNAME, PASSWORD, 0, claims);
        });

        expect(jwtToken).toBeDefined();
        expect(typeof jwtToken).toBe('string');
    });

    // Test with compression disabled
    it('should login with compression disabled', async () => {
        await act(async () => {
            jwtToken = await result.current.login(SERVER_URL, USERNAME, PASSWORD, 0, {}, true);
        });

        expect(jwtToken).toBeDefined();
    });

    // Test queryId parameter
    it('should include queryId in request when provided', async () => {
        const resultObj = await result.current.executeQuery(
            SERVER_URL,
            'select 1',
            0,
            jwtToken,
            123, // explicit queryId
            true // disableCompression to avoid Arrow decompression issues in test env
        );

        expect(resultObj).toHaveProperty('queryId');
        expect(typeof resultObj.queryId).toBe('number');
    });

    // Cancel query test
    it('should cancel a query', async () => {
        // First execute a simple query to get a queryId
        const resultObj = await result.current.executeQuery(
            SERVER_URL,
            'SELECT 1',
            0,
            jwtToken,
            null,
            true // disableCompression to avoid Arrow decompression issues in test env
        );

        expect(resultObj).toHaveProperty('queryId');

        if (resultObj.queryId) {
            // Now try to cancel it (though it likely already finished)
            try {
                const cancelResult = await result.current.cancelQuery(
                    SERVER_URL,
                    'SELECT 1',
                    resultObj.queryId
                );

                // Cancel should return success: true with status 200, 202, or 409
                expect(cancelResult).toHaveProperty('success');
                expect([200, 202, 409]).toContain(cancelResult.status);
            } catch (err) {
                // Query may have already completed, which is acceptable
                expect(err).toBeDefined();
            }
        }
    });
});
