import { describe, it, expect, beforeAll } from 'vitest';
import { renderHook } from '@testing-library/react';
import { LoggingProvider, useLogging } from '../src/context/LoggingContext';
import Cookies from 'js-cookie';

const SERVER_URL = 'http://localhost:8080';
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
        const hook = renderUseLogging();
        result = hook.result;
        jwtToken = await result.current.login(SERVER_URL, USERNAME, PASSWORD);
        console.log('JWT Token: ', jwtToken);
        Cookies.set('jwtToken', jwtToken);
    });

    // Basic test to ensure login works by checking if a token is string
    it('should login successfully and return a token', () => {
        expect(jwtToken).toBeDefined();
        expect(typeof jwtToken).toBe('string');
    });

    // /query tests ---------------------- START
    // test /query with splitSize 0.
    it('should runQuery directly (not going to /plan as split is 0)', async () => {
        const directRes = await result.current.executeQuery(
            `${SERVER_URL}/query`,
            USERNAME,
            PASSWORD,
            'select 2+2 as sum',
            0
        );

        expect(Array.isArray(directRes)).toBe(true);
        if (directRes.length > 0) {
            expect(directRes[0]).toHaveProperty('sum');
            expect(directRes[0].sum).toBe(4);
        }
    });

    // test to ensure executeQuery works with select 1
    it('should execute a /query', async () => {
        const rows = await result.current.executeQuery(
            `${SERVER_URL}/query`,
            USERNAME,
            PASSWORD,
            'select 1',
            0
        );

        expect(Array.isArray(rows)).toBe(true);
        if (rows.length > 0) {
            expect(rows[0]).toHaveProperty('1');
            expect(rows[0]['1']).toBe(1);
        }
    });

    // test executeQuery with /query directly
    it('should execute the simple sql query with /query', async () => {

        const rows = await result.current.executeQuery(
            `${SERVER_URL}/query`,
            USERNAME,
            PASSWORD,
            'select 21 as age',
            0
        );

        expect(Array.isArray(rows)).toBe(true);
        if (rows.length > 0) {
            expect(rows[0]).toHaveProperty('age');
            expect(rows[0].age).toBe(21);
        }
    });
    // /query tests ---------------------- END

    // /plan and split queries tests ---------------------- START
    // test to ensure executeQuery works with /plan -> /query split logic
    it('should execute /plan -> /query split logic correctly', async () => {
        const splitQuery = `FROM (FROM (VALUES(NULL::DATE, NULL::VARCHAR, NULL::VARCHAR, NULL::VARCHAR)) t(dt, p, key, value)
        WHERE false
        UNION ALL BY NAME
        FROM read_parquet('example/hive_table/*/*/*.parquet', hive_partitioning = true, hive_types = {'dt': DATE, 'p': VARCHAR}))`;

        const resultRows = await result.current.executeQuery(
            SERVER_URL,
            USERNAME,
            PASSWORD,
            splitQuery,
            1
        );

        expect(Array.isArray(resultRows)).toBe(true);
        if (resultRows.length > 0) {
            const row = resultRows[0];
            expect(row).toHaveProperty('dt');
            expect(row).toHaveProperty('p');
            expect(row).toHaveProperty('key');
            expect(row).toHaveProperty('value');
        }
    });
    // /plan and split queries tests ---------------------- END


    // test to ensure executeQuery handles invalid input gracefully
    it('should handle invalid input gracefully', async () => {
        await expect(
            result.current.executeQuery(SERVER_URL, USERNAME, PASSWORD, '', 0)
        ).rejects.toThrow(/Please fill in all fields/);
    });

    // test to ensure executeQuery handles bad splits query
    it('should handle /plan returning no splits gracefully', async () => {
        const badSplitQuery = 'select * from wrong_table';
        try {
            await result.current.executeQuery(SERVER_URL, USERNAME, PASSWORD, badSplitQuery, 1);
        } catch (err) {
            expect(err.message).toMatch(/Query execution failed/);
        }
    });

    // test query execution with invlalid token
    // this test will fail until we start server with auth enabled, so we skipping it for now.
    it.skip('should fail if token is invalid', async () => {
        Cookies.set('jwtToken', 'invalidtoken');
        await expect(
            result.current.executeQuery(`${SERVER_URL}/query`, USERNAME, PASSWORD, 'select 1', 0)
        ).rejects.toThrow(/Fail|responded|Unauthorized/i);
    });

    // test to ensure forwardToDazzleDuck works correctly
    it('should forward to a valid endpoint using forwardToDazzleDuck', async () => {
        const res = await result.current.executeQuery(
            SERVER_URL,
            USERNAME,
            PASSWORD,
            'select current_date',
            0
        );
        expect(Array.isArray(res)).toBe(true);
    });

})