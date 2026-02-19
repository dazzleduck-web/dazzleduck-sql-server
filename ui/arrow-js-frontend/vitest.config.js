import { defineConfig } from "vite";
import react from '@vitejs/plugin-react';

/**
 * Vitest configuration for running tests
 *
 * This file is separate from build configs to keep concerns separated:
 * - vite.config.js      - Library build for npm package
 * - vite.config.app.js   - App build for Docker deployment
 * - vitest.config.js     - Test configuration (this file)
 *
 * Usage:
 * - npm run test          - Run all tests
 * - npm run test:watch    - Run tests in watch mode
 */
export default defineConfig({
  plugins: [react()],

  test: {
    environment: 'jsdom',
    globals: true,
    setupFiles: ["./tests/setup.js"],
    coverage: {
      provider: 'v8',
      reporter: ['text', 'json', 'html'],
      exclude: [
        'node_modules/',
        'tests/',
        '**/*.test.jsx',
        '**/*.test.js',
        '**/*.spec.jsx',
        '**/*.spec.js',
        'dist/',
        'dist-app/',
      ]
    }
  },
});
