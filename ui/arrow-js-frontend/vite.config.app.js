import { defineConfig } from "vitest/config";
import react from '@vitejs/plugin-react';
import tailwindcss from '@tailwindcss/vite';

/**
 * Vite config for building the web application (not the library)
 *
 * This project serves dual purposes:
 * 1. Library build (vite.config.js) - Produces arrow-ui.es.js and arrow-ui.cjs.js for npm package distribution
 * 2. App build (vite.config.app.js) - Produces the full React web application for Docker deployment
 *
 * Key differences:
 * - Library mode: Uses build.lib with external dependencies for reusable components
 * - App mode: Standard Vite build with bundled dependencies for standalone web app
 *
 * Usage:
 * - npm run build:lib  - Build the library package (default npm run build)
 * - npm run build:app  - Build the web application for Docker
 * - npm run dev        - Run dev server for local development
 */
export default defineConfig({
  plugins: [react(), tailwindcss()],

  build: {
    outDir: 'dist',
    sourcemap: true,
    emptyOutDir: true,
  },
});
