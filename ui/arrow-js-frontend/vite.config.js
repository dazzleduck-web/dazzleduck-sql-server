import { defineConfig } from "vitest/config";
import react from '@vitejs/plugin-react';
import tailwindcss from '@tailwindcss/vite';
import path from 'path';

/**
 * Vite config for building the component library (not the web application)
 *
 * This project serves dual purposes:
 * 1. Library build (this config) - Produces arrow-ui.es.js and arrow-ui.cjs.js for npm package distribution
 * 2. App build (vite.config.app.js) - Produces the full React web application for Docker deployment
 *
 * Key differences:
 * - Library mode: Uses build.lib with external dependencies for reusable components
 * - App mode: Standard Vite build with bundled dependencies for standalone web app
 *
 * Usage:
 * - npm run build (or npm run build:lib) - Build the library package
 * - npm run build:app  - Build the web application for Docker
 * - npm run dev        - Run dev server for local development
 */
export default defineConfig({
  plugins: [react(), tailwindcss(),],
  server: {
    host: '0.0.0.0',
    port: 5174,
    strictPort: true,
    allowedHosts: ['dazzleduck-ui.com', 'www.dazzleduck-ui.com'],
    // WebSocket HMR connection
    hmr: {
      host: 'dazzleduck-ui.com',
      port: 8000,
      protocol: 'ws'
    }
  },
  build: {
    lib: {
      // Use process.cwd() to resolve the entry file from the project root
      entry: path.resolve(process.cwd(), 'src/lib/index.js'),
      name: 'ArrowUI', // This will be the global variable name in UMD builds
      formats: ['es', 'cjs'], // Output formats
      fileName: (format) => `arrow-ui.${format}.js`,
    },
    rollupOptions: {
      // Externalize dependencies that your library consumers should provide
      external: ['react', 'react-dom', 'tailwindcss'],
      output: {
        // Define global variables for UMD (Universal Module Definition) build
        globals: {
          react: 'React',
          'react-dom': 'ReactDOM',
          tailwindcss: 'tailwindcss',
        },
      },
    },
    sourcemap: true,  // Generate sourcemaps for easier debugging
    emptyOutDir: true, // Clean the 'dist' folder before each build
  },

  // Vitest configuration
  test: {
    environment: 'jsdom',
    globals: true,
    setupFiles: ["./tests/setup.js"],
  },
});
