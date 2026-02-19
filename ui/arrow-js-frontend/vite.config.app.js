import { defineConfig } from "vite";
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

  // Base path for deployment (useful for reverse proxies or subdirectories)
  base: '/',

  // Dev server configuration for local development
  server: {
    host: '0.0.0.0',
    port: 5174,
    strictPort: true,
    allowedHosts: ['dazzleduck-ui.com', 'www.dazzleduck-ui.com'],
    hmr: {
      host: 'dazzleduck-ui.com',
      port: 8000,
      protocol: 'ws'
    }
  },

  build: {
    outDir: 'dist-app',
    sourcemap: true,
    emptyOutDir: true,
    // Production build optimizations
    chunkSizeWarningLimit: 1000,
    minify: 'esbuild',
    rollupOptions: {
      output: {
        manualChunks: {
          vendor: ['react', 'react-dom'],
          router: ['react-router-dom'],
          charts: ['d3'],
          utils: ['js-cookie', 'uuidv4']
        }
      }
    }
  },
});
