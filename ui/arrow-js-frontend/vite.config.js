import { defineConfig } from "vite";
import react from '@vitejs/plugin-react';
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
  plugins: [react()],

  build: {
    lib: {
      entry: path.resolve(__dirname, 'src/lib/index.js'),
      name: 'ArrowUI',
      formats: ['es', 'cjs'],
      fileName: (format) => `arrow-ui.${format}.js`,
    },
    rollupOptions: {
      external: ['react', 'react-dom', 'react-router-dom', 'react-icons', 'react-hook-form', 'js-cookie', 'uuidv4', 'd3'],
      output: {
        globals: {
          react: 'React',
          'react-dom': 'ReactDOM',
          'react-router-dom': 'ReactRouterDOM',
          'react-icons': 'ReactIcons',
          'react-hook-form': 'ReactHookForm',
          'js-cookie': 'Cookies',
          'uuidv4': 'uuidv4',
          'd3': 'd3',
        },
      },
    },
    sourcemap: true,
    emptyOutDir: true,
  },
});
