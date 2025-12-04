import { defineConfig } from "vitest/config";
import react from '@vitejs/plugin-react';
import tailwindcss from '@tailwindcss/vite';
import path from 'path';

// https://vite.dev/config/
export default defineConfig({
  plugins: [react(), tailwindcss(),],
  server: {
    host: '0.0.0.0',
    port: 5174,
    strictPort: true
  },
  // Configuration for building your library for NPM
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

  // Your existing Vitest configuration
  test: {
    environment: 'jsdom',
    globals: true,
    setupFiles: ["./tests/setup.js"],
  },
});
