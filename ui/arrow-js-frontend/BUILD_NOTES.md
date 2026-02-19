# DazzleDuck Frontend Build Configuration

## Overview

This project serves **dual purposes** and requires different build configurations for each:

1. **Component Library** (for npm package distribution)
2. **Web Application** (for Docker deployment)

## Problem

The original Docker build was using the library configuration which only produces:
- `arrow-ui.es.js` - ES module format library
- `arrow-ui.cjs.js` - CommonJS format library
- Sourcemap files

This resulted in a directory listing of library files instead of the web application.

## Solution

Created separate Vite configurations for each purpose:

### Configuration Files

- **`vite.config.js`** - Library build configuration (original)
  - Uses `build.lib` mode
  - Externalizes React dependencies
  - Outputs: `arrow-ui.es.js`, `arrow-ui.cjs.js`
  - Command: `npm run build` or `npm run build:lib`

- **`vite.config.app.js`** - Web application build configuration (new)
  - Standard Vite app build
  - Bundles all dependencies
  - Outputs: `index.html`, bundled JS/CSS assets
  - Command: `npm run build:app`

### Updated Scripts

```json
{
  "scripts": {
    "dev": "vite",
    "build": "vite build",              // Default: library build
    "build:app": "vite build --config vite.config.app.js",  // Web app build
    "build:lib": "vite build",         // Explicit library build
    "preview": "vite preview",
    "lint": "eslint .",
    "test": "vitest run --coverage",
    "test:watch": "vitest"
  }
}
```

### Docker Build

The Dockerfile now uses the app configuration:

```dockerfile
# Build the web application (using app config, not library config)
RUN npm run build:app
```

This produces a proper web application with:
- `index.html` - Application entry point
- `dist/assets/index-*.js` - Bundled JavaScript
- `dist/assets/index-*.css` - Compiled styles
- Asset files (favicon, images, etc.)

## Usage

### Local Development
```bash
npm run dev  # Runs dev server on port 5174
```

### Build Library (for npm package)
```bash
npm run build:lib  # or npm run build
```
Output: `dist/arrow-ui.es.js`, `dist/arrow-ui.cjs.js`

### Build Web Application (for Docker)
```bash
npm run build:app
```
Output: `dist/index.html`, `dist/assets/`

### Build Docker Image
```bash
docker build -t dazzleduck-frontend:0.0.20 .
```

### Run Docker Container
```bash
docker run -p 5173:5173 dazzleduck-frontend:0.0.20
```
Access at: `http://localhost:5173`

## Technical Details

### Library Mode (vite.config.js)
- Entry: `src/lib/index.js`
- External deps: React, ReactDOM, TailwindCSS
- Formats: ES modules, CommonJS
- Purpose: Reusable component library for other projects

### App Mode (vite.config.app.js)
- Entry: `index.html` (auto-detected by Vite)
- Bundled deps: All dependencies included
- Format: Optimized web application bundle
- Purpose: Standalone web application for Docker deployment

## Troubleshooting

### If Docker serves directory listing instead of app:
1. Ensure `vite.config.app.js` exists
2. Check that Dockerfile uses `npm run build:app`
3. Verify `dist/index.html` exists in the container
4. Check build logs for "index.html" output

### If library build is broken:
1. Use `npm run build:lib` explicitly
2. Check `vite.config.js` has proper `build.lib` configuration
3. Verify `src/lib/index.js` exports are correct
