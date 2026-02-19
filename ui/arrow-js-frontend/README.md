# DazzleDuck Arrow JS Frontend

A modern React-based web UI for the DazzleDuck SQL Server, providing interactive SQL query execution, data visualization, and session management.

---

## Overview

The Arrow JS Frontend is a feature-rich web application that connects to the DazzleDuck SQL HTTP Server, enabling users to:

- Execute SQL queries with real-time results
- Visualize data using interactive charts (Line, Bar, Pie)
- Manage multiple query rows with independent results
- Save and restore session configurations
- Search and browse query results
- Configure advanced connection parameters including JWT claims

---

## Technology Stack

- **Framework:** React 18/19 with React Router
- **Build Tool:** Vite
- **Styling:** Tailwind CSS 4.x
- **State Management:** React Context API
- **Form Handling:** React Hook Form
- **Data Processing:** Apache Arrow 21.x (with ZSTD compression via fzstd)
- **HTTP Client:** Axios
- **Authentication:** JWT with Cookie persistence (js-cookie)
- **Charts:** D3.js for custom chart visualizations
- **Testing:** Vitest with React Testing Library
- **Icons:** React Icons (Heroicons, React Icons)
- **UUID Generation:** uuidv4

---

## Features

### Connection Management

**ConnectionPanel Component**
- Server URL, username, and password fields
- Real-time connection status indicator
- JWT-based authentication with automatic token management
- Cookie-based session persistence across page refreshes
- Advanced settings toggle for additional parameters

**Advanced Settings**
- Split size configuration for query optimization
- JWT claims management with predefined keys:
  - `cluster`
  - `orgId`
  - `database`
  - `schema`
  - `table`
  - `path`
  - `function`
- Claim validation with suggestions
- Add, remove, and update claims dynamically

### Query Management

**Multiple Query Rows**
- Add multiple query rows for parallel execution
- Each row has independent results and state
- Run queries individually or all at once
- Query ID tracking for cancellation support

**Query Execution**
- Support for both direct queries and split-based queries
- Automatic response type detection (JSON vs Arrow binary)
- ZSTD decompression support for compressed Arrow data
- Query timeout handling (5 minutes)
- Error message extraction from server responses

**Query Cancellation**
- Cancel running queries via query ID
- Status code handling (200, 202, 409)
- Real-time cancellation status feedback

### Data Visualization

**DisplayCharts Component**
Supports three visualization modes:

1. **Table View**
   - Sortable table with sticky headers
   - Date normalization for timestamp fields
   - Hover effects for row highlighting

2. **Line Chart (D3)**
   - Auto-detection of X and Y axes from first 3 columns
   - Time series support with date formatting
   - Grid lines and legends

3. **Bar Chart (D3)**
   - Multi-series bar charts
   - Automatic column type detection
   - Customizable dimensions

4. **Pie Chart (D3)**
   - Uses first two columns (label + numeric value)
   - Timestamp-aware numeric column detection
   - Responsive sizing

### Session Management

**Save Session**
- Exports current state to JSON file
- Includes connection settings (excluding password)
- Saves all query configurations
- Uses File System Access API when available

**Restore Session**
- Load previously saved sessions
- Restores connection URL, username, and claims
- Restores all query configurations
- Requires password re-entry for security

### Search Functionality

**SearchTable Component**
- Full-text search across query results
- Time range filtering (24 hours, 7 days, 30 days)
- Field sidebar for column selection
- Pagination with customizable page size
- Row expansion for detailed JSON view

---

## Installation and build

```bash
# Navigate to frontend directory
cd ui/arrow-js-frontend

# Install dependencies
npm install

# Start development server
npm run dev

# Build for production
npm run build:app
```

The development server runs on `http://localhost:5174`.

## Build for Deployment

```bash
# Build for production deployment
npm run build:app
```

This creates a `dist-app/` folder containing the production-ready static files. Upload this folder to any static hosting service (Netlify, Vercel, GitHub Pages, etc.) or deploy via the included Dockerfile.

---

## Backend API Integration

The frontend integrates with the DazzleDuck SQL HTTP Server via the following endpoints:

| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/v1/login` | POST | Authenticate and receive JWT token |
| `/v1/query` | POST | Execute SQL query |
| `/v1/plan` | POST | Get query execution plan with splits |
| `/v1/cancel` | POST | Cancel a running query |

### Request Headers

```javascript
{
  "Content-Type": "application/json",
  "Accept": "application/json, application/vnd.apache.arrow.stream",
  "Authorization": "Bearer <jwt-token>"
}
```

### Response Types

1. **JSON Response**
   - Direct JSON data from query results
   - Error messages from server

2. **Arrow Binary Response**
   - Apache Arrow IPC format
   - Optionally ZSTD compressed
   - Automatically decompressed and parsed

---

## Key Components

### LoggingContext

Global state management providing:
- `executeQuery(url, query, splitSize, jwt, queryId)` - Execute SQL
- `login(url, username, password, splitSize, claims)` - Authenticate
- `logout()` - Clear session and cookies
- `cancelQuery(url, query, queryId)` - Cancel running query
- `saveSession(queries)` - Export session state
- `loadSession(file)` - Parse session file
- `restoreSession(sessionData)` - Restore UI state
- `connectionInfo` - Current connection details

### QueryRow

Individual query interface with:
- SQL query editor with Shift+Enter to run
- Run, Cancel, and Clear buttons
- Collapsible panel for query editor
- View selector (table/line/bar/pie)
- Results display with loading/error states

### ConnectionPanel

Sidebar panel for:
- Server URL, username, password inputs
- Advanced settings toggle
- Claims management
- Session save/restore buttons
- Connection status indicator

---

## Testing

Run tests with Vitest:

```bash
# Run all tests
npm test

# Run tests in watch mode
npm run test:watch

# Run with coverage
npm run test -- --coverage
```

Test files are located in the `tests/` directory:
- `Logging.test.jsx` - Main logging component tests
- `LoggingContext.test.jsx` - Context provider tests
- `setup.js` - Test environment configuration

---

## Session File Format

```json
{
  "version": "1.0",
  "savedAt": "2025-02-19T12:00:00.000Z",
  "connection": {
    "serverUrl": "http://localhost:8081",
    "username": "admin",
    "claims": [
      { "key": "database", "value": "mydb" }
    ],
    "splitSize": 0
  },
  "queries": [
    { "query": "SELECT * FROM table" }
  ]
}
```

---

## Configuration

### Connection Settings
- **Server URL:** Base URL of DazzleDuck HTTP server
- **Username/Password:** Authentication credentials
- **Split Size:** Number of splits for query optimization (0 = no splitting)

### Advanced Settings
- **Claims:** Key-value pairs for JWT claims
  - Predefined keys: `cluster`, `orgId`, `database`, `schema`, `table`, `path`, `function`
  - Custom keys supported

### View Settings
- **Default View:** Table, Line, Bar, or Pie
- **Chart Dimensions:** Width and height (customizable per chart)

---

## Build Configuration

The project uses Vite with two build configurations:

- `vite.config.app.js` - Development server and web application build
- `vite.config.js` - Library build for NPM publishing

### Development

```bash
npm run dev
```

### App Build (Production Deployment)

```bash
npm run build:app
```

**Output:**
- Folder: `dist-app/`
- Contents: Optimized static files ready for deployment
- Deploy to: Any static hosting service (Netlify, Vercel, GitHub Pages, etc.) or use Docker

### Library Build (NPM Package)

```bash
npm run build:lib
```

**Output:**
- Folder: `dist/`
- Files:
  - `arrow-ui.cjs.js` - CommonJS bundle
  - `arrow-ui.es.js` - ES module bundle
  - `index.d.ts` - TypeScript definitions
- Use: NPM package publishing

### Preview

```bash
npm run preview
```

---

## NPM Package

The UI components are published as `dazzleduck-arrow-ui` on NPM.

### Publishing

```bash
# Build library
npm run build

# Bump version
npm version patch  # or minor, major

# Publish to NPM
npm publish --access public
```

### Using the Package

```bash
npm install dazzleduck-arrow-ui --legacy-peer-deps
```

---

## Browser Support

- Modern browsers with ES6+ support
- File System Access API for save dialog (Chrome/Edge)
- Fallback download for other browsers

---

## Security Notes

- Passwords are never saved to session files or cookies
- JWT tokens stored in cookies with:
  - `SameSite: lax`
  - Secure flag for non-localhost
- CORS enabled for development
- Server-side validation required for claims

---

## Troubleshooting

### Connection Issues
- Verify DazzleDuck HTTP server is running on port 8081
- Check CORS configuration on server
- Ensure correct server URL format (http:// or https://)

### ZSTD Decompression Errors
- Verify `fzstd` dependency is installed
- Check server compression settings
- Review console for codec registration errors

### Session Load Failures
- Validate JSON file format
- Ensure file contains `connection` and `queries` keys
- Check for file corruption

---

### Debugging

- Use React DevTools for component state inspection
- Check Network tab for API requests and responses
- Console logs show detailed error messages
- Vitest tests for unit verification

---

## Contributing

When adding new features:
1. Create corresponding hook if needed
2. Update context if global state is required
3. Add tests for new components
4. Update documentation in README
