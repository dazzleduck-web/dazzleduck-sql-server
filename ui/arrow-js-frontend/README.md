# DazzleDuck SQL HTTP + Arrow JS Frontend

An integrated project combining **DazzleDuck SQL HTTP Server** and a modern **Arrow JS Frontend** for interactive SQL query execution and visualization in your browser.

---

## 🧩 Setup Guide

### 1. DazzleDuck SQL HTTP Server

1. Navigate to the HTTP server folder:
   ```bash
   cd dazzleduck-sql-http
   ```

2. Build the project:
   ```bash
   mvn clean install
   ```

3. Start the HTTP server:
   ```bash
   java -jar target/dazzleduck-sql-http.jar
   ```

4. Verify that it listens on:
   ```
   http://localhost:8080
   ```

> The backend provides SQL execution APIs and has CORS enabled for the frontend.

---

### 2. Frontend (Arrow JS UI)

1. Navigate to the frontend folder:
   ```bash
   cd ui/arrow-js-frontend
   ```

2. Install dependencies:
   ```bash
   npm install
   ```

3. Run the development server:
   ```bash
   npm run dev
   ```

4. Access the app:
   ```
   http://localhost:5173
   ```
---

# Docker Support

## 🛠️ Build the Docker Image
From the project root (where the `Dockerfile` is located), run:

```bash
docker build -t dazzleduck-frontend .
```

## 🛠️ Start the container
```bash
docker run -p 5174:5174 dazzleduck-frontend
```
---

## 🔄 Integration Flow

1. **Frontend** sends SQL queries to:
   ```
   POST http://localhost:8080
   ```
2. **DazzleDuck HTTP Server** executes the query and sends back the **Response**.

3. **Frontend** renders results using Arrow JS components.

---

## 🧰 Tech Stack

**DazzleDuck HTTP:** Java 21 • Helidon 4.x • Apache Arrow • DazzleDuck SQL  
**Frontend:** React 18 • Vite • Tailwind CSS • Arrow JS Client

---

## 🧪 Frontend Testing with Vitest

The Arrow JS frontend uses [Vitest](https://vitest.dev/) for unit and integration testing.

### Run All Tests
```bash
npm run test
```
or
```bash
npm test
```

### Run a Specific Test File
```bash
npm test Logging.test.jsx
```
> Replace `Logging.test.jsx` with your specific test file name.

---

# 🚀 Publishing to NPM (Arrow UI Library)

The **Arrow UI** components (e.g., `DisplayCharts`, `EntityTable`, `Navbar`, etc.) are reusable and published as a standalone NPM package:  
**[`dazzleduck-arrow-ui`](https://www.npmjs.com/package/dazzleduck-arrow-ui)**

---

## 🧭 First-Time Setup (Only Once)

1️⃣ **Create an npm account**  
👉 [https://www.npmjs.com/signup](https://www.npmjs.com/signup)

2️⃣ **Login from your terminal**
```bash
npm login
```

3️⃣ **Check your account**
```bash
npm whoami
```

4️⃣ **Remove private flag**
Make sure your `package.json` has:
```json
"private": false
```

5️⃣ **Ensure proper fields in package.json**
```json
{
  "name": "dazzleduck-arrow-ui", 
  "version": "1.0.0",
  "description": "Reusable UI components for Arrow frontend",
  "main": "dist/arrow-ui.cjs.js",
  "module": "dist/arrow-ui.es.js",
  "types": "dist/index.d.ts",
  "files": ["dist"],
  "peerDependencies": {
    "react": "^18.0.0",
    "react-dom": "^18.0.0"
  }
}
```

6️⃣ **Build the library**
```bash
npm run build
```

7️⃣ **Publish for the first time**
```bash
npm publish --access public
```

You should see:
```
+ dazzleduck-arrow-ui@1.0.0
```
✅ Your package is now live on npm.

---

## 🔁 Updating After Changes

When you modify components (e.g., `DisplayCharts`):

1️⃣ **Make your changes**

2️⃣ **Rebuild**
```bash
npm run build
```

3️⃣ **Bump the version**
```bash
npm version patch
```
Example: `1.0.0 → 1.0.1`

4️⃣ **Publish again**
```bash
npm publish --access public
```

5️⃣ **Update in other projects**
```bash
npm install dazzleduck-arrow-ui@latest --legacy-peer-deps
```

✅ Your updated version is now available to everyone.

---

## ⚙️ Quick One-Step Command

Add this script to your `package.json`:

```json
"scripts": {
  "release": "npm version patch && npm run build && npm publish --access public"
}
```

Then publish new updates easily:
```bash
npm run release
```

---

## 🧑‍🤝‍🧑 Collaborator Access (Team Publishing)

By default, only the **package owner** can publish new versions.  
To let teammates also publish new updates:

### ➕ Add a collaborator
```bash
npm access grant read-write <username> package:dazzleduck-arrow-ui
```

Example:
```bash
npm access grant read-write alice package:dazzleduck-arrow-ui
```

### 👀 List collaborators
```bash
npm access ls-collaborators dazzleduck-arrow-ui
```

### ❌ Remove collaborator
```bash
npm access revoke <username> package:dazzleduck-arrow-ui
```

Now that user can log in with their npm account and publish new versions using:
```bash
npm version patch
npm publish --access public
```

> 🧠 Tip: Each collaborator’s npm account must be logged in via `npm login` before publishing.

---

## 🧩 Local Development (Without Publishing)

You can test your package locally without publishing it each time.

In your **library project**:
```bash
npm link
```

In your **main project**:
```bash
npm link dazzleduck-arrow-ui
```

Rebuild to reflect changes:
```bash
npm run build
```

When done:
```bash
npm unlink dazzleduck-arrow-ui
npm install dazzleduck-arrow-ui@latest
```

---

✅ **Summary Commands**

| Action | Command |
|--------|----------|
| Login | `npm login` |
| Build | `npm run build` |
| First publish | `npm publish --access public` |
| Update version | `npm version patch` |
| Republish | `npm publish --access public` |
| Add collaborator | `npm access grant read-write <username> package:dazzleduck-arrow-ui` |
| List collaborators | `npm access ls-collaborators dazzleduck-arrow-ui` |
| Local link | `npm link` / `npm link dazzleduck-arrow-ui` |

---