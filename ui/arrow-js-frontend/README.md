# DazzleDuck SQL HTTP + Arrow JS Frontend

An integrated project combining **DazzleDuck SQL HTTP Server** and a modern **Arrow JS Frontend** for interactive SQL query execution and visualization in your browser.

---

## Project Structure

```
dazzleduck/
├── dazzleduck-sql-http/         # DazzleDuck HTTP Server
├── ...
├── ...
└── ui/
    └── arrow-js-frontend/       # Frontend: React + Vite app
```

# Setup Guide

##  1.DazzleDuck SQL HTTP Server

1. Navigate to the http server folder:
   ```bash
   cd dazzleduck-sql-http
   ```

2. Build the project:
   ```bash
   mvn clean install
   ```

3. Start the http server.


4. Verify that it listens on:
   ```
   http://localhost:8080
   ```

> The backend provides SQL execution APIs and has CORS enabled for the frontend.

---

## 2. Frontend (Arrow JS UI)

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

## Integration Flow

1. **Frontend** sends SQL queries to:
   ```
   POST http://localhost:8080
   ```
2. **DD HTTP** executes the query and sends back **Response**.

3. **Frontend** renders results using Arrow JS components.

---

## Tech Stack

**DazzleDuck http:** Java 17 • Helidon 4.x • Apache Arrow • DazzleDuck SQL  
**Frontend:** React 18 • Vite • Tailwind CSS • Arrow JS Client
