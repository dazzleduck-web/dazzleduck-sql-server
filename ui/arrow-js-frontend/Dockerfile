# Multi-stage build for production
# Version is hardcoded to match package.json: 0.0.20

FROM node:20-alpine AS builder
WORKDIR /app

# Copy package files
COPY package*.json package-lock.json ./

# Install dependencies
RUN npm ci

# Copy source code
COPY . .

# Build the web application (using app config, not library config)
# Note: vite.config.js builds the library, vite.config.app.js builds the web app
# Use npm run build:app to build the web application
RUN npm run build:app

# Production stage
FROM node:20-alpine AS production
WORKDIR /app

# Copy only necessary files from builder
COPY --from=builder /app/dist-app /app/dist

# Install serve package to serve static files
RUN npm install -g serve

# Expose port 5174 (matching your docker run command)
EXPOSE 5174

# Set version label
LABEL version=0.0.20

# Use production command to serve built files
CMD ["serve", "-l", "5174", "dist/"]
