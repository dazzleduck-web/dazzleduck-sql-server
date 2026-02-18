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

# Build the application
RUN npm run build

# Production stage
FROM node:20-alpine AS production
WORKDIR /app

# Copy only necessary files from builder
COPY --from=builder /app/node_modules /app/node_modules
COPY --from=builder /app/dist /app/dist

# Expose port 5173
EXPOSE 5173

# Set version label
LABEL version=0.0.20

# Use production command to serve built files
CMD ["npx", "serve", "-l", "5173", "dist/"]
