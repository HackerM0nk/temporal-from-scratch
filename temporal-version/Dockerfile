FROM node:20-alpine

WORKDIR /app

# Install dependencies first (layer cache)
COPY package*.json ./
RUN npm ci --omit=dev || npm install

COPY tsconfig.json ./
COPY src/ ./src/

# Build TypeScript
RUN npm run build

# Default command: run the worker
CMD ["node", "dist/worker.js"]
