FROM node:20-alpine

WORKDIR /app

COPY package*.json ./
RUN npm ci --omit=dev || npm install

COPY tsconfig.json ./
COPY src/ ./src/

RUN npm run build

# Default: run the worker. Override in docker-compose with `command:`
CMD ["node", "dist/worker.js"]
