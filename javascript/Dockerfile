FROM node:22-bookworm-slim@sha256:6c74791e557ce11fc957704f6d4fe134a7bc8d6f5ca4403205b2966bd488f6b3

WORKDIR /app/javascript
COPY javascript/package.json javascript/package-lock.json ./
RUN npm ci --include=dev --ignore-scripts

COPY proto /app/proto
COPY javascript/sdk ./sdk
COPY examples /app/examples
COPY javascript/tests ./tests
COPY javascript/tsconfig.json ./tsconfig.json
COPY javascript/consumer.js javascript/alarm_rpc.js javascript/receive_data.js javascript/evidence_receiver.js ./
RUN npm run check \
    && npm prune --omit=dev

ENV NODE_ENV=production

CMD ["node", "consumer.js"]
