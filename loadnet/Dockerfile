FROM node:20-bookworm-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends iproute2 ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY package.json package-lock.json ./
COPY packages ./packages
COPY scripts ./scripts
COPY loadnet ./loadnet

RUN npm ci

ENV NODE_ENV=production
ENTRYPOINT ["npx", "tsx", "loadnet/node.ts"]
