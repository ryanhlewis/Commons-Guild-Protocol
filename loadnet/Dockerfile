FROM node:22-bookworm-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends iproute2 ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY package.json package-lock.json ./
COPY packages ./packages

RUN npm ci \
    && npm run build --workspace=@cgp/core \
    && npm run build --workspace=@cgp/client

COPY scripts ./scripts
COPY loadnet ./loadnet

ENV NODE_ENV=production
ENTRYPOINT ["node", "--import", "tsx", "loadnet/node.ts"]
