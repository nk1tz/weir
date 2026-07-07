# weir — bitcoin address -> webhook daemon
# Multi-stage: build TypeScript with full deps, ship only dist + pruned prod deps.

# ---- build stage -------------------------------------------------------------
FROM node:22-alpine AS build

# zeromq's native addon may compile from source on alpine/musl
RUN apk add --no-cache python3 make g++

# pnpm via corepack (lockfileVersion 9 -> pnpm 9)
ENV COREPACK_ENABLE_DOWNLOAD_PROMPT=0
RUN corepack enable && corepack prepare pnpm@9.15.9 --activate

WORKDIR /app

COPY package.json pnpm-lock.yaml ./
RUN pnpm install --frozen-lockfile

COPY tsconfig.json ./
COPY src ./src
RUN pnpm build

# drop dev deps in place; runtime stage copies the pruned node_modules
RUN pnpm prune --prod

# ---- runtime stage -----------------------------------------------------------
FROM node:22-alpine

ENV NODE_ENV=production
WORKDIR /app

COPY --from=build /app/package.json ./package.json
COPY --from=build /app/node_modules ./node_modules
COPY --from=build /app/dist ./dist

# non-root: the node image ships a 'node' user (uid 1000)
USER node

# no listening ports unless ADMIN_TOKEN is set, so no EXPOSE by default
CMD ["node", "dist/index.js"]
