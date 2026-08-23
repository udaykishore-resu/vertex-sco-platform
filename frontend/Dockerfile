FROM node:22-alpine AS build
WORKDIR /app
COPY package*.json ./
RUN npm ci
COPY . .
# VITE_* env vars are baked into the static bundle at build time, not read
# at container-run time — so they must arrive as build ARGs (wired from
# docker-compose.yml's `build.args`), not as a runtime `environment:` entry
# on the final nginx stage below, which would be silently ignored.
ARG VITE_CONTROL_PLANE_URL
ENV VITE_CONTROL_PLANE_URL=${VITE_CONTROL_PLANE_URL}
RUN npm run build

FROM nginx:1.27-alpine
COPY --from=build /app/dist /usr/share/nginx/html
EXPOSE 80
