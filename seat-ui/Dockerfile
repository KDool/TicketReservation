FROM node:20-alpine AS build
WORKDIR /app
ENV VITE_API_BASE=http://localhost:8080
ENV VITE_EVENT_ID=E1
ENV VITE_HOLD_SECONDS=30
COPY package*.json ./
RUN npm ci
COPY . .
RUN npm run build

FROM nginx:alpine
COPY nginx.conf /etc/nginx/conf.d/default.conf
COPY --from=build /app/dist /usr/share/nginx/html
EXPOSE 80
