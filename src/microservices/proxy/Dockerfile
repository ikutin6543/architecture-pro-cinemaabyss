FROM node:20-alpine
WORKDIR /usr/src/app
COPY package*.json ./
COPY . .
RUN npm ci
CMD ["node", "src/index.js"]