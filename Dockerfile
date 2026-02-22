FROM node:22-alpine

WORKDIR /app

COPY package.json package-lock.json* ./
RUN npm install --production

COPY server.js edcClient.js ./

EXPOSE 3000

CMD ["node", "server.js"]
