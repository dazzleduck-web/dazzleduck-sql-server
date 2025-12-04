FROM node:20-alpine

WORKDIR /main

COPY package*.json .

RUN npm install

COPY .. .

EXPOSE 5174

CMD ["npm", "run", "dev"]