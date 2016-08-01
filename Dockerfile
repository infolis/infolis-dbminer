# Run as:
# docker run -d --name infolis-webapp -p '3000:3000' infolis/infolis-web
FROM alpine

WORKDIR /app

COPY entrypoint.sh /app/entrypoint.sh

CMD sh entrypoint.sh
