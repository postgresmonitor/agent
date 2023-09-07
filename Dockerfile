# build step to keep final image small
FROM golang:1.21-alpine as build

WORKDIR /app

RUN apk add --update --no-cache

COPY . ./

# build dependencies
RUN go get .

# build
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o bin/agent .


# final image
FROM alpine:3.18.3

LABEL maintainer="contact@postgresmonitor.com"

ARG APP_HOME=/app
ARG APP_USER=agent
WORKDIR $APP_HOME

RUN addgroup -S $APP_USER && adduser -S $APP_USER -G $APP_USER && chown -R $APP_USER:$APP_USER $APP_HOME

USER $APP_USER

# copy from build stage
COPY --chown=$APP_USER:$APP_USER --from=build $APP_HOME $APP_HOME

CMD ["bin/agent"]
