FROM alpine:latest AS core

RUN apk update
RUN apk upgrade
RUN apk add --update bash cmake g++ gcc git make vips vips-dev

COPY --from=golang:1.24-alpine /usr/local/go/ /usr/local/go/
ENV PATH="/usr/local/go/bin:${PATH}"

WORKDIR /state-consumer/src

COPY backend/go.mod backend/
COPY backend/go.mod backend/
COPY backend/go.sum backend/
COPY core/go.mod core/
COPY core/go.sum core/
COPY postgres-data-handler/go.mod postgres-data-handler/
COPY postgres-data-handler/go.sum postgres-data-handler/
COPY state-consumer/go.mod state-consumer/
COPY state-consumer/go.sum state-consumer/

WORKDIR /state-consumer/src/state-consumer

RUN go mod download

# include backend src
COPY backend/apis      ../backend/apis
COPY backend/config    ../backend/config
COPY backend/cmd       ../backend/cmd
COPY backend/miner     ../backend/miner
COPY backend/routes    ../backend/routes
COPY backend/countries ../backend/countries
COPY backend/scripts   ../backend/scripts

## include core src
COPY core/desohash ../core/desohash
COPY core/cmd       ../core/cmd
COPY core/lib       ../core/lib
COPY core/migrate   ../core/migrate
COPY core/bls         ../core/bls
COPY core/collections ../core/collections
COPY core/consensus   ../core/consensus

## include postgres-data-handler src
COPY postgres-data-handler/handler ../postgres-data-handler/handler
COPY postgres-data-handler/entries ../postgres-data-handler/entries
COPY postgres-data-handler/entries ../postgres-data-handler/migrations
COPY postgres-data-handler/main.go ../postgres-data-handler/main.go
COPY postgres-data-handler/tests ../postgres-data-handler/tests
COPY postgres-data-handler/migrations ../postgres-data-handler/migrations

## include state-consumer src
COPY state-consumer/consumer consumer
COPY state-consumer/consumer .
COPY state-consumer/tests tests

RUN go mod tidy

# No need to build since we're just running tests
# ENTRYPOINT ["ls", "../"]
ENTRYPOINT ["go", "test", "./tests", "-v", "-failfast", "-p", "1"]
