FROM golang:1.24-alpine AS build

WORKDIR /src

COPY go.mod go.sum* ./
RUN go mod download

COPY . .

ARG SERVICE
RUN go build -o /out/service ./cmd/${SERVICE}

FROM alpine:3.22

WORKDIR /app

COPY --from=build /out/service /app/service
COPY migrations /app/migrations

ENTRYPOINT ["/app/service"]
