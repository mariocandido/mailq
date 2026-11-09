FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/mailq ./cmd/mailq \
 && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/fakesmtp ./cmd/fakesmtp

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/ /usr/local/bin/
USER nonroot
ENTRYPOINT ["/usr/local/bin/mailq"]
