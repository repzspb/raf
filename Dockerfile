FROM golang:1.26-alpine AS build

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -o /raf ./cmd/raf

FROM scratch
COPY --from=build /raf /raf
EXPOSE 8080
ENTRYPOINT ["/raf"]
