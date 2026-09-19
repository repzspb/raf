FROM golang:1.26-alpine AS build

WORKDIR /src
COPY . .
RUN CGO_ENABLED=0 go build -o /raf ./cmd/raf

FROM scratch
COPY --from=build /raf /raf
EXPOSE 8080
ENTRYPOINT ["/raf"]
