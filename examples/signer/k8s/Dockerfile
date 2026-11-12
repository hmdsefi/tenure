# Builds one program of the examples module into a binary-only image. Build from the repository
# root, which is the context, because the examples module replaces the root and k8s modules with
# their directories:
#
#   docker build -f examples/signer/k8s/Dockerfile --build-arg PKG=./signer/k8s -t tenure-signer:dev .
#   docker build -f examples/signer/k8s/Dockerfile --build-arg PKG=./signer/noncesvc -t tenure-noncesvc:dev .
FROM golang:1.26.0 AS build
# PKG is the package to build, relative to the examples module.
ARG PKG
COPY . /src
WORKDIR /src/examples
RUN test -n "$PKG" && GOWORK=off CGO_ENABLED=0 GOOS=linux \
    go build -trimpath -ldflags="-s -w" -o /out/app "$PKG"

FROM scratch
COPY --from=build /out/app /app
USER 65532
ENTRYPOINT ["/app"]
