# Runtime image for Portage. The binary is built by GoReleaser (see
# .goreleaser.yaml, dockers_v2), which places it at $TARGETPLATFORM/portage in
# the build context, so this file never compiles anything.
FROM gcr.io/distroless/static-debian13:nonroot
ARG TARGETPLATFORM
COPY $TARGETPLATFORM/portage /usr/bin/portage
USER nonroot:nonroot
ENTRYPOINT ["/usr/bin/portage"]
