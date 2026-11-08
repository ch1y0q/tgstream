ARG IMAGE_VERSION=N/A
ARG BUILD_DATE=N/A

# s6 overlay builder
FROM alpine:3.24.1 AS s6-builder

ARG TARGETARCH
ARG TARGETVARIANT

ENV PACKAGE="just-containers/s6-overlay"
ENV PACKAGEVERSION="3.2.3.2"

RUN echo "**** install mandatory packages ****" && \
    apk --no-cache --no-progress add \
        tar=1.35-r5 \
        xz=5.8.4-r0 \
        && \
    echo "**** create folders ****" && \
    mkdir -p /s6 && \
    echo "**** download ${PACKAGE} ****" && \
    echo "Target arch: ${TARGETARCH}${TARGETVARIANT}" && \
    case "${TARGETARCH}${TARGETVARIANT}" in \
        amd64)      s6_arch="x86_64" ;; \
        arm64)      s6_arch="aarch64" ;; \
        armv7)      s6_arch="arm" ;; \
        *)          s6_arch="x86_64" ;; \
    esac && \
    s6_url_base="https://github.com/${PACKAGE}/releases/download/v${PACKAGEVERSION}" && \
    wget -q "${s6_url_base}/s6-overlay-noarch.tar.xz" -qO /tmp/s6-overlay-noarch.tar.xz && \
    wget -q "${s6_url_base}/s6-overlay-${s6_arch}.tar.xz" -qO /tmp/s6-overlay-binaries.tar.xz && \
    wget -q "${s6_url_base}/s6-overlay-symlinks-noarch.tar.xz" -qO /tmp/s6-overlay-symlinks-noarch.tar.xz && \
    wget -q "${s6_url_base}/s6-overlay-symlinks-arch.tar.xz" -qO /tmp/s6-overlay-symlinks-arch.tar.xz && \
    tar -C /s6/ -Jxpf /tmp/s6-overlay-noarch.tar.xz && \
    tar -C /s6/ -Jxpf /tmp/s6-overlay-binaries.tar.xz && \
    tar -C /s6/ -Jxpf /tmp/s6-overlay-symlinks-noarch.tar.xz && \
    tar -C /s6/ -Jxpf /tmp/s6-overlay-symlinks-arch.tar.xz

# rootfs builder
FROM alpine:3.24.1 AS rootfs-builder

ARG IMAGE_VERSION
ARG BUILD_DATE

COPY root/ /rootfs/
RUN chmod +x /rootfs/usr/local/bin/* || true && \
    chmod +x /rootfs/etc/s6-overlay/s6-rc.d/*/run || true && \
    sed -i "s|__IMAGE_VERSION__|${IMAGE_VERSION}|g; s|__BUILD_DATE__|${BUILD_DATE}|g" \
        /rootfs/usr/local/bin/entrypoint
COPY --from=s6-builder /s6/ /rootfs/

# Main image
FROM alpine:3.24.1

ARG IMAGE_VERSION
ARG BUILD_DATE

LABEL org.opencontainers.image.authors="Alexander Zinchenko <alexander@zinchenko.com>" \
      org.opencontainers.image.description="Guide aggregator for tgstream: merges capture containers into one M3U playlist and XMLTV guide for Plex/Jellyfin." \
      org.opencontainers.image.title="tgstream-guide" \
      org.opencontainers.image.source="https://github.com/azinchen/tgstream" \
      org.opencontainers.image.url="https://github.com/azinchen/tgstream" \
      org.opencontainers.image.version="${IMAGE_VERSION}" \
      org.opencontainers.image.created="${BUILD_DATE}"

ENV S6_BEHAVIOUR_IF_STAGE2_FAILS=2

RUN echo "**** install mandatory packages ****" && \
    apk --no-cache --no-progress add \
        busybox-extras=1.37.0-r31 \
        curl=8.23.0-r0 \
        jq=1.8.2-r0 \
        tzdata=2026f-r0 \
        && \
    echo "**** cleanup ****" && \
    rm -rf /tmp/* /var/cache/apk/*

COPY --from=rootfs-builder /rootfs/ /

HEALTHCHECK --interval=60s --timeout=10s --start-period=60s --retries=3 \
    CMD ["/usr/local/bin/guide-healthcheck"]

EXPOSE 8409

ENTRYPOINT ["/usr/local/bin/entrypoint"]
