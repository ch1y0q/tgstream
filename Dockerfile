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
    chmod +x /rootfs/etc/s6-overlay/s6-rc.d/*/finish || true && \
    sed -i "s|__IMAGE_VERSION__|${IMAGE_VERSION}|g; s|__BUILD_DATE__|${BUILD_DATE}|g" \
        /rootfs/usr/local/bin/entrypoint
COPY --from=s6-builder /s6/ /rootfs/

# Main image
FROM alpine:3.24.1

ARG IMAGE_VERSION
ARG BUILD_DATE
ARG TARGETARCH

LABEL org.opencontainers.image.authors="Alexander Zinchenko <alexander@zinchenko.com>" \
      org.opencontainers.image.description="Capture Telegram channel live streams, restream to Plex/Jellyfin as live TV and record to files." \
      org.opencontainers.image.title="tgstream" \
      org.opencontainers.image.source="https://github.com/azinchen/tgstream" \
      org.opencontainers.image.url="https://github.com/azinchen/tgstream" \
      org.opencontainers.image.version="${IMAGE_VERSION}" \
      org.opencontainers.image.created="${BUILD_DATE}"

ENV S6_BEHAVIOUR_IF_STAGE2_FAILS=2 \
    S6_CMD_WAIT_FOR_SERVICES_MAXTIME=120000 \
    PYTHONUNBUFFERED=1

# MTProto capture: no browser. Telethon pulls the stream chunks; ffmpeg
# remuxes (-c copy) and generates the slate; qrencode renders the login QR.
RUN echo "**** install mandatory packages ****" && \
    apk --no-cache --no-progress add \
        curl=8.23.0-r0 \
        ffmpeg=8.1.3-r0 \
        font-dejavu=2.37-r6 \
        font-noto=2026.11.01-r0 \
        jq=1.8.2-r0 \
        libqrencode-tools=4.1.1-r3 \
        py3-pyaes=1.6.1-r7 \
        py3-rsa=4.9.1-r1 \
        py3-telethon=1.43.2-r0 \
        python3=3.14.10-r1 \
        tzdata=2026f-r0 \
        && \
    echo "**** install PyAV (in-process remux; abi3 musllinux wheel) ****" && \
    apk --no-cache --no-progress add --virtual .pip py3-pip && \
    pip install --break-system-packages --no-cache-dir av==18.1.0 && \
    apk del .pip && \
    echo "**** cleanup ****" && \
    rm -rf /tmp/* /var/cache/apk/* /root/.cache

COPY --from=rootfs-builder /rootfs/ /

HEALTHCHECK --interval=60s --timeout=10s --start-period=30s --retries=3 \
    CMD ["/usr/local/bin/tgstream-healthcheck"]

EXPOSE 8409

ENTRYPOINT ["/usr/local/bin/entrypoint"]
