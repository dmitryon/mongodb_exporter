FROM alpine AS builder
RUN apk add --no-cache ca-certificates curl  \
    # Download a root bundle for DocumentDB
    && curl -s https://truststore.pki.rds.amazonaws.com/global/global-bundle.pem -o /opt/global-bundle.pem


FROM scratch AS final
USER 65535:65535
COPY  --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY  --from=builder /opt/global-bundle.pem /
COPY ./mongodb_exporter /
EXPOSE 9216
ENTRYPOINT ["/mongodb_exporter"]