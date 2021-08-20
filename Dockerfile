FROM kubemq/gobuilder as builder
ARG VERSION
ARG GIT_COMMIT
ARG BUILD_TIME
ENV GOPATH=/go
ENV PATH=$GOPATH:$PATH
ENV ADDR=0.0.0.0
ADD . $GOPATH/github.com/kubemq-io/kubemq-community
WORKDIR $GOPATH/github.com/kubemq-io/kubemq-community
RUN go mod vendor
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -a -mod=vendor -installsuffix cgo -ldflags="-w -s -X main.version=$VERSION" -o kubemq-run .
FROM registry.access.redhat.com/ubi8/ubi-minimal
MAINTAINER KubeMQ info@kubemq.io
ENV GOPATH=/go
ENV PATH=$GOPATH/bin:$PATH
RUN mkdir /kubemq
COPY --from=builder $GOPATH/github.com/kubemq-io/kubemq-community/kubemq-run ./kubemq
COPY entrypoint.sh /
RUN chown -R 1001:root  /kubemq && chmod g+rwX  /kubemq
EXPOSE 50000
EXPOSE 9090
EXPOSE 8080
#RUN adduser -D kubemq
WORKDIR kubemq
USER 1001
CMD ["./kubemq-run"]

