FROM ubuntu:24.04

RUN apt update

RUN apt install golang-go -y

RUN apt install git -y

RUN git clone https://github.com/open-net-tools/goflow2.git && \
    cd goflow2 && \
    go build -o /opt/goflow2 cmd/goflow2/main.go

ENTRYPOINT ["/opt/goflow2"]

CMD ["-transport", "clickhouse", "-format", "bin", "-transport.clickhouse.dsn", "clickhouse://db:9000/default", "-loglevel", "debug", "-listen", "netflow://0.0.0.0:2055"]
