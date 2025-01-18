FROM ubuntu:24.04

RUN apt update

RUN apt install golang-go -y

RUN apt install git -y

RUN git clone https://github.com/meirdev/goflow2.git && \
    cd goflow2 && \
    go build -o /opt/goflow2 cmd/goflow2/main.go

ENTRYPOINT ["/opt/goflow2"]

CMD ["-transport", "clickhouse", "-format", "bin", "-transport.clickhouse.dsn", "clickhouse://clickhouse:9000/default"]
