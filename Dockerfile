FROM golang

RUN go get -u github.com/Zilliqa/goduplicator

CMD ["goduplicator"]