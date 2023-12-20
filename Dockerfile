FROM golang:alpine
ENV CGO_ENABLED=0

WORKDIR /github.com/apache/datasketches-go/
COPY go.mod .
COPY go.sum .
COPY main.go .
COPY common ./common
COPY thetacommon ./thetacommon
COPY hll ./hll


RUN go mod download
RUN go build -v ./...

CMD ["go", "test", "-v", "./..."]
