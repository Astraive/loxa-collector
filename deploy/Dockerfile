FROM golang:1.22
WORKDIR /workspace
COPY . .
RUN go mod download
CMD ["go", "test", "./..."]
