FROM golang:1.17.7-alpine3.15
WORKDIR /usr/src/app:
COPY go.mod go.sum ./
RUN go mod download && go mod verify
COPY . .
RUN apk update && apk add git && apk add nodejs && apk add npm
RUN npm install
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o main main.go
CMD ./main
