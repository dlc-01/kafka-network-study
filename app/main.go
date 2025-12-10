package main

import (
	"fmt"
	"net"
	"os"

	"github.com/codecrafters-io/kafka-starter-go/domain"
	"github.com/codecrafters-io/kafka-starter-go/domain/request"
	"github.com/codecrafters-io/kafka-starter-go/domain/response"
)

func main() {
	fmt.Println("Logs from your program will appear here!")

	l, err := net.Listen("tcp", "0.0.0.0:9092")
	if err != nil {
		fmt.Println("Failed to bind to port 9092")
		os.Exit(1)
	}

	conn, err := l.Accept()
	if err != nil {
		fmt.Println("Error accepting connection: ", err.Error())
		os.Exit(1)
	}
	defer conn.Close()

	buffer := make([]byte, 128)
	_, err = conn.Read(buffer)
	if err != nil {
		fmt.Println("Error reading buffer: ", err.Error())
	}
	msgRequest, err := request.ParseMessageRequest(buffer)
	if err != nil {
		fmt.Println("Error request MessageRequest: ", err.Error())
	}
	headerRequest := msgRequest.Header()

	headerResponse := response.ParseRequestHeader(&headerRequest)
	msg := response.NewMessageResponse(headerResponse, []domain.ApiKey{domain.NewApiKey(18, 0, 4, []byte{})}, 0)
	conn.Write(msg.ToBytes())
}
