package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"time"
	"unicode"

	pb "snowcast-jamesyan2028/pkg/protocol"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	joinedStation bool = false
	listenerPort  uint64
)

func main() {
	if len(os.Args) != 4 {
		log.Fatalf("Usage: ./snowcast_control <server IP> <server port> <listener_port>")
		return
	}
	clientIP := os.Args[1]
	clientPort := os.Args[2]

	var err error
	listenerPort, err = strconv.ParseUint(os.Args[3], 10, 16)
	if err != nil {
		log.Fatalf("Error parsing listener port: %v", err)
		return
	}

	conn, err := grpc.NewClient(clientIP+":"+clientPort, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Error connecting to server: %v", err)
	}
	defer conn.Close()

	client := pb.NewSnowcastControlClient(conn)

	hello := &pb.HelloMessage{
		UdpPort: uint32(listenerPort),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	welcome, err := client.Handshake(ctx, hello)

	if err != nil {
		log.Fatalf("Handshake failed: %v", err)
	}
	fmt.Printf("Welcome to Snowcast! The server has %d stations\n", welcome.NumStations)
 
	exit := make(chan bool)

	go func() {
		handleUserInput(client)
		exit <- true
	}()

	<- exit

	client.Disconnect(context.Background(), &pb.DisconnectRequest{
		UdpPort: uint32(listenerPort),
	})
}

func handleUserInput(client pb.SnowcastControlClient) {
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		input := scanner.Text()
		input = strings.TrimSpace(input)
		switch {
		case input == "q":
			return
		case input == "":
			continue
		case len(strings.Fields(input)) >= 1 && checkOnlyNumbers(strings.Fields(input)[0]):
			fields := strings.Fields(input)
			stationNum, err := strconv.ParseUint(fields[0], 10, 16)
			if err != nil {
				fmt.Printf("Error Parsing Station Number: %s\n", err)
				continue
			}

			bitrate := "high"
			if len(fields) >= 2 {
				b := strings.ToLower(fields[1])
				if b == "low" || b == "medium" || b == "high" {
					bitrate = b
				} else {
					fmt.Printf("Invalid bitrate '%s', using 'high'. Options: low, medium, high\n", fields[1])
				}
			}

			stream, err := client.SetStation(context.Background(), &pb.SetStationMessage{
				StationNumber: uint32(stationNum),
				UdpPort:       uint32(listenerPort),
				Bitrate: bitrate,
			})

			if err != nil {
				fmt.Printf("Error setting station: %v\n", err)
				continue
			}

			joinedStation = true
			go handleServerStream(stream)
		case input == "l":
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			resp, err := client.ListStations(ctx, &pb.ListStationsRequest{})
			cancel()
			if err != nil {
				fmt.Printf("Error listing stations: %v\n", err)
				continue
			}
			for _, s := range resp.Stations {
				fmt.Printf("Station %d: %s [bitrates: %s]\n",
					s.StationNumber, s.SongName, strings.Join(s.BitrateLevels, ", "))
			}
		default:
			fmt.Printf("Invalid Command: %s\n", input)
			continue
		}
	}
}

func handleServerStream(stream pb.SnowcastControl_SetStationClient) {
	for {
		event, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				return
			}
			fmt.Printf("Stream error: %v\n", err)
			return
		}
 
		switch evt := event.Event.(type) {
		case *pb.ServerEvent_Announce:
			fmt.Printf("New Song Announced: %s\n", evt.Announce.SongName)
		case *pb.ServerEvent_Invalid:
			fmt.Printf("Invalid Command: %s\n", evt.Invalid.ReplyString)
			return
		}
	}
}

func checkOnlyNumbers(s string)bool {
	for _, r := range s {
		if !unicode.IsDigit(r) {
			return false
		}
	} 
	return true
}