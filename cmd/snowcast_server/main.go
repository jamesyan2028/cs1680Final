package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	pb "snowcast-jamesyan2028/pkg/protocol"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
)

//clientMap := make(map[string]int)

type ClientInfo struct {
	udpAddr *net.UDPAddr
	currStation int
	bitrate     string
	eventChan chan *pb.ServerEvent
}

type Station struct {
	mutex sync.Mutex
	clients []*ClientInfo
	name string
	bitrateKbps map[string]int
}

var (
	currentClients = make(map[string]*ClientInfo)
	clientMutex sync.Mutex
	stationList []Station
)

func main() {
	if len(os.Args) < 3 {
		log.Fatalf("Usage: ./snowcast_server <listen port> <file0> [file1] ...")
		return
	}
	port := os.Args[1]
	files := os.Args[2:]

	stationList = make([]Station, len(files))

	udpAddr, err := net.ResolveUDPAddr("udp", "127.0.0.1:"+port)
	if err != nil {
		log.Fatalf("Error creating UDP address: %s", err)
	}
	udpConn, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		log.Fatalf("Error starting UDP sender: %s", err)
	}

	for i, path := range files {
		stationList[i].name = path
		go streamStation(i, path, udpConn)
	}

	listen, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatalf("Failed to listen: %s", err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterSnowcastControlServer(grpcServer, &SnowcastServer{
		numStations: len(files),
	})

	fmt.Printf("Snowcast server started on port %s with %d stations\n", port, len(files))

	go func() {
		if err := grpcServer.Serve(listen); err != nil {
			log.Fatalf("Failed to serve gRPC: %s", err)
		}
	}()

	handleUserInput(grpcServer)	

}

func streamStation(id int, filename string, udpConn *net.UDPConn) {
	file, err := os.Open(filename)
	if err != nil {
		fmt.Printf("Error opening file: " + "%s", filename)
		return
	}
	defer file.Close()
	buffer := make([]byte, 1500)
	ticker := time.NewTicker(91550 * time.Microsecond)
	defer ticker.Stop()
	for range ticker.C {
		n, err := file.Read(buffer)
		if n > 0 {
			currStation := &stationList[id]
			currStation.mutex.Lock()
			for _, client := range currStation.clients {
				chunkSize := bitrateChunkSize(client.bitrate)
				if n < chunkSize {
					chunkSize = n
				}
				udpConn.WriteToUDP(buffer[:chunkSize], client.udpAddr)
			}
			currStation.mutex.Unlock()
		}

		if err != nil {
			if err == io.EOF {
				file.Seek(0, 0)
				event := &pb.ServerEvent{
					Event: &pb.ServerEvent_Announce{
						Announce: &pb.AnnounceMessage{
							SongName: filename,
						},
					},
				}
				station := &stationList[id]
				station.mutex.Lock()
				for _, client := range station.clients {
					select {
					case client.eventChan <- event:
					default:
					}
				}
				station.mutex.Unlock()
			} else {
				fmt.Printf("Error reading file: %s", err)
			}
		}

	}
}

func bitrateChunkSize(bitrate string) int {
    switch bitrate {
    case "low":
        return 375   // 64 kbps
    case "medium":
        return 750   // 128 kbps
    default:         
        return 1500  // around 192 kbps
    }
}

func handleUserInput(grpcServer *grpc.Server) {
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		input := scanner.Text()
		input = strings.TrimSpace(input)

		words := strings.Fields(input)
		if len(words) == 0 {
			continue
		}
		switch words[0] {
		case "p":
			if len(words) == 1 {
				fmt.Print(formatStationString())
			} else if len(words) == 2 {
				fileName := words[1]
				err := os.WriteFile(fileName, []byte(formatStationString()), 0644)
				if err != nil {
					fmt.Printf("Error writing output to file: %s", err)
				}
			} else {
				fmt.Printf("Invalid Command Type\n")
			}
		case "q":
			//May be an error here if deleting while iterating through
			clientMutex.Lock()
			for key, client := range currentClients {
				close(client.eventChan)
				delete(currentClients, key)
			}
			clientMutex.Unlock()
			grpcServer.GracefulStop()
			os.Exit(0)
			return
		default:
			fmt.Printf("Invalid command\n")
		} 

	}
	select {}
}

func formatStationString () string {
	var builder strings.Builder
	clientMutex.Lock()
	defer clientMutex.Unlock()

	for i := range stationList {
		station := &stationList[i]
		station.mutex.Lock()
		builder.WriteString(fmt.Sprintf("%d,%s", i, station.name))

		for _, client := range station.clients {
			builder.WriteString(fmt.Sprintf(",%s:%d", client.udpAddr.IP.String(), client.udpAddr.Port))
		}
		builder.WriteString("\n")
		station.mutex.Unlock()
	}
	return builder.String()
}