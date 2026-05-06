package main

import (
	"fmt"
	"context"
	"net"
	pb "snowcast-jamesyan2028/pkg/protocol"

)

type SnowcastServer struct {
	pb.UnimplementedSnowcastControlServer
	numStations int
}

func (s *SnowcastServer) Handshake(ctx context.Context, req *pb.HelloMessage) (*pb.WelcomeMessage, error) {
	udpPort := req.UdpPort
	fmt.Printf("Client handshake received on UDP port %d\n", udpPort)

	clientIP := getClientIP(ctx)

	udpAddr := &net.UDPAddr{
		IP:   net.ParseIP(clientIP),
		Port: int(udpPort),
	}

	clientKey := fmt.Sprintf("%s:%d", clientIP, udpPort)

	client := &ClientInfo{
		udpAddr: udpAddr,
		currStation: -1,
		eventChan: make(chan *pb.ServerEvent, 100),
	}

	clientMutex.Lock()
	currentClients[clientKey] = client
	clientMutex.Unlock()

	fmt.Printf("Client successfully connected: %s\n", clientKey)
	
	return &pb.WelcomeMessage{NumStations: uint32(s.numStations)}, nil
}

func (s *SnowcastServer) SetStation(req *pb.SetStationMessage, stream pb.SnowcastControl_SetStationServer) error {
	stationNum := int(req.StationNumber)

	if stationNum < 0 || stationNum >= len(stationList) {
		stream.Send(&pb.ServerEvent{
			Event: &pb.ServerEvent_Invalid{
					Invalid: &pb.InvalidCommandMessage{
						ReplyString: "Invalid Station Number",
					},
			},
		})
		return fmt.Errorf("invalid station number: %d", stationNum)
	}

	clientIP := getClientIP(stream.Context())
	

	clientKey := fmt.Sprintf("%s:%d", clientIP, req.UdpPort)
	clientMutex.Lock()
	clientInfo, found := currentClients[clientKey]
	clientMutex.Unlock()

	if !found {
		return fmt.Errorf("client not found, %s\n", clientKey)
	}

	removeFromCurrentStation(clientInfo)

	clientMutex.Lock()
	clientInfo.currStation = stationNum
	bitrate := req.Bitrate
	if bitrate == "" {
		bitrate = "high"
	}
	clientInfo.bitrate = bitrate
	clientMutex.Unlock()
	
	station := &stationList[stationNum]
	station.mutex.Lock()
	station.clients = append(station.clients, clientInfo)
	station.mutex.Unlock()

	stream.Send(&pb.ServerEvent{
		Event: &pb.ServerEvent_Announce{
			Announce: &pb.AnnounceMessage{
				SongName: station.name,
			},
		},
	})

	for {
		select {
		case event, ok := <-clientInfo.eventChan:
			if !ok {
				return nil
			}
			if err := stream.Send(event); err != nil {
				return err
			}
		case <-stream.Context().Done():
			return stream.Context().Err()
		}
	}
}

func (s *SnowcastServer) ListStations(ctx context.Context, req *pb.ListStationsRequest) (*pb.ListStationsResponse, error) {
    var stations []*pb.StationInfo
    for i := range stationList {
        station := &stationList[i]
        station.mutex.Lock()
        stations = append(stations, &pb.StationInfo{
            StationNumber: uint32(i),
            SongName:      station.name,
            BitrateLevels: []string{"low", "medium", "high"},
        })
        station.mutex.Unlock()
    }
    return &pb.ListStationsResponse{Stations: stations}, nil
}

func (s *SnowcastServer) Disconnect(ctx context.Context, req *pb.DisconnectRequest) (*pb.DisconnectResponse, error) {
	clientIP := getClientIP(ctx)
	clientKey := fmt.Sprintf("%s:%d", clientIP, req.UdpPort)

	clientMutex.Lock()
	client, found := currentClients[clientKey]
	if !found {
		clientMutex.Unlock()
		return &pb.DisconnectResponse{Success: false}, nil
	}
	delete(currentClients, clientKey)
	clientMutex.Unlock()

	removeFromCurrentStation(client)
	close(client.eventChan)

	fmt.Printf("Client disconnected: %s\n", clientKey)
	return &pb.DisconnectResponse{Success: true}, nil
}



func getClientIP(ctx context.Context) (string) {
	return "127.0.0.1"
}


func removeFromCurrentStation(client *ClientInfo) {
	if client.currStation == -1 {
		return
	}
 
	station := &stationList[client.currStation]
	station.mutex.Lock()
	for i, c := range station.clients {
		if c == client {
			station.clients[i] = station.clients[len(station.clients)-1]
			station.clients = station.clients[:len(station.clients)-1]
			break
		}
	}
	station.mutex.Unlock()
}