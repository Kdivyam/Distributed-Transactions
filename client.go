package main

import (
	"bufio"
	"encoding/gob"
	"fmt"
	"net"
	"os"
	"runtime"
	"strings"
)

type GobTransport struct {
	Type string
	Data interface{}
}

type ServerVal struct {
	Server     string
	Object     string
	Value      string
	ClientName string
}

var coordAddress string
var coordPort string

// var localPort string
var nodeName string

var yieldSetChannel chan bool
var yieldGetChannel chan bool
var scanSetChannel chan bool
var scanGetChannel chan bool

func main() {
	// fmt.Println("Entering main...")

	gob.Register(ServerVal{})

	yieldSetChannel = make(chan bool)
	yieldGetChannel = make(chan bool)
	scanSetChannel = make(chan bool)
	scanGetChannel = make(chan bool)

	coordAddress = "sp19-cs425-g31-09.cs.illinois.edu" // Totally not hardcoded
	coordPort = "4444"                                 // Hardcoded

	// Client port
	localPort := os.Args[1]

	nodeName, _ = os.Hostname()
	// Append local port
	nodeName = nodeName + localPort

	coordConn := handleCoordConnection(coordAddress, coordPort)

	readInput(coordConn)

	runtime.Goexit()
	// fmt.Println("Exit")
}

// readInput ingests typed messages from the console
// broadcasts message to all connected chat users
func readInput(coordConn net.Conn) {
	// inTransaction := false

	// read from standard in (console)
	// reader := bufio.NewReader(os.Stdin)
	s := bufio.NewScanner(os.Stdin)
	// fmt.Println("Created Scanner\n")
	for {
		// if inTransaction {
		// 	continue
		// }

		// check when user presses return
		//text, err := reader.ReadString('\n')
		text := s.Text()
		//if err == nil {
		parts := strings.SplitN(strings.TrimSpace(text), " ", 2)
		command := parts[0]
		// payload := parts[1]

		// fmt.Println("Command: ", command, "\n")

		if command == "BEGIN" {
			fmt.Println("OK\n")
			// fmt.Println("Parsed BEGIN\n")
			// make current client as in a transaction
			// inTransaction = true
			sendBegin(coordConn)
		} else if command == "GET" {
			// fmt.Println("Parsed GET\n")
			payload := parts[1]
			// get value on server
			objLoc := strings.SplitN(payload, ".", 2) // ["A", "x"]
			sendGet(coordConn, objLoc[0], objLoc[1])

			go func() {
				scanGetChannel <- s.Scan()
			}()

			// Spin until get returns
			select {
			case <-yieldGetChannel:
				// fmt.Println("Channel returned!\n")

				scanned := <-scanGetChannel
				if !scanned {
					break
				}
			case scanned := <-scanGetChannel: // read ABORTED:
				// fmt.Println("GET scanned ABORT?")

				// abort by clearing structures
				if !scanned {
					break
				}

				if s.Text() == "ABORT" {
					// ABORT THE TRANSACTION
					sendAbort(coordConn)
					s.Scan()
				}

				select {
				case <-yieldGetChannel:
					continue
				default:
					continue
				}
			}
		} else if command == "SET" {
			// set value on server
			payload := parts[1]
			components := strings.SplitN(payload, " ", 2)   // ["A.x", "1"]
			objLoc := strings.SplitN(components[0], ".", 2) // ["A", "x"]
			sendSet(coordConn, objLoc[0], objLoc[1], components[1])

			go func() {
				scanSetChannel <- s.Scan()

			}()

			// Spin until get returns
			select {
			case <-yieldSetChannel:
				// fmt.Println("Channel returned!\n")

				scanned := <-scanSetChannel
				if !scanned {
					break
				}
			case scanned := <-scanSetChannel: // read ABORTED:
				// abort by clearing structures
				if !scanned {
					break
				}

				if s.Text() == "ABORT" {
					// ABORT THE TRANSACTION
					sendAbort(coordConn)
					s.Scan()
				}

				select {
				case <-yieldSetChannel:
					continue
				default:
					continue
				}
			}
		} else if command == "COMMIT" {
			// commit transaction
			// release locks
			// end transaction
			sendCommit(coordConn)
			// inTransaction = false

		} else if command == "ABORT" {
			// release locks
			// end transaction
			sendAbort(coordConn)
			// inTransaction = false
		}
		//}

		if command != "GET" && command != "SET" {
			scanned := s.Scan()
			if !scanned {
				break
			}
		}
	}
}

func sendBegin(coordConn net.Conn) {

	enc := gob.NewEncoder(coordConn)
	//fmt.Println("Sending BEGIN\n")

	getVals := ServerVal{Server: "nil", Object: "nil", Value: nodeName, ClientName: nodeName}
	gob_data := GobTransport{Type: "BEGIN", Data: interface{}(getVals)}

	err := enc.Encode(gob_data)

	if err != nil {
		panic(err)
	}
}

func sendGet(coordConn net.Conn, server string, obj string) {

	// fmt.Println("Server Parsed: ", server, "\n")
	// fmt.Println("Object Parsed: ", obj, "\n")

	enc := gob.NewEncoder(coordConn)

	getVals := ServerVal{Server: server, Object: obj, Value: "nil", ClientName: nodeName} // Value is ignored

	gob_data := GobTransport{Type: "GET", Data: interface{}(getVals)}

	// fmt.Println("Sent GET to Coordinator\n")

	err := enc.Encode(gob_data)

	if err != nil {
		panic(err)
	}
}

func sendSet(coordConn net.Conn, server string, obj string, val string) {

	enc := gob.NewEncoder(coordConn)

	getVals := ServerVal{Server: server, Object: obj, Value: val, ClientName: nodeName}

	gob_data := GobTransport{Type: "SET", Data: interface{}(getVals)}

	err := enc.Encode(gob_data)

	if err != nil {
		panic(err)
	}

	// fmt.Println("Sent SET request to Coordinator\n")

}

func sendCommit(coordConn net.Conn) {

	// fmt.Println("Sending commit to Coordinator\n")

	enc := gob.NewEncoder(coordConn)

	getVals := ServerVal{Server: "nil", Object: "nil", Value: "nil", ClientName: nodeName}

	gob_data := GobTransport{Type: "COMMIT", Data: interface{}(getVals)}

	err := enc.Encode(gob_data)

	if err != nil {
		panic(err)
	}

	// fmt.Println("Sent commit request to Coordinator\n")
}

func sendAbort(coordConn net.Conn) {
	// fmt.Println("Entering sendAbort")

	enc := gob.NewEncoder(coordConn)

	getVals := ServerVal{Server: "nil", Object: "nil", Value: "nil", ClientName: nodeName}

	gob_data := GobTransport{Type: "ABORT", Data: interface{}(getVals)}

	err := enc.Encode(gob_data)

	if err != nil {
		panic(err)
	}
}

// Connect to coordinator
func handleCoordConnection(coordAddress string, coordPort string) net.Conn {

	coordRemote := coordAddress + ":" + coordPort

	var coordConn net.Conn
	var err error

	// Try connection until successful
	for {

		coordConn, err = net.Dial("tcp", coordRemote)

		if err == nil {
			break
		}

	}

	// fmt.Println("Connected to Coordinator")

	go coordListener(coordConn)

	return coordConn

}

// Listen to incoming messages from coordinator
func coordListener(conn net.Conn) {

	// dec := gob.NewDecoder(conn)

	for {

		dec := gob.NewDecoder(conn)

		var gob_data GobTransport
		err := dec.Decode(&gob_data)

		if err != nil {
			panic(err)
			//return
		}

		if gob_data.Type == "ABORTED" {
			fmt.Println("ABORTED\n")

		} else if gob_data.Type == "OK" {
			// fmt.Println("Received OK from Coordinator\n")

		} else if gob_data.Type == "GET_RESPONSE" {

			yieldGetChannel <- true
			// fmt.Println("Received GET RESPONSE \n")

			srvr := gob_data.Data.(ServerVal).Server // server
			obj := gob_data.Data.(ServerVal).Object  // object
			val := gob_data.Data.(ServerVal).Value

			fmt.Printf("%s.%s = %s\n", srvr, obj, val)

		} else if gob_data.Type == "SET_RESPONSE" {

			yieldSetChannel <- true
			fmt.Println("OK\n")

		} else if gob_data.Type == "COMMIT_OK" {
			fmt.Println("COMMIT OK\n")
		}

	}
}

// // Listen to incoming messages from coordinator
// func sendToCoord(cmd string,conn net.Conn) {

//   enc := gob.NewEncoder(conn)

//   gob_data := GobTransport{Type: cmd, Data: interface{}(transaction)}

//   err := encoder.Encode(gob_data)

//   if err != nil {
//     panic(err)
//   }
// }
