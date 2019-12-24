// server.go

package main

import (
	"encoding/gob"
	"fmt"
	"os"

	// "fmt"
	"net"
	"runtime"
)

type GobTransport struct {
	Type string
	Data interface{}
}

type ServerVal struct {
	Server string
	Object string
	Value  string
}

var objects map[string]string

func main() {

	fmt.Println("Hello World!\n")

	gob.Register(ServerVal{})

	objects = make(map[string]string)

	// srvrPort := "4444"

	srvrPort := os.Args[1]

	// Server accepts all incoming connections (coordinator)
	var port_str string = ":" + srvrPort
	ln, err := net.Listen("tcp", port_str)
	if err != nil {
		panic(err)
	}

	conn, err := ln.Accept()
	if err != nil {
		panic(err)
	}

	listenForUpdates(conn)

	runtime.Goexit()
}

func listenForUpdates(conn net.Conn) {

	dec := gob.NewDecoder(conn)
	enc := gob.NewEncoder(conn)
	fmt.Println("Server listening for updates...\n")

	for {

		// dec := gob.NewDecoder(conn)
		// enc := gob.NewEncoder(conn)

		var gob_data GobTransport
		err := dec.Decode(&gob_data)

		if err != nil {
			panic(err)
			// continue
			//return
		}

		fmt.Println("Server received GOB\n")
		fmt.Println("Data: ", gob_data.Type)

		if gob_data.Type == "COMMIT_UPDATE" {

			fmt.Println("Received commit update \n")
			obj := gob_data.Data.(ServerVal).Object
			val := gob_data.Data.(ServerVal).Value

			// deletes the object if it already exists, else does nothing
			delete(objects, obj)

			// (Re)load objects
			objects[obj] = val

		} else if gob_data.Type == "GET_QUERY" {

			srvr := gob_data.Data.(ServerVal).Server // server
			obj := gob_data.Data.(ServerVal).Object  // object
			// val := gob_data.Data.(ServerVal).Value   // value
			find_data, ok := objects[obj]

			if !ok {
				gob_data_write := GobTransport{Type: "GET_ABORTED", Data: interface{}("nil")}
				err := enc.Encode(gob_data_write)
				if err != nil {
					panic(err)
				}

			} else {
				getVals := ServerVal{Server: srvr, Object: obj, Value: find_data}
				gob_data_write := GobTransport{Type: "GET_RESPONSE", Data: interface{}(getVals)}
				err := enc.Encode(gob_data_write)
				if err != nil {
					panic(err)
				}
			}

		} else if gob_data.Type == "SET_QUERY" {

			fmt.Println("Received SET Query\n")

			// srvr := gob_data.Data.(ServerVal).Server // server
			obj := gob_data.Data.(ServerVal).Object // object
			val := gob_data.Data.(ServerVal).Value

			objects[obj] = val

			var gob_data_write GobTransport
			// sends back to the client
			fmt.Println("Sending SET Response\n")
			gob_data_write = GobTransport{Type: "SET_RESPONSE", Data: interface{}("nil")}

			err := enc.Encode(gob_data_write)

			if err != nil {
				panic(err)
			}

		}
	}
}
