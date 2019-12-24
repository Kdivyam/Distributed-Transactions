package main

import (
	"encoding/gob"
	"fmt"
	"net"
	"runtime"
	"strings"
	"sync"
	"time"
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

var servers map[string]string

// acquired lock signal
var acquiredLock chan bool

// Do these need to be sync maps?
var serverConns map[string]net.Conn
var decoders map[string]*gob.Decoder
var encoders map[string]*gob.Encoder

// Key: "Client:Server:Object" Value:"Obj_value"
var objects sync.Map //map[string]map[string]map[string]string

// Key: "Server:Object" Value: *RWMutex
var locks sync.Map //map[string]map[string]*sync.RWMutex

// Key: "Client:Server:Object" Value: true/false
var acquired sync.Map //map[string]map[string]map[string]string

//**********************************************
// Client -> HAS/WANTS -> "Server:Object"
// var clientLockStatus map[string]map[string][]string

// Client -> list of objects "Server:Object"
var has map[string]map[string]string
var wants map[string]map[string]string

var updateLockStatus *sync.Mutex

// Object -> [List of clients reading]
var numberOfReaders map[string][]string

var numReaderLock *sync.Mutex

var visited map[string]bool

var visitedStack map[string]bool

// var deadlockSig chan bool
var deadlockSig map[string]chan bool

// Client -> Other client(s)
// var dependencyGraph map[string][]string

//**********************************************

// Key: "Server:Object" Value: true/false
// var lockInUse sync.Map

// Holds info on which client has sent abort
// var clientAbortStatus sync.Map

func main() {
	// servers will run on VMs 04-08 on port 4444
	servers = map[string]string{
		"A": "sp19-cs425-g31-09.cs.illinois.edu:7001",
		"B": "sp19-cs425-g31-09.cs.illinois.edu:7002",
		"C": "sp19-cs425-g31-09.cs.illinois.edu:7003",
		"D": "sp19-cs425-g31-09.cs.illinois.edu:7004",
		"E": "sp19-cs425-g31-09.cs.illinois.edu:7005",
	}

	coordPort := "4444"

	gob.Register(ServerVal{})

	acquiredLock = make(chan bool)
	deadlockSig = make(map[string]chan bool)
	has = make(map[string]map[string]string)
	wants = make(map[string]map[string]string)

	numberOfReaders = make(map[string][]string)

	serverConns = make(map[string]net.Conn)
	decoders = make(map[string]*gob.Decoder)
	encoders = make(map[string]*gob.Encoder)

	updateLockStatus = &sync.Mutex{}

	numReaderLock = &sync.Mutex{}

	// Client -> Server -> Object -> Value
	// objects = make(map[string]map[string]map[string]string)
	// locks = make(map[string]map[string]*sync.RWMutex)
	// acquired = make(map[string]map[string]map[string]string)

	for k, v := range servers {
		for {
			fmt.Println("Connecting to Server: ", k, "\n")
			conn, err := net.Dial("tcp", v)

			if err == nil {
				serverConns[k] = conn
				decoders[k] = gob.NewDecoder(conn)
				encoders[k] = gob.NewEncoder(conn)
				break
			}
		}

		// locks[k] = make(map[string]*sync.RWMutex)
	}

	// Coordinator accepts all incoming connections from clients
	var port_str string = ":" + coordPort
	ln, err := net.Listen("tcp", port_str)
	if err != nil {
		panic(err)
	}

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				panic(err)
			}
			fmt.Println("Accepting Client Connection\n")
			go listenForUpdates(conn)
		}
	}()

	// go detectDeadlocks()

	runtime.Goexit()
}

func listenForUpdates(conn net.Conn) {

	abortSig := make(chan bool)

	for {

		dec := gob.NewDecoder(conn)
		enc := gob.NewEncoder(conn)

		var gob_data GobTransport
		err := dec.Decode(&gob_data)

		if err != nil {
			panic(err)
		}

		fmt.Println("Received Gob Update\n")

		go func() {

			if gob_data.Type == "BEGIN" {

				fmt.Println("Received BEGIN from Client\n")

				clientName := gob_data.Data.(ServerVal).ClientName

				deadlockSig[clientName] = make(chan bool)
				has[clientName] = make(map[string]string)
				wants[clientName] = make(map[string]string)

				// clientName := gob_data.Data.(ServerVal).Value

				// Initialisation

				// // ** Initialise client abort status as false **
				// _, ok := clientAbortStatus.Load(clientName)
				// if !ok {
				// 	clientAbortStatus.Store(clientName, false)
				// } else {
				// 	// Update client status to aborted
				// 	clientAbortStatus.Delete(clientName)
				// 	clientAbortStatus.Store(clientName, true)
				// }

				//objects[clientName] = make(map[string]map[string]string)
				//acquired[clientName] = make(map[string]map[string]string)
				// for k, _ := range servers {
				// 	objects[clientName][k] = make(map[string]string)
				// 	acquired[clientName][k] = make(map[string]string)
				// }

				gob_data_write := GobTransport{Type: "OK", Data: interface{}("nil")}

				err := enc.Encode(gob_data_write)
				fmt.Println("Sending OK to Client\n")
				if err != nil {
					panic(err)
				}
			} else if gob_data.Type == "GET" {

				fmt.Println("Received GET from Client\n")

				srvr := gob_data.Data.(ServerVal).Server // server
				obj := gob_data.Data.(ServerVal).Object  // object

				clientName := gob_data.Data.(ServerVal).ClientName

				val_returned := forwardGet(clientName, srvr, obj, abortSig)

				getVals := ServerVal{Server: srvr, Object: obj, Value: val_returned}

				var gob_data_write GobTransport
				// sends back to the client
				if val_returned == "nil" {
					gob_data_write = GobTransport{Type: "ABORTED", Data: interface{}("nil")}

					err := enc.Encode(gob_data_write)

					if err != nil {
						panic(err)
					}

					// Abort transaction here i.e. clear objects associated with client, release locks
					// Abort all stored data for transaction
					doCommit(clientName, true)

				} else if val_returned != "pass" {
					gob_data_write = GobTransport{Type: "GET_RESPONSE", Data: interface{}(getVals)}

					err := enc.Encode(gob_data_write)

					if err != nil {
						panic(err)
					}
				}
			} else if gob_data.Type == "SET" {

				fmt.Println("Received SET from Client\n")

				srvr := gob_data.Data.(ServerVal).Server // server
				obj := gob_data.Data.(ServerVal).Object  // object
				val := gob_data.Data.(ServerVal).Value   // value

				clientName := gob_data.Data.(ServerVal).ClientName

				success := forwardSet(clientName, srvr, obj, val, abortSig)

				var gob_data_write GobTransport

				if success == "success" {
					fmt.Println("Sending OK to Client\n")

					// sends back to the client
					gob_data_write = GobTransport{Type: "SET_RESPONSE", Data: interface{}("nil")}

					err := enc.Encode(gob_data_write)

					if err != nil {
						panic(err)
					}
				} else if success != "pass" {

					gob_data_write = GobTransport{Type: "ABORTED", Data: interface{}("nil")}

					err := enc.Encode(gob_data_write)

					if err != nil {
						panic(err)
					}

					// Abort transaction here i.e. clear objects associated with client, release locks
					// Abort all stored data for transaction
					doCommit(clientName, true)
				}

			} else if gob_data.Type == "COMMIT" {

				clientName := gob_data.Data.(ServerVal).ClientName

				// Send local stored objects to servers
				committed := doCommit(clientName, false)

				if committed {
					gob_data_write := GobTransport{Type: "COMMIT_OK", Data: interface{}("nil")}

					err := enc.Encode(gob_data_write)

					if err != nil {
						panic(err)
					}
				} else {
					// send abort
					gob_data_write := GobTransport{Type: "ABORTED", Data: interface{}("nil")}

					err := enc.Encode(gob_data_write)

					if err != nil {
						panic(err)
					}
				}
			} else if gob_data.Type == "ABORT" {

				clientName := gob_data.Data.(ServerVal).ClientName

				// // Update client status to aborted
				// clientAbortStatus.Delete(clientName)
				// clientAbortStatus.Store(clientName, true)

				// Abort all stored data for transaction
				aborted := doCommit(clientName, true)

				select {
				case abortSig <- true:
					fmt.Println("Sending abort sig\n")
				case <-time.After(250 * time.Millisecond):
					fmt.Println("Not sending abort sig\n")
				}

				if aborted {

					gob_data_write := GobTransport{Type: "ABORTED", Data: interface{}("nil")}

					err := enc.Encode(gob_data_write)

					if err != nil {
						panic(err)
					}
				}
				fmt.Println("Successfully aborted\n")
			} // Add other commands here
		}()
	}
}

func doCommit(client string, aborting bool) bool {

	// return true

	if !aborting {
		// write each value into the server
		// for server, _ := range objects[client] {
		// 	for object, value := range objects[client][server] {
		// 		// tell server to update object to value
		// 		//conn := serverConns[server]
		// 		enc := encoders[server]

		// 		getVals := ServerVal{Server: server, Object: object, Value: value}
		// 		gob_data_write := GobTransport{Type: "COMMIT_UPDATE", Data: interface{}(getVals)}

		// 		// Send to server
		// 		err := enc.Encode(gob_data_write)

		// 		if err != nil {
		// 			panic(err)
		// 		}
		// 	}
		// }

		objects.Range(func(key interface{}, value interface{}) bool {

			clientName := strings.Split(key.(string), ":")[0]

			if clientName == client {

				server := strings.Split(key.(string), ":")[1]
				object := strings.Split(key.(string), ":")[2]

				// tell server to update objects
				enc := encoders[server]

				getVals := ServerVal{Server: server, Object: object, Value: value.(string)}
				gob_data_write := GobTransport{Type: "COMMIT_UPDATE", Data: interface{}(getVals)}

				err := enc.Encode(gob_data_write)
				if err != nil {
					panic(err)
				}
			}
			return true
		})

	}

	// clear has, wants
	updateLockStatus.Lock()
	has[client] = make(map[string]string)
	wants[client] = make(map[string]string)
	updateLockStatus.Unlock()

	numReaderLock.Lock()

	for x := range numberOfReaders {
		mySlice := []string{}
		for _, clientReading := range numberOfReaders[x] {
			if clientReading != client {
				mySlice = append(mySlice, clientReading)
			}
		}
		numberOfReaders[x] = mySlice
	}
	numReaderLock.Unlock()

	// unlock the mutexes
	acquired.Range(func(key interface{}, value interface{}) bool {

		clientName := strings.Split(key.(string), ":")[0]

		if clientName == client {
			server := strings.Split(key.(string), ":")[1]
			object := strings.Split(key.(string), ":")[2]

			objLock, _ := locks.Load(server + ":" + object)

			if value.(string) == "read" {

				// objLock, _ := locks.Load(server + ":" + object)
				objLock.(*sync.RWMutex).RUnlock()

			} else {
				objLock.(*sync.RWMutex).Unlock()
			}
		}
		return true
	})

	// delete all the locks and objects

	objects.Range(func(key interface{}, value interface{}) bool {

		clientName := strings.Split(key.(string), ":")[0]

		if clientName == client {

			server := strings.Split(key.(string), ":")[1]
			object := strings.Split(key.(string), ":")[2]

			_, ok := objects.Load(client + ":" + server + ":" + object)

			if ok {
				fmt.Println("Load was succesful in COMMIT\n")
			}
			objects.Delete(client + ":" + server + ":" + object)
		}
		return true
	})

	acquired.Range(func(key interface{}, value interface{}) bool {

		clientName := strings.Split(key.(string), ":")[0]

		if clientName == client {

			server := strings.Split(key.(string), ":")[1]
			object := strings.Split(key.(string), ":")[2]

			acquired.Delete(client + ":" + server + ":" + object)
		}
		return true
	})

	return true

}

func forwardGet(client, server, object string, abortSig chan bool) string {

	// ok := true

	fmt.Println("Client:", client, "\n")
	fmt.Println("Server: ", server, "\n")
	fmt.Println("Object: ", object, "\n")

	// load connection instance decoders and encoders
	dec := decoders[server]
	enc := encoders[server]

	// initialisation only
	if _, ok := locks.Load(server + ":" + object); !ok {
		locks.Store(server+":"+object, &sync.RWMutex{})
	}

	// ***********************

	updateLockStatus.Lock()
	// Read lock wanted

	remoteobj := server + ":" + object

	haveLock := false
	// Check if you already have the read or write lock
	for key, _ := range has[client] {

		if key == remoteobj {
			haveLock = true
			break
		}
	}

	// If you don't already have the read or write lock...
	if !haveLock {
		wants[client][remoteobj] = "READ"
	}
	updateLockStatus.Unlock()

	go detectDeadlocks()
	//************************

	// If lock is not yet acquired
	if _, ok := acquired.Load(client + ":" + server + ":" + object); !ok {

		objLock, _ := locks.Load(server + ":" + object)

		go func() {

			objLock.(*sync.RWMutex).RLock()
			acquiredLock <- true
		}()

		fmt.Println("Entering SELECT block\n")
		fmt.Println("client: ", client, "\n")
		select {
		//fmt.Println("Entered SELECT block\n")

		case <-acquiredLock:
			fmt.Println("Acquired lock\n")

			numReaderLock.Lock()
			currReaders := numberOfReaders[server+":"+object]

			alreadyHoldingReadLock := false
			for _, x := range currReaders {
				if x == client {
					alreadyHoldingReadLock = true
				}
			}

			if !alreadyHoldingReadLock {
				currReaders = append(currReaders, client)
			}

			numberOfReaders[server+":"+object] = currReaders

			presentReaderNum := len(currReaders)

			fmt.Println("Number of readers: ", presentReaderNum, "\n")

			numReaderLock.Unlock()

			updateLockStatus.Lock()
			// Move read lock wanted to read lock acquired

			// remoteobj := server + ":" object

			// Delete object from WANTS
			if val, ok := wants[client][remoteobj]; ok && val == "READ" {
				delete(wants[client], remoteobj)
			}

			// Add object to HAS
			if _, ok := has[client][remoteobj]; !ok {
				has[client][remoteobj] = "READ"
			}

			updateLockStatus.Unlock()

			fmt.Println("Acquired lock successfully\n")
		case <-abortSig:
			fmt.Println("Received abort signal\n")
			// Going to abort

			return "pass"

		case <-deadlockSig[client]:
			fmt.Println("Received abort from deadlock detection\n")
			// Going to abort

			select {
			case <-abortSig:
				//continue
				fmt.Println("Clearing sig\n")
			default:
				fmt.Println("Clearing sig default\n")
				//continue
			}

			return "nil"

		}

		acquired.Store(client+":"+server+":"+object, "read")
	}

	if val, ok := objects.Load(client + ":" + server + ":" + object); ok {
		fmt.Println("Entered this line", val)
		return val.(string)
	}

	getVals := ServerVal{Server: server, Object: object, Value: "nil"}
	// Write to server
	gob_data_write := GobTransport{Type: "GET_QUERY", Data: interface{}(getVals)}
	err := enc.Encode(gob_data_write)

	// Check if write is successful
	if err != nil {
		panic(err)
	}

	// Read for value
	var gob_data GobTransport
	var cmd string
	var data ServerVal
	for {

		err := dec.Decode(&gob_data)

		if err == nil {
			cmd = gob_data.Type
			if cmd == "GET_RESPONSE" {
				data = gob_data.Data.(ServerVal)
			}
			break
		}
		fmt.Println(err)
	}

	if cmd == "GET_ABORTED" {
		fmt.Println("Get aborted\n")
		return "nil"

	} else if cmd == "GET_RESPONSE" {
		fmt.Println("Received Get Val from server")
		return data.Value
		// Shouldn't ever get here
	} else {
		return "nil"
	}

	//return gob_data.Data.(ServerVal).Value, ok
}

func forwardSet(client, server, object, val string, abortSig chan bool) string {

	// ok := true

	// load connection instance decoders and encoders
	// dec := decoders[server]
	// enc := encoders[server]

	fmt.Println("Client:", client, "\n")
	fmt.Println("Server: ", server, "\n")
	fmt.Println("Object: ", object, "\n")
	fmt.Println("Value: ", val, "\n")

	// initialisation only
	if _, ok := locks.Load(server + ":" + object); !ok {
		locks.Store(server+":"+object, &sync.RWMutex{})
	}

	// ***********************

	updateLockStatus.Lock()
	// Write lock wanted

	remoteobj := server + ":" + object

	haveLock := false
	// Check if you already have the write lock
	for key, val := range has[client] {

		if key == remoteobj && val == "WRITE" {
			fmt.Println("Client already holds write lock\n")
			haveLock = true
			break
		}
	}

	// If you don't already have the write lock...
	if !haveLock {
		fmt.Println("Added write lock to WANTS list...\n")
		wants[client][remoteobj] = "WRITE"
	}
	updateLockStatus.Unlock()

	go detectDeadlocks()
	//************************

	status, ok := acquired.Load(client + ":" + server + ":" + object)

	// If you don't have the read or write lock...
	if !ok {

		objLock, _ := locks.Load(server + ":" + object)

		go func() {
			objLock.(*sync.RWMutex).Lock()
			acquiredLock <- true
		}()
		fmt.Println("Selecting in forwardSet()\n")
		fmt.Println("client: ", client)
		select {

		case <-acquiredLock:

			updateLockStatus.Lock()
			fmt.Println("Re-acquired lock to update HAS list\n")
			// Move write lock wanted to write lock acquired

			// remoteobj := server + ":" object

			// Delete object from WANTS
			if _, ok := wants[client][remoteobj]; ok {
				delete(wants[client], remoteobj)
			}

			// Add object to HAS
			has[client][remoteobj] = "WRITE"
			fmt.Println("Added lock to HAS list\n")

			updateLockStatus.Unlock()

			fmt.Println("Acquired lock successfully\n")

		case <-abortSig:
			fmt.Println("Received abort signal\n")
			// Going to abort
			return "pass"

		case <-deadlockSig[client]:
			fmt.Println("Received abort from deadlock detection\n")

			select {
			case <-abortSig:
				//continue
				fmt.Println("Clearing sig\n")
			default:
				fmt.Println("Clearing sig default\n")
				//continue
			}
			// Going to abort
			return "failed"

		}
		//objLock.(*sync.RWMutex).Lock()

		// locks[server][object].Lock()
	} else if status == "read" { // currently read: need to upgrade lock

		objLock, _ := locks.Load(server + ":" + object)

		// updateLockStatus.Lock()
		// // Update WANTS here (overwrites if necessary)
		// wants[client][server+":"+object] = "WRITE"
		// updateLockStatus.Unlock()

		// go detectDeadlocks()

		// **************** Lock promotion
		go func() {
			//objLock.(*sync.RWMutex).Lock()
			for {
				numReaderCheck := false
				numReaderLock.Lock()

				currReaders := numberOfReaders[server+":"+object]
				if len(currReaders) == 1 {
					numReaderCheck = true
				}
				numReaderLock.Unlock()

				if numReaderCheck {
					break
				}
				// sleep for 250 ms
				time.Sleep(250 * time.Millisecond)
			}
			acquiredLock <- true
		}()

		select {

		case <-acquiredLock:

			objLock.(*sync.RWMutex).RUnlock()

			objLock.(*sync.RWMutex).Lock()

			updateLockStatus.Lock()
			fmt.Println("Re-acquired lock to update HAS list\n")
			// Move write lock wanted to write lock acquired

			// remoteobj := server + ":" object

			// Delete object from WANTS
			if _, ok := wants[client][remoteobj]; ok {
				delete(wants[client], remoteobj)
			}

			// Add object to HAS
			has[client][remoteobj] = "WRITE"
			fmt.Println("Added lock to HAS list\n")

			updateLockStatus.Unlock()

			fmt.Println("Acquired lock successfully\n")

		case <-abortSig:
			fmt.Println("Received abort signal\n")
			// Going to abort
			return "pass"

		case <-deadlockSig[client]:
			fmt.Println("Received abort from deadlock detection\n")

			select {
			case <-abortSig:
				//continue
				fmt.Println("Clearing sig\n")
			default:
				fmt.Println("Clearing sig default\n")
				//continue
			}
			// Going to abort
			return "failed"

		}
	}

	acquired.Store(client+":"+server+":"+object, "write")
	objects.Store(client+":"+server+":"+object, val)
	fmt.Println("Got here\n")
	return "success"
}

func detectDeadlocks() {

	updateLockStatus.Lock()

	for x := range has {
		for obj := range has[x] {
			fmt.Println("HAS object:", obj, "\n")
		}
	}
	for x := range wants {
		for obj := range wants[x] {
			fmt.Println("WANTS object:", obj, "\n")
		}
	}

	visited = make(map[string]bool)

	// Initialization
	adj := make(map[string]map[string]bool)
	for x := range has {
		adj[x] = make(map[string]bool)
		for y := range has {
			if x != y {
				for obj := range wants[x] {

					if _, ok := has[y][obj]; ok {
						// Extra logic
						if wants[x][obj] == "READ" {
							if has[y][obj] == "WRITE" {
								adj[x][y] = true
							}
						} else {
							adj[x][y] = true
						}
						adj[x][y] = true
						break
					}
				}
			}
		}
	}

	// Run DFS for cycle detection
	for node := range adj {
		if _, ok := visited[node]; !ok {
			visitedStack = make(map[string]bool)
			if DFS(node, adj, visitedStack) {
				break
			}
		}
	}

	updateLockStatus.Unlock()

}

func DFS(currNode string, adj map[string]map[string]bool, visitedStack map[string]bool) bool {

	// Return if already visited
	if _, ok := visitedStack[currNode]; ok {
		// deadlockSig[currNode] <- true
		return true
	}

	visited[currNode] = true
	visitedStack[currNode] = true

	for neighbour := range adj[currNode] {

		if DFS(neighbour, adj, visitedStack) {
			fmt.Println("Sending deadlock signal\n")
			fmt.Println("currnode: ", currNode, "\n")
			deadlockSig[currNode] <- true
			fmt.Println("Sent deadlock signal\n")
			return true
		}

	}

	return false

}
