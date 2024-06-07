package mr

import (
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strconv"
	"syscall"
)

const TASKS_AVAILABLE_NOTIFICATION = 1
const QUIT_NOTIFICATION = 15

func UnixSocketExample() {
	// This function handles client connections in a separate goroutine.
	// Uses Accept() to block and wait for incoming connections.

	// Create a Unix domain socket and listen for incoming connections.
	socket, err := net.Listen("unix", "/tmp/echo.sock")
	if err != nil {
		log.Fatal(err)
	}

	// Cleanup the sockfile.
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		os.Remove("/tmp/echo.sock")
		os.Exit(1)
	}()

	for {
		// Accept an incoming connection.
		conn, err := socket.Accept()
		if err != nil {
			log.Fatal(err)
		}

		// Handle the connection in a separate goroutine.
		go func(conn net.Conn) {
			defer conn.Close()
			// Create a buffer for incoming data.
			buf := make([]byte, 4096)

			// Read data from the connection.
			n, err := conn.Read(buf)
			if err != nil {
				log.Fatal(err)
			}

			// Echo the data back to the connection.
			_, err = conn.Write(buf[:n])
			if err != nil {
				log.Fatal(err)
			}
		}(conn)
	}
}

// Unique server socket address
func broadcastServerSock() string {
	s := "/var/tmp/5840-broadcast-server-"
	s += strconv.Itoa(os.Getuid())
	return s
}

// Unique client socket address
func broadcastClientSock() string {
	s := "/var/tmp/5840-broadcast-client-"
	s += strconv.Itoa(os.Getuid())
	s += "-"
	s += strconv.Itoa(os.Getpid())

	return s
}

func CreateBroadcastSocket(clientAddresses *[]string) (map[string]chan int, error) {
	// Connection-less Unix domain socket
	// Takes an array of client socket paths and writes broadcasts a message out to all of them

	// Specify the server socket address
	serverPath := broadcastServerSock()

	// Ensure the server socket file does not already exist
	// os.Remove(serverFilename)
	syscall.Unlink(serverPath)

	// Create a unixgram domain socket using the server filename
	serverAddr, err := net.ResolveUnixAddr("unixgram", serverPath)
	if err != nil {
		log.Fatalf("Failed to resolve server address: %v", err)
		return nil, err
	}

	// Create the Unix domain socket connection
	serverConn, err := net.ListenUnixgram("unixgram", serverAddr)
	if err != nil {
		log.Fatalf("Failed to create Unix domain socket: %v", err)
		return nil, err
	}

	// Server control channels
	c := make(chan int)
	quit := make(chan int)

	go func() {
		// Close the server connection when the goroutine exits
		defer func() {
			serverConn.Close()

			// Clean up the client socket file
			os.Remove(serverPath)
		}()

		for {
			select {
			case notification := <-c:
				// Message to be sent to clients
				var msg []byte
				switch notification {
				case TASKS_AVAILABLE_NOTIFICATION:
					fmt.Println("Broadcast: Tasks available")
					msg = []byte("Tasks available")
				case QUIT_NOTIFICATION:
					fmt.Println("Broadcast: Quit")
					msg = []byte("Quit")
				default:
					log.Fatalf("Unknown notification: %v", notification)
				}

				for _, clientAddress := range *clientAddresses {
					// Resolve the client address
					// Clients can drop off and rejoin, so we need to resolve the address each time
					clientUnixAddr, err := net.ResolveUnixAddr("unixgram", clientAddress)
					if err != nil {
						log.Fatalf("Failed to resolve client address: %v", err)
					} else {
						// Send the message to each client over server connection
						_, err = serverConn.WriteToUnix(msg, clientUnixAddr)
						if err != nil {
							log.Fatalf("Failed to write to Unix domain socket: %v", err)
						}
					}
				}
				//log.Printf("Message sent: %s\n", msg)
			case <-quit:
				fmt.Println("CreateBroadcastSocket: Quitting")
				return
			}
		}
	}()

	return map[string]chan int{
		"notification": c,
		"quit":         quit,
	}, nil
}

func CreateBroadcastListener(clientPath string) (map[string]chan int, error) {
	// Ensure the client socket file does not already exist
	//os.Remove(clientFilename)
	syscall.Unlink(clientPath)

	// Create a Unix domain socket
	clientAddr, err := net.ResolveUnixAddr("unixgram", clientPath)
	if err != nil {
		log.Fatalf("Failed to resolve client address: %v", err)
		return nil, err
	}

	// Create the Unix domain socket connection
	conn, err := net.ListenUnixgram("unixgram", clientAddr)
	if err != nil {
		log.Fatalf("Failed to create Unix domain socket: %v", err)
		return nil, err
	}

	// Client control channels
	c := make(chan int)
	quit := make(chan int)

	go func() {

		for {
			// Buffer to read the message from the server
			buf := make([]byte, 1024)
			n, err := conn.Read(buf)
			if err != nil {
				log.Fatalf("Failed to read from Unix domain socket: %v", err)
			}

			msg := string(buf[:n])

			log.Printf("Received %d bytes: %s\n", n, msg)
			switch msg {
			case "Tasks available":
				c <- TASKS_AVAILABLE_NOTIFICATION
			case "Quit":
				quit <- QUIT_NOTIFICATION
				return
			default:
				log.Fatalf("Unknown message: %s", msg)
			}
		}
	}()

	return map[string]chan int{
		"notification": c,
		"quit":         quit,
	}, nil
}

func CleanUpBroadcastSocket(path string) {
	// Clean up the socket file
	fmt.Println("CreateBroadcastListener: Removing client socket")
	os.Remove(path)
}
