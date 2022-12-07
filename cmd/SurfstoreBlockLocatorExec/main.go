package main

import (
	"crypto/sha256"
	"cse224/proj4/pkg/surfstore"
	"encoding/hex"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
)

func main() {

	downServers := flag.String("downServers", "", "Comma-separated list of server IDs that have failed")
	flag.Parse()

	if flag.NArg() != 3 {
		fmt.Printf("Usage: %s numServers blockSize inpFilename\n", os.Args[0])
		flag.PrintDefaults()
		os.Exit(1)
	}

	numServers, err := strconv.Atoi(flag.Arg(0))
	if err != nil {
		log.Fatal("Invalid number of servers argument: ", flag.Arg(0))
	}

	blockSize, err := strconv.Atoi(flag.Arg(1))
	if err != nil {
		log.Fatal("Invalid block size argument: ", flag.Arg(0))
	}

	inpFilename := flag.Arg(2)

	log.Println("Total number of blockStore servers: ", numServers)
	log.Println("Block size: ", blockSize)
	log.Println("Processing input data filename: ", inpFilename)

	if *downServers != "" {
		for _, downServer := range strings.Split(*downServers, ",") {
			log.Println("Server ", downServer, " is in a failed state")
		}
	} else {
		log.Println("No servers are in a failed state")
	}

	// This is an example of the format of the output
	// Your program will emit pairs for each block has where the
	// first part of the pair is the block hash, and the second
	// element is the server number that the block resides on
	//
	// This output is simply to show the format, the actual mapping
	// isn't based on consistent hashing necessarily

	// changing the downserver to int array
	downServersList := make([]int, 0)
	if *downServers != "" {
		for _, downServer := range strings.Split(*downServers, ",") {
			downServerInt, err := strconv.Atoi(downServer)
			if err != nil {
				log.Fatalf("Error: %s", err)
			}
			downServersList = append(downServersList, downServerInt)
		}
	}

	hashRing := surfstore.NewConsistentHashRing(numServers, downServersList)

	fileHash := getHashList(inpFilename, blockSize)

	servers := hashRing.OutputMap(fileHash)

	for i, hash := range fileHash {
		server := servers["block"+strconv.Itoa(i)][10:]
		if i == len(fileHash)-1 {

			fmt.Printf("{%s,%s}", hash, server)
		} else {
			fmt.Printf("{%s,%s},", hash, server)
		}
	}
	fmt.Printf("}")

}

func getHashList(filename string, blockSize int) []string {
	if blockSize <= 0 {
		return nil
	}
	file, err := os.ReadFile(filename)
	if err != nil {
		log.Fatalf("Error %s ", err)
	}
	hashList := make([]string, 0)
	for i := 0; i < len(file); i += blockSize {
		if i+blockSize > len(file) {
			hashList = append(hashList, hash(file[i:]))
		} else {
			hashList = append(hashList, hash(file[i:i+blockSize]))
		}
	}
	return hashList
}

func hash(b []byte) string {
	h := sha256.New()
	h.Write(b)
	return hex.EncodeToString(h.Sum(nil))
}
