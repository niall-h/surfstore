package surfstore

import (
	"io"
	"io/fs"
	"io/ioutil"
	"log"
	"os"
)

func ClientSync(client RPCClient) {
	// create variables
	// fileMap: stores hashlist of all the files in the directory (key: filename, value: hashlist)
	fileMap := make(map[string][]string)
	// boolean value to check if index file exists
	indexFileExists := false

	// scan the base directory and compute each file's hash list
	log.Print("Scanning base directory")
	files, err := ioutil.ReadDir(client.BaseDir)
	checkError(err)
	for _, file := range files {
		if !file.IsDir() {
			if file.Name() == "index.txt" {
				indexFileExists = true
				continue
			}
			hashList := computeHashList(client, file)
			fileMap[file.Name()] = hashList
		}
	}
	PrintLocalMap(fileMap)

	// check if index.txt file exists
	// create one if not found
	if !indexFileExists {
		log.Print("Creating index.txt")
		index, err := os.Create(ConcatPath(client.BaseDir, "index.txt"))
		checkError(err)
		defer index.Close()
	}
	log.Print("Loading from index.txt")
	localFileInfoMap, err := LoadMetaFromMetaFile(client.BaseDir)
	checkError(err)
	PrintMetaMap(localFileInfoMap)

	// compare hashlist for each file
	// create modified_list
	log.Print("Populating modified_list")
	modified_list := make(map[string]*FileMetaData, 0)
	for filename, hashList := range fileMap {
		newMetaData := &FileMetaData{Filename: filename, Version: 1, BlockHashList: hashList}
		if metadata, found := localFileInfoMap[filename]; found {
			if !equal(hashList, metadata.BlockHashList) {
				newMetaData.Version = metadata.Version + 1
			} else {
				continue
			}
		}
		modified_list[filename] = newMetaData
	}

	for filename, metadata := range localFileInfoMap {
		if _, found := fileMap[filename]; !found {
			newMetaData := &FileMetaData{Filename: filename, Version: metadata.Version + 1, BlockHashList: []string{"0"}}
			modified_list[filename] = newMetaData
		}
	}
	// printing meta map
	PrintMetaMap(modified_list)

	// do client.GetFileInfoMap to get remote index
	log.Print("Getting remote index")
	serverFileInfoMap := make(map[string]*FileMetaData)
	err = client.GetFileInfoMap(&serverFileInfoMap)
	checkError(err)
	PrintMetaMap(serverFileInfoMap)

	// get block store address
	var blockStoreAddr string
	err = client.GetBlockStoreAddr(&blockStoreAddr)
	checkError(err)
	log.Printf("blockStoreAddr: %s", blockStoreAddr)

	// compare remote index with local index
	// if new file in remote index, download the new file + blocks
	log.Print("Comparing remote with local index")
	for filename, metadata := range serverFileInfoMap {
		hashList, found := localFileInfoMap[filename]
		if equal(metadata.BlockHashList, []string{"0"}) {
			modified_list[filename] = metadata
		}
		if !found || !equal(hashList.BlockHashList, metadata.BlockHashList) {
			// check to see if it's in upload or delete list, remove if found
			if _, found := modified_list[filename]; found {
				delete(modified_list, filename)
			}
			// download blocks and overwrite the file
			log.Printf("Downloading blocks for %s", filename)
			downloadBlocks(client, filename, metadata, blockStoreAddr)
			localFileInfoMap[filename] = metadata
		}
	}
	PrintMetaMap(modified_list)

	// if new file in local index, upload the new file + blocks
	for filename, metadata := range modified_list {
		log.Printf("Files uploading: %s", filename)
		fileIsDeleted := false
		success := false
		if equal(metadata.BlockHashList, []string{"0"}) {
			log.Printf("%s deleted", filename)
			fileIsDeleted = true
			success = true
		} else {
			f, err := os.Open(ConcatPath(client.BaseDir, filename))
			checkError(err)

			log.Printf("%s being uploaded block by block", filename)
			for {
				buf := make([]byte, client.BlockSize)
				n, err := f.Read(buf)
				if err != nil {
					if err == io.EOF {
						break
					} else {
						checkError(err)
					}
				}
				newData := buf[0:n]
				log.Printf("Uploaded Block Size: %d", len(newData))
				block := &Block{BlockData: newData, BlockSize: int32(client.BlockSize)}
				log.Printf("Block upload to block store address: %s", blockStoreAddr)
				err = client.PutBlock(block, blockStoreAddr, &success)
				checkError(err)
				if !success {
					break
				}
			}
		}
		// update server with new FileInfo
		log.Printf("success: %v", success)
		if success {
			var latestVersion int32 = 0
			err = client.UpdateFile(metadata, &latestVersion)
			err = client.GetFileInfoMap(&serverFileInfoMap)
			checkError(err)
			PrintMetaMap(serverFileInfoMap)
			if latestVersion != -1 {
				log.Printf("latestVersion != -1")
				localFileInfoMap[filename] = metadata
				if fileIsDeleted {
					log.Printf("%s is deleted", filename)
					localFileInfoMap[filename].BlockHashList = nil
				}
			} else {
				log.Printf("latestVersion == -1")
				err = client.GetFileInfoMap(&serverFileInfoMap)
				checkError(err)
				downloadBlocks(client, filename, serverFileInfoMap[filename], blockStoreAddr)
			}
		}
	}

	// Writing to local index
	log.Printf("Writing to local index")
	PrintMetaMap(localFileInfoMap)
	for filename, metadata := range localFileInfoMap {
		if equal(metadata.BlockHashList, []string{"0"}) {
			localFileInfoMap[filename].BlockHashList = nil
		}
	}
	err = WriteMetaFile(localFileInfoMap, client.BaseDir)
	checkError(err)
}

func computeHashList(client RPCClient, file fs.FileInfo) []string {
	// hashList stores the hashes of each block in the file
	// buf is the buffer for reading file, its size is specified in RPCClient
	hashList := make([]string, 0)

	// open file
	f, err := os.Open(ConcatPath(client.BaseDir, file.Name()))
	checkError(err)
	defer f.Close()

	// read in
	for {
		buf := make([]byte, client.BlockSize)
		n, err := f.Read(buf)
		if err == io.EOF {
			break
		}
		checkError(err)

		newData := buf[0:n]
		hash := GetBlockHashString(newData)
		hashList = append(hashList, hash)
	}
	return hashList
}

func downloadBlocks(client RPCClient, filename string, metadata *FileMetaData, blockStoreAddr string) {
	// if server has deleted the file
	if equal(metadata.BlockHashList, []string{"0"}) {
		os.Remove(ConcatPath(client.BaseDir, filename))
	} else {
		f, err := os.Create(ConcatPath(client.BaseDir, filename))
		checkError(err)
		defer f.Close()

		block := &Block{}
		for _, hash := range metadata.BlockHashList {
			err = client.GetBlock(hash, blockStoreAddr, block)
			log.Printf("Downloaded Block: %s", block)
			checkError(err)
			_, err = f.Write(block.BlockData)
			checkError(err)
		}
	}
}

func equal(a []string, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i, v := range a {
		if v != b[i] {
			return false
		}
	}
	return true
}

func checkError(err error) {
	if err != nil {
		log.Fatalf("Error: %s", err)
	}
}

func PrintLocalMap(fileMap map[string][]string) {
	log.Print("-------------------BEGINN-------------------")
	for filename, hashList := range fileMap {
		log.Printf(filename)
		for _, hash := range hashList {
			log.Printf(hash)
		}
	}
	log.Print("-------------------END-------------------")
}
