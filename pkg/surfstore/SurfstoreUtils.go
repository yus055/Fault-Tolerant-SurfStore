package surfstore

import (
	"errors"
	"fmt"
	"io/fs"
	"io/ioutil"
	"log"
	"math"
	"os"
	"strings"
)

// Implement the logic for a client syncing with the server here.
func ClientSync(client RPCClient) {
	//indexMap index.txt
	//localIndexMap compare basefiles and index.txt ->update
	//serverMap server index
	//newIndex compare server and localIndexMap -> update
	basefiles, err := ioutil.ReadDir(client.BaseDir)
	if err != nil {
		fmt.Println("Cannot read basedir")
		return
	}

	//record the files in the base dir
	localfiles := make(map[string]fs.FileInfo)
	for _, f := range basefiles {
		if f.Name() == "index.txt" {
			continue
		}
		localfiles[f.Name()] = f
	}

	if _, err_create := os.Stat(client.BaseDir + "/index.txt"); errors.Is(err_create, os.ErrNotExist) {
		os.Create(client.BaseDir + "/index.txt")
	}
	//read index.txt
	indexMap, err := LoadMetaFromMetaFile(client.BaseDir)
	if err != nil {
		fmt.Errorf("cannot read local index.txt")
	}

	//update local index.txt
	localIndexMap, filestatus := updatelocalindex(client, indexMap, localfiles)

	//server metadata
	serverMap := make(map[string]*FileMetaData)
	err1 := client.GetFileInfoMap(&serverMap)
	if err1 != nil && strings.Contains(err.Error(), ERR_MAJORITY_CRASHED.Error()) {
		os.Exit(1)
		fmt.Errorf("cannot get server file into map ")
	}
	if err1 != nil {
		fmt.Printf("cannot find server files")
		fmt.Printf(err1.Error())
	}
	//upload
	newIndex := make(map[string]*FileMetaData)
	for filename := range localIndexMap {
		if _, ok := serverMap[filename]; ok {
			//both in base and server
			serverMeta := serverMap[filename]
			clientMeta := localIndexMap[filename]
			if serverMeta.Version == clientMeta.Version && filestatus[filename] == "unchange" {
				newIndex[filename] = serverMeta
				continue
			}
			if serverMeta.Version >= clientMeta.Version && filestatus[filename] == "change" {
				changenotvalid(serverMeta, client, clientMeta, newIndex)
			} else if serverMeta.Version > clientMeta.Version && filestatus[filename] == "unchange" {
				changenotvalid(serverMeta, client, clientMeta, newIndex)
			} else if serverMeta.Version+1 == clientMeta.Version {
				//update
				uploadnewfile(clientMeta, client, serverMap, newIndex)
			}
		} else {
			uploadnewfile(localIndexMap[filename], client, serverMap, newIndex)
		}
	}

	for filename := range serverMap {
		if _, ok := localIndexMap[filename]; !ok {
			downloadtoclient(serverMap[filename], client, newIndex)
		}
	}

	errw := WriteMetaFile(newIndex, client.BaseDir)
	if errw != nil {
		fmt.Errorf("write index.txt failed")
	}

	PrintMetaMap(newIndex)

}

func gethashlist(path string, f fs.FileInfo, blocksize int) []string {
	if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
		return []string{"0"}
	}
	data, err := ioutil.ReadFile(path)
	if err != nil {
		log.Fatal(err)
	}
	size := len(data)
	if size == 0 {
		return []string{}
	}
	nums := int(math.Ceil(float64(size) / float64(blocksize)))

	var newHashList []string
	for i := 0; i < nums; i++ {
		buf := make([]byte, blocksize)
		length := 0
		if nums == i+1 {
			buf = data[i*blocksize : size]
			length = size - i*blocksize
		} else {
			buf = data[i*blocksize : (i+1)*blocksize]
			length = blocksize
		}
		s := GetBlockHashString(buf[:length])
		newHashList = append(newHashList, s)
	}
	return newHashList
}

func is_changed(path string, f fs.FileInfo, blockhashlist []string, blocksize int) (bool, []string) {

	newhashlists := gethashlist(path, f, blocksize)
	if len(newhashlists) != len(blockhashlist) {
		return true, newhashlists
	} else {
		for i := 0; i < len(blockhashlist); i++ {
			if blockhashlist[i] != newhashlists[i] {
				return true, newhashlists
			}
		}
	}
	return false, nil

}

func updatelocalindex(client RPCClient, indexMap map[string]*FileMetaData, localfiles map[string]fs.FileInfo) (map[string]*FileMetaData, map[string]string) {

	newIndexIntoMap := make(map[string]*FileMetaData)
	filestatus := make(map[string]string)
	for _, f := range localfiles {
		name := f.Name()
		path := ConcatPath(client.BaseDir, name)
		//old file
		if _, ok := indexMap[name]; ok {
			//change or not
			change, newhashlist := is_changed(path, f, indexMap[name].BlockHashList, client.BlockSize)
			//if change vaersion+1/ hashlist
			if change {
				filematadata := new(FileMetaData)
				filematadata.Filename = name
				filematadata.Version = indexMap[name].Version + 1
				filematadata.BlockHashList = newhashlist
				newIndexIntoMap[name] = filematadata
				filestatus[name] = "change"
			} else {
				newIndexIntoMap[name] = indexMap[name]
				filestatus[name] = "unchange"
			}

		} else {
			// new file
			filematadata := new(FileMetaData)
			filematadata.Filename = name
			filematadata.Version = 1
			filematadata.BlockHashList = gethashlist(path, f, client.BlockSize)
			newIndexIntoMap[name] = filematadata
			filestatus[name] = "change"
		}
	}
	//delete files
	for filename, metadata := range indexMap {
		if _, ok := localfiles[filename]; !ok {
			if len(metadata.BlockHashList) == 1 && metadata.BlockHashList[0] == "0" {
				newIndexIntoMap[filename] = indexMap[filename]
				filestatus[filename] = "unchange"
			} else {
				filematadata := new(FileMetaData)
				filematadata.Filename = filename
				filematadata.Version = indexMap[filename].Version + 1
				filematadata.BlockHashList = []string{"0"}
				newIndexIntoMap[filename] = filematadata
				filestatus[filename] = "change"
			}
		}
	}
	return newIndexIntoMap, filestatus
}

func download(filemetadata *FileMetaData, client RPCClient) error {
	name := filemetadata.Filename
	BlockHashList := filemetadata.BlockHashList
	if len(BlockHashList) == 1 && BlockHashList[0] == "0" {
		return nil
	}
	path := ConcatPath(client.BaseDir, name)
	f, e := os.Create(path)
	if e != nil {
		return e
	}
	defer f.Close()

	for _, hash := range BlockHashList {
		block := new(Block)
		blockStoreAddr := ""
		err_ad := client.GetBlockStoreAddr(&blockStoreAddr)
		if err_ad != nil {
			fmt.Errorf("cannot get block store address")
		}
		err := client.GetBlock(hash, blockStoreAddr, block)
		if err != nil {
			fmt.Errorf("cannot get this block when download to client")
			return err
		}
		_, err1 := f.Write(block.BlockData)
		if err1 != nil {
			fmt.Errorf("cannot write this block when download to client")
			return err1
		}
	}
	return nil
}

func downloadtoclient(filemetadata *FileMetaData, client RPCClient, newindex map[string]*FileMetaData) {
	err := download(filemetadata, client)
	if err != nil {
		fmt.Errorf("download failed")
		return
	}
	newmeta := new(FileMetaData)
	newmeta.Filename = filemetadata.Filename
	newmeta.Version = filemetadata.Version
	newmeta.BlockHashList = make([]string, len(filemetadata.BlockHashList))
	copy(newmeta.BlockHashList, filemetadata.BlockHashList)
	newindex[filemetadata.Filename] = newmeta
	return
}

func changenotvalid(servermeta *FileMetaData, client RPCClient, clientmeta *FileMetaData, newindex map[string]*FileMetaData) {
	name := clientmeta.Filename
	path := ConcatPath(client.BaseDir, name)
	//delete file
	if _, err := os.Stat(path); err == nil {
		if errr := os.Remove(path); errr != nil {
			fmt.Println("file remove Error!\n")
		}
	}

	err1 := download(servermeta, client)
	if err1 != nil {
		fmt.Println("download file Error!\n")
		return
	}
	newindex[name] = new(FileMetaData)
	newindex[name].Filename = name
	newindex[name].Version = servermeta.Version
	newindex[name].BlockHashList = make([]string, len(servermeta.BlockHashList))
	copy(newindex[name].BlockHashList, servermeta.BlockHashList)
	return
}

func uploadnewfile(metadata *FileMetaData, client RPCClient, serverFileInfoMap map[string]*FileMetaData, newindex map[string]*FileMetaData) {
	name := metadata.Filename
	path := ConcatPath(client.BaseDir, name)
	//if the change is "delete" in local
	if len(metadata.BlockHashList) == 1 && metadata.BlockHashList[0] == "0" {
		//update meta
		var ver int32

		err2 := client.UpdateFile(metadata, &ver)
		if err2 != nil {
			new_serverFileInfoMap := make(map[string]*FileMetaData)
			client.GetFileInfoMap(&new_serverFileInfoMap)
			changenotvalid(new_serverFileInfoMap[metadata.Filename], client, metadata, newindex)
			return
		}

		//update index if succ upload

		newmetadata := new(FileMetaData)
		newmetadata.Filename = name
		newmetadata.Version = metadata.Version
		newmetadata.BlockHashList = []string{"0"}
		newindex[name] = newmetadata
		return
	}
	// change file or write to a deleted file
	data, err := ioutil.ReadFile(path)
	if err != nil {
		log.Fatal(err)
	}
	b_size := client.BlockSize
	size := len(data)
	nums := int(math.Ceil(float64(size) / float64(b_size)))
	blockStoreAddr := ""
	err_ad := client.GetBlockStoreAddr(&blockStoreAddr)
	if err_ad != nil {
		print("getadd failed")
	}
	// putblock
	for i := 0; i < nums; i++ {
		buf := []byte{}
		if nums == i+1 {
			buf = data[i*b_size : size]
		} else {
			buf = data[i*b_size : (i+1)*b_size]
		}
		var succ bool
		block := new(Block)
		block.BlockSize = int32(len(buf))
		block.BlockData = buf
		err1 := client.PutBlock(block, blockStoreAddr, &succ)
		if err1 != nil {
			fmt.Println("PutBlock failed")

		}
		s := GetBlockHashString(buf)
		blockget := new(Block)
		errget := client.GetBlock(s, blockStoreAddr, blockget)
		if errget != nil {
			print("can not get block")
		} else {
		}
	}

	//update meta
	var ver int32
	err2 := client.UpdateFile(metadata, &ver)
	if err2 != nil {
		new_serverFileInfoMap := make(map[string]*FileMetaData)
		client.GetFileInfoMap(&new_serverFileInfoMap)
		changenotvalid(serverFileInfoMap[metadata.Filename], client, metadata, newindex)
		return
	}

	//update index if succ upload
	newmetadata := new(FileMetaData)
	newmetadata.Filename = name
	newmetadata.Version = metadata.Version
	newmetadata.BlockHashList = make([]string, len(metadata.BlockHashList))

	copy(newmetadata.BlockHashList, metadata.BlockHashList)
	newindex[name] = newmetadata
	return
}
