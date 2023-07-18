package surfstore

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"path/filepath"

	_ "github.com/mattn/go-sqlite3"
)

/* Hash Related */
func GetBlockHashBytes(blockData []byte) []byte {
	h := sha256.New()
	h.Write(blockData)
	return h.Sum(nil)
}

func GetBlockHashString(blockData []byte) string {
	blockHash := GetBlockHashBytes(blockData)
	return hex.EncodeToString(blockHash)
}

/* File Path Related */
func ConcatPath(baseDir, fileDir string) string {
	return baseDir + "/" + fileDir
}

/*
	Writing Local Metadata File Related
*/

const createTable string = `create table if not exists indexes (
		fileName TEXT, 
		version INT,
		hashIndex INT,
		hashValue TEXT
	);`

const insertTuple string = `insert into indexes (fileName, version, hashIndex, hashValue) VALUES (?,?,?,?);`

// WriteMetaFile writes the file meta map back to local metadata file index.db
func WriteMetaFile(fileMetas map[string]*FileMetaData, baseDir string) error {
	// remove index.db file if it exists
	outputMetaPath := ConcatPath(baseDir, DEFAULT_META_FILENAME)
	if _, err := os.Stat(outputMetaPath); err == nil {
		e := os.Remove(outputMetaPath)
		if e != nil {
			log.Fatal("Error During Meta Write Back")
		}
	}
	db, err := sql.Open("sqlite3", outputMetaPath)
	if err != nil {
		log.Fatal("Error During Meta Write Back")
	}
	statement, err := db.Prepare(createTable)
	statement.Exec()
	if err != nil {
		log.Fatal("Error During Meta Write Back")
	}
	//panic("todo")

	statement, err = db.Prepare(insertTuple)
	for _, filemeta := range fileMetas {
		for hashIdx, hashValue := range filemeta.BlockHashList {
			statement.Exec(filemeta.Filename, filemeta.Version, hashIdx, hashValue)
		}
	}
	if err != nil {
		log.Println("Insert Tuple Error: ", err.Error())
	}

	return statement.Close()
}

/*
Reading Local Metadata File Related
*/
const getDistinctFileName string = `SELECT fileName, max(version) FROM indexes GROUP BY fileName;`

const getTuplesByFileName string = `SELECT hashValue FROM indexes
									WHERE fileName = ?;`

// LoadMetaFromMetaFile loads the local metadata file into a file meta map.
// The key is the file's name and the value is the file's metadata.
// You can use this function to load the index.db file in this project.
func LoadMetaFromMetaFile(baseDir string) (fileMetaMap map[string]*FileMetaData, e error) {
	metaFilePath, _ := filepath.Abs(ConcatPath(baseDir, DEFAULT_META_FILENAME))
	fileMetaMap = make(map[string]*FileMetaData)
	metaFileStats, e := os.Stat(metaFilePath)
	if e != nil || metaFileStats.IsDir() {
		return fileMetaMap, fmt.Errorf("error opening the file")
	}
	db, err := sql.Open("sqlite3", metaFilePath)
	if err != nil {
		log.Fatal("Error When Opening Meta")
	}
	//panic("todo")
	statement, err := db.Prepare(createTable)
	statement.Exec()
	if err != nil {
		log.Fatal("Error During Meta Write Back")
	}
	
	fileNames, err := db.Query(getDistinctFileName)
	if err != nil {
		log.Println(err.Error())
	}

	for fileNames.Next() {
		var fileName string
		var version int32
		fileNames.Scan(&fileName, &version)
		//log.Printf("filename: %s; version: %d\n", fileName, version)
		hashValues, err := db.Query(getTuplesByFileName, fileName)
		if err != nil {
			log.Println(err.Error())
		}
		var blockHashList []string
		for hashValues.Next() {
			var blockHash string
			hashValues.Scan(&blockHash)
			blockHashList = append(blockHashList, blockHash)
		}
		fileMetaMap[fileName] = &FileMetaData{Filename: fileName, Version: version, BlockHashList: blockHashList}
	}
	return fileMetaMap, nil

}

/*
	Debugging Related
*/

// PrintMetaMap prints the contents of the metadata map.
// You might find this function useful for debugging.
func PrintMetaMap(metaMap map[string]*FileMetaData) {

	fmt.Println("--------BEGIN PRINT MAP--------")

	for _, filemeta := range metaMap {
		fmt.Println("\t", filemeta.Filename, filemeta.Version)
		for _, blockHash := range filemeta.BlockHashList {
			fmt.Println("\t", blockHash)
		}
	}

	fmt.Println("---------END PRINT MAP--------")

}
