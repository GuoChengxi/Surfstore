## proj5 setup
# cp ../proj5/cmd/SurfstoreRaftServerExec/main.go cmd/SurfstoreRaftServerExec/
# cp ../proj5/pkg/surfstore/Raft* pkg/surfstore/
# cp ../proj5/pkg/surfstore/SurfStore.proto pkg/surfstore/

# cp ../proj5/cmd/SurfstoreClientExec/main.go cmd/SurfstoreClientExec/
# cp ../proj5/cmd/SurfstorePrintBlockMapping/main.go cmd/SurfstorePrintBlockMapping/

# regenerate protobuf
# protoc --proto_path=. --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative pkg/surfstore/SurfStore.proto

##
go run cmd/SurfstoreRaftServerExec/main.go -f configfile.txt -i 0 -b localhost:8080
go run cmd/SurfstoreClientExec/main.go -f configfile.txt baseDir blockSize
# or instead:
make run-blockstore
make IDX=0 run-raft

# Test
make test
make TEST_REGEX=SyncTwoClients specific-test