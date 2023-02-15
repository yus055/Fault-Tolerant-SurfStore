# Fault-Tolerant SurfStore
## Description
- Developed a fault-tolerant and cloud-based file storage service called SurfStore using Go language, inspired by Dropbox
- Created a cloud service that synchronized files between the cloud and client, which interacted with the server via gRPC
- Multiple clients were able to concurrently connect to the SurfStore service to access a common, shared set of files
- Implemented a BlockStore service to store file content in blocks, and a RaftSurfStore service to hold the mapping of filenames to blocks.
- Implemented the log replication part of the RAFT protocol to achieve fault tolerant in the RaftSurfStore service


## Makefile

Run BlockStore server:
```console
$ make run-blockstore
```

Run RaftSurfstore server:
```console
$ make IDX=0 run-raft
```

Test:
```console
$ make test
```

Specific Test:
```console
$ make TEST_REGEX=Test specific-test
```

Clean:
```console
$ make clean
```
