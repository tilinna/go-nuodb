all: libcnuodb.so

libcnuodb.so: cnuodb.cpp cnuodb.h
	g++ -Wall -shared `go env GOGCCFLAGS` -I/opt/nuodb/include $< -o $@

test: all
	go test
