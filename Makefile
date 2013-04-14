libcnuodb.so: cnuodb.cpp cnuodb.h
	g++ -Wall -shared `go env GOGCCFLAGS` -I/opt/nuodb/include -L/opt/nuodb/lib64/ -lNuoRemote $< -o $@

all: libcnuodb.so

test: all
	go test -ldflags="-r ."
