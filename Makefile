libcnuodb.so: cnuodb.cpp cnuodb.h
	g++ -Wall -shared `go env GOGCCFLAGS` -I/opt/nuodb/include $< -o $@ -L/opt/nuodb/lib64/ -lNuoRemote

all: libcnuodb.so

test: all
	go test -ldflags="-r ."
