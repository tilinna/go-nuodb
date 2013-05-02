all: libcnuodb.so ldflags.go

libcnuodb.so: cnuodb.cpp cnuodb.h
	g++ -Wall -shared `go env GOGCCFLAGS` -I/opt/nuodb/include $< -o $@ -L/opt/nuodb/lib64/ -lNuoRemote

ldflags.go:
	@echo 'package nuodb' > $@
	@echo '// #cgo LDFLAGS: -Wl,-rpath,$(CURDIR) -L $(CURDIR)' >> $@
	@echo 'import "C"' >> $@

test: all
	go test

install: all
	go install
