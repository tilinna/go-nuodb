go-nuodb
========

NuoDB driver for Go's database/sql interface.

It wraps the libNuoRemote.so C++ API with a custom C API and then uses CGO for calling it.

Tested with Go 1.0.3 and NuoDB 1.0.2-142-902dc7c on x86_64 CentOS 6.4.

## Installation

### Download and build the package

```shell
$ go get -d github.com/tilinna/go-nuodb
$ make -C `go env GOPATH`/src/github.com/tilinna/go-nuodb
$ go install github.com/tilinna/go-nuodb
```

### Setup NuoDB for the package tests

```shell
$ java -jar /opt/nuodb/jar/nuoagent.jar --broker --domain go-nuodb --password archer &
$ /opt/nuodb/bin/nuodb --allow-non-durable --database tests --initialize --password archer --dba-user robinh --dba-password crossbow &
```

### Run the package tests

```shell
$ go test -ldflags="-r ." github.com/tilinna/go-nuodb
```
