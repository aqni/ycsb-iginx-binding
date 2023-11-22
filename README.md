# IGinX-YCSB-Binding

To run YCSB on IGinX.

## Build

```
mvn clean package
```

## Usage

### Run the YCSB benchmark

Load the data:
```shell
./bin/ycsb.bat load iginx -s -P workloads/workloada > outputLoad.txt
```

Run the workload test:
```shell
./bin/ycsb.bat run iginx -s -P workloads/workloada > outputRun.txt
```

### Properties

Set host, port, user, and password in the workload file.

| properties       | default     |
|------------------|-------------|
| `iginx.host`     | `127.0.0.1` |
| `iginx.port`     | `6888`      |
| `iginx.user`     | `root`      |
| `iginx.password` | `root`      |

Or set configs with the shell command:

```shell
./bin/ycsb.bat load iginx -s -P workloads/workloada -p "iginx.host=127.0.0.1" -p "iginx.port=6888" > outputLoad.txt
```

## Contributing

PRs accepted.

## License
Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0