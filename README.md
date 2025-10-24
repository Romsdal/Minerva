# Minerva
Artifact Repository for Machine Learning Models *Epoch-based Optimistic Concurrency Control in Geo-replicated Databases*.

## Dependencies
1. Python 3.10+
2. DotNet 9.0+
3. Ubuntu 24.04

## Setup Instructions
1. Clone the repository
2. Build the StableSolver submodule, check readme in `Minerva/StableSolver/README.md` for instructions. There are additional dependencies for StableSolver. Pre-built binaries for Ubuntu 24.04 are provided.

Minerva consists of database server application and client application. The database server runs in a cluster and must have > 3 nodes for fault tolerance. The client application can be run on any machine and connect to each server through TPC sockets.

### Running the Database Server

Under the `scripts` directory, create `configs` directory and place your `logger_config.json` and `minerva_config.json` files there, see `DB-Server/minerva_config_example.json` and `DB-Server/logger_config_example.json` for example configuration files.


Minevrva config explanation:
- `ReadStorage`: Path to the directory where the database files are stored. (optional, set empty string to disable)
- `DatabaseToLoad`: List of database names to load on startup. Can be "YCSB" (key-value store) or "TPCC"
- `SolverExact`: Boolean flag to indicate whether to use exact solver or approximate solver.
- `ReplicaPriority`: List of replica id indicating the priority of each replica using the order of the list, must equal to the number of replicas in the cluster.
- `LocalEpochInterval`: Interval (in milliseconds) for local epoch advancement.
- `CoordinatorGlobalEpochInterval`: Interval (in milliseconds) for global epoch advancement by the coordinator


```
$cd scripts
$./start_servers.py rstart 1 [ip1, ip2, ip3, ...]'
```
This will start the database server cluster with the specified number of replicas. Each server will be started on the specified IP addresses, each ip address should correspond to a different machine (must be unique). Server id is assigned based on the order of the ip addresses provided, starting from 0.

See `scripts/start_servers.py --help` to change port numbers and other configurations. Port is default to 5000.

### Running the Client Application
```
$cd Client
```

To run the client application in benchmark mode:
```
 dotnet run -c Release [benchmark_config.json]
```
See `Client/benchmark_config_example.json` for an example configuration file.

Set `pre_load_db` to true if the server database is set to read from Storage otherwise the client will populate the database on startup.