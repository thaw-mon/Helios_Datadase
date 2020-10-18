# Helios
### 1. Envirment Congiure
we need next package :

* boost_1_73_0:  https://dl.bintray.com/boostorg/release/1.73.0/source/boost_1_73_0.tar.gz

* oneTBB-2020.3: https://github.com/oneapi-src/oneTBB/archive/v2020.3.tar.gz

* redis-5.0.5: http://download.redis.io/releases/redis-5.0.5.tar.gz

* hiredis-0.11.0: https://github.com/redis/hiredis/archive/v0.11.0.tar.gz

* mpich-3.3: http://www.mpich.org/static/downloads/3.3.2/mpich-3.3.2.tar.gz

* zeromq-4.1.4: https://archive.org/download/zeromq_4.1.4/zeromq-4.1.4.tar.gz

This software compiles with autoMake, and you can use any version of autoMake.

### 2. Running Redis Database
As we can run several Redis DB on the same server, several configuration files can be found and each configuration file for a port. 

More configuration files can be created and more Redis DBs can be created.upder directory In the src/redis_dp, the script db_op.sh exists

To start or stop Redis server, use command ¡°./db_op.sh start|stop [port]+¡±

a.	Start Redis server with port 6377 and 6378 eg: #./db_op.sh start 6377 6378 

b.	Stop Redis server with port 6377 and 6278  eg: #./db_op.sh stop 6377 6378

##### Importance parameters in Redis configuration file

a.	Loadmodule, indicating the library we implement to extend Redis commands. 

As for our extended Redis implementation, you can directly make in src/redis-module-helios and then get the heliosmodule.so file.

b.	Bind, indicating the ip that Redis Server will be listening to. ¡°0.0.0.0¡± indicates it will receive requests from any servers, including remote servers. ¡°127.0.0.1¡± means it will only answering requests from local server.just change bind 127.0.0.1 to 0.0.0.0

c.	Port and unixsocket, indicating the TCP socket port and UDP sockfile that Redis will be listening to.

For example, port = 7371. The attribute changes are shown below £º unixsocket = /tmp/redis_7371.sock and unixsocketperm = 700

d.	Pidfile, logfile and dbfilename.

For example, port = 7371. The attribute changes are shown below
pidfile = /var/run/redis_7371.pid
logfile "log_7371"
dbfilename dump-7371.rdb
dir = ../../dump.rdb/

e.	Save, indicating whether store the updates to the disk. As Redis is in-memory database, if using save ¡°¡± then all updates are only in memory and all data will be lost if the Redis server stops. Usually when loading data we can set Redis DB to write data to disk, and when we are testing the query performance, we can disallow writing data to disk.

### 3.start load data
    the data source is come from (http://swat.cse.lehigh.edu/projects/lubm/).

    You can visit the website to get the data that has been processed: https://github.com/SJTU-IPADS/wukong/blob/master/docs/INSTALL.md#data

### 4.start server

##### 4-1 Check configuration files: helios.cfg, machineFile and hostfile

* helios.cfg: Contains parameters of Helios project.

    a.	HOST_FILE = "hostfile"  //file storing the Redis database port and sockfile info //For loading data

    b.	DATA_FILE = "dataDir_50" // file storing where source RDF triples stores

    c.	DICT_FILE = "dictDir_50"    // file storing where source RDF dictionary stores

    d.	NUM_LOADER_THREADS = 20 // number of threads running on each server to load data

    e.	DB_DICT_PORT = 6376  //redis port where the Redis DB stores the Dictionary  //For query processing

    f.	NUM_QUERY_THREADS = 1

    g.	NUM_QUERYPLAN_THREADS = 10 //repartitioning related parameters

    h.	ENABLE_REASSIGN = true //allow reassign or not

    i.	TRIGGER_K = 2              //initial trigger parameter, reassign frequency.

    j.	TRIGGER_DOUBLE = false     // trigger frequency: evenly or double

    k.	GAMA = 1.1                 //maximal imbalance ratio

    l.	BETA = 1                   //locality enlarger parameter in fennel

    m.	EDGELOG_LEN = 10000 // length of the edge weight log

    n.	VERTICELOG_LEN = 50000 // length of the vertex weight log

* hostfile, as the helios.cfg indicates: storing the local Redis port and sockefile, which indicates how many partitions the RDF dataset divided into, where each of the partitions stores, and what are the ports (for TCP socket connection to Redis DB) and the sockfile (for UDP socket connection to the corresponding Redis DB). 

* machineFile, lists all servers and is used by mpi to run program on several servers. 

##### 4-2 start helios server

Files, including program main file and configuration files, should be synchronous on all mpi running workspace. Make sure that Redis DB are also running on all servers

assert Run helios on a cluster of 3 nodes 

You can simply 'run sh server_init.sh' log_name to load the data into the RedIS database,

Run 'sh server_start.sh' log_name to start the helios Server service

Run 'sh client_query log_name' to execute the query command.The result information is saved in the Office workloads


