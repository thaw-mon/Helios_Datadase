# start server module 
nohup mpiexec -n 3 -f machineFile ../server/helios_server 1 1 1 > server_log/$1 2>&1 &
