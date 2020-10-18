# query data
# cd ../helios/src/main
query=$1
nohup mpiexec -n 3 -f machineFile ./helios_server 1 1 > log/$1 2>&1 &
