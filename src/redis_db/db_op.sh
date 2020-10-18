num=$#
com=$1
for((i=1; i<$num;i++))
do 
	./redis_script $2 $com &
	shift 1
done
