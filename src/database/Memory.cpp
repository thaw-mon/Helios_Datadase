#include "Memory.hpp"
#include "Database.hpp"

int Memory::init_lock(sid_t x, lock_type type){
	if(type == VERTEX){
		pthread_spin_lock(&v_lock_lock);
		if(v_locks.find(x) == v_locks.end()){
			pthread_spinlock_t l;
			pthread_spin_init(&l, 0);
			v_locks[x] = l;
		}
		pthread_spin_unlock(&v_lock_lock);
		return 1;
	}else if(type == PREDICATE){
		pthread_spin_lock(&p_lock_lock);
		if(p_locks.find(x) == p_locks.end()){
			pthread_spinlock_t l;
			pthread_spin_init(&l, 0);
			p_locks[x] = l;
		}
		pthread_spin_unlock(&p_lock_lock);
		return 1;
	}else{
		printf("ERR - wrong LOCK type");
		return 0;
	}
} 

/*int Memory::get_v_loc(sid_t v){
	if(v_loc.find(v) == v_loc.end())	
		return -1;
	else return v_loc[v];
}

int Memory::set_v_loc(sid_t v, int sid){
	if(!init_lock(v, VERTEX))
		return 0;	
	pthread_spin_lock(&v_locks[v]);
	v_loc[v] = sid;
	pthread_spin_unlock(&v_locks[v]);
	return 1;
}*/

int Memory::get_p_counter(sid_t p, int *scounter, int *ocounter, Database *db){
	if(!init_lock(p, PREDICATE))
		return 0;
	pthread_spin_lock(&p_locks[p]);
	if(p_counter.find(p) != p_counter.end()){
		int* counters = p_counter[p];
		pthread_spin_unlock(&p_locks[p]);
		(*scounter) = counters[0];
		(*ocounter) = counters[1];
		return 1;
	}
	//else 
	int *counters = (int *)malloc(sizeof(int) * 2);
	counters[0] = 0;
	counters[1] = 0;	
	db->get_global_pcounter(p, counters, &(p_out_loc[p]), &(p_in_loc[p]));
	p_counter[p] = counters;
	
	/*p_counter[p] = (int *)malloc(sizeof(int) * 2);
	p_counter[p][0] = 0;
	p_counter[p][1] = 0;	
	db->get_global_pcounter(p, p_counter[p], &(p_out_loc[p]), &(p_in_loc[p]));
	int *counters = p_counter[p];*/

	(*scounter) = counters[0];
	(*ocounter) = counters[1];
	pthread_spin_unlock(&p_locks[p]);
	printf("pcounter %d %d %d \n", p, *scounter, *ocounter);
	return 1;	
}

vector<int> Memory::get_p_loc(sid_t p, dir_t d){
	if(d == OUT){
		if(p_out_loc.find(p) == p_out_loc.end())
			return vector<int>();
		else return p_out_loc[p];
	}
	else if(d == IN){
		if(p_in_loc.find(p) == p_in_loc.end())
			return vector<int>();
		else return p_in_loc[p];
	}
}

/************** Begin: request lock in memory? ***********************/
/*bool Memory::request_v_lock(sid_t v, lock_t lock){
	if(!init_lock(v, VERTEX))
		return 0;
	bool flag = false;
	
	pthread_spin_lock(&v_locks[v]);
	if(v_lock_table.find(v) == v_lock_table.end()){
		v_lock_table[v] = (int *)malloc(sizeof(int) * 2);
		v_lock_table[v][0] = 0;
		v_lock_table[v][1] = 0;
	}

	if(lock == RW_LOCK){
		if(v_lock_table[v][1] > 0){//under reassignment
			assert(v_lock_table[v][0] == 0);	
		}else{
			v_lock_table[v][0] ++;
			flag = true;
		}
	}else if(lock == REASSIGN_LOCK){
		if(v_lock_table[v][0] == 0 
			&& v_lock_table[v][1] == 0){
			v_lock_table[v][1]++;
			flag = true;
		}
	}
	pthread_spin_unlock(&v_locks[v]);
	return flag;
}

bool Memory::release_v_lock(sid_t v, lock_t lock){
	bool flag = false;
	
	pthread_spin_lock(&v_locks[v]);
	assert(v_lock_table.find(v) != v_lock_table.end());

	if(lock == RW_LOCK){
		assert(v_lock_table[v][1] == 0);
		assert(v_lock_table[v][0] > 0);
		v_lock_table[v][0] --;
		flag = true;
	}else if(lock == REASSIGN_LOCK){
		assert(v_lock_table[v][0] == 0);
		assert(v_lock_table[v][1] == 1);
		v_lock_table[v][1] --;
		flag = true;
	}
	pthread_spin_unlock(&v_locks[v]);
	return flag;
}*/
/************************End: lock **********************************/
