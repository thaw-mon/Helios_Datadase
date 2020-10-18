#ifndef MEMORY_HPP_
#define MEMORY_HPP_

#include <pthread.h>
#include <vector>
#include <tbb/concurrent_unordered_map.h>
#include "type.h"
#include "triple_t.hpp"
#include "ReassignQueue.hpp"
#include <map>
/** GlobalMetaData includes: 
 * 	v-loc (Redis String without replica; Set with replica),
 *	v_lock (Hash Map)
 *		works as a global lock, because accessing each vertex requires reading this value
 *		v - 0 -read/write#
 *		v - 1 - isDuringReassign
 *
 * 	pd-loc, (Redis HashMap, as Set is not easy to delete)
 * 	pd-counter (Redis String)
 *	
 * The storage server of globalMetaData is fixed based on consistent hash of the key.
 *
 * */


//shared by all threads on the same server

using namespace std;
class Database;
class Memory {
	private:
		
		typedef tbb::concurrent_unordered_map<sid_t, int> V_LOC_T;
		typedef tbb::concurrent_unordered_map<sid_t, int*> V_LOCK_T;
		
		typedef tbb::concurrent_unordered_map<sid_t, int*> P_COUNTER_T;
		typedef tbb::concurrent_unordered_map<sid_t, vector<int>> P_LOC_T;

		typedef tbb::concurrent_unordered_map<sid_t, pthread_spinlock_t> SPINLOCK_T;
		
		typedef tbb::concurrent_unordered_map<sid_t, map<int, vector<sid_t>*>> INDEX_CACHE;

	//	V_LOC_T v_loc;

		/**must be on the initial server*/
		V_LOCK_T v_lock_table; // v - int[0] num-read-write; int[1] is-migration
		SPINLOCK_T v_locks;
		pthread_spinlock_t v_lock_lock;

		P_COUNTER_T p_counter; // p - int[0] OUT-counter; int[1] IN-counter 
		P_LOC_T p_out_loc;
		P_LOC_T p_in_loc;
		SPINLOCK_T p_locks;
		pthread_spinlock_t p_lock_lock;

		enum lock_type{VERTEX = 1, PREDICATE};

		INDEX_CACHE index_cache;


		V_LOC_T metis_loc;

		typedef tbb::concurrent_unordered_map<sid_t, int> VERTEX_WEIGHT;
		typedef tbb::concurrent_unordered_map<sid_t, 
			tbb::concurrent_unordered_map<sid_t, int>> EDGE_WEIGHT;
	public:
		ReassignQueue *reassign_queue;
		Memory(int num_servers){
			reassign_queue = new ReassignQueue(num_servers);
			pthread_spin_init(&v_lock_lock, 0);
			pthread_spin_init(&p_lock_lock, 0);
		}
		virtual ~Memory(){
			//printf("why shy");
			for(auto l : v_lock_table){
				if(l.second != NULL){
					delete l.second;
					l.second = NULL;
				}
			}
			
			for(auto l : p_counter){
				if(l.second != NULL){
					free((void*)l.second);
					l.second = NULL;
				}
			}
			
			/*for(auto l : index_cache){
				if(l.second != NULL){
					delete l.second;
					l.second = NULL;
				}
			}*/
			delete reassign_queue;
		}

		/*concurrency control of query and reassignment*/
	/*	bool request_v_lock(sid_t v, lock_t lock);

		bool release_v_lock(sid_t v, lock_t lock);*/

		int init_lock(sid_t x, lock_type type);

		//int get_v_loc(sid_t v);

		//int set_v_loc(sid_t v, int sid);

		/*global statistical info of predicate*/

		int get_p_counter(sid_t p, int *scounter, int *ocounter, Database *db);

		vector<int> get_p_loc(sid_t p, dir_t d);

		int put_index(sid_t key, int sid, vector<sid_t> *value);

		vector<sid_t>* get_index(sid_t key, int sid);
		
		bool is_local_metis(sid_t v){
			if(metis_loc.find(v) == metis_loc.end())	
				return false;
			else return true;
		}

		void set_local_metis(sid_t v){
			metis_loc[v] = 1;
		}
};

#endif
