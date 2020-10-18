#ifndef REASSIGNQUEUE_HPP_
#define REASSIGNQUEUE_HPP_
#include <iostream>
#include <vector>
#include <set>
#include <queue>
#include <pthread.h>
#include <tbb/concurrent_unordered_map.h>
#include "type.h"
#include "triple_t.hpp"
#include "WeightUpdateTask.hpp"

using namespace std;
class ReassignQueue{

	private:
		vector<set<sid_t>> objs; //TODO 修改为队列格式
		//todo 添加一个reassigned_record;

		vector<queue<sid_t> > quene_objs;
		vector <set<sid_t>> reassign_record; //

		long long reassignment_count=0;
		int endProgram = 0;
		pthread_spinlock_t lock_reassign_count;
		pthread_spinlock_t* lock_queue;
		pthread_spinlock_t* lock_reassign_counts;
		pthread_spinlock_t lock_status;

		//vector<vector<WeightUpdateTask*>> weight_tasks;
		vector<queue <WeightUpdateTask*>>  weight_tasks_queue;
		pthread_spinlock_t* tri_locks;
	public:
		ReassignQueue(int num_servers){
			pthread_spin_init(&lock_status, 0);
			pthread_spin_init(&lock_reassign_count, 0);

			//weight_tasks.resize(num_servers);
			weight_tasks_queue.resize(num_servers);
			tri_locks = (pthread_spinlock_t*)malloc
				(sizeof(pthread_spinlock_t)*num_servers);
			for(int i = 0; i < num_servers; i++){
				pthread_spinlock_t l;
				pthread_spin_init(&l, 0);
				tri_locks[i] = l;
			}
			objs.resize(num_servers);
			lock_queue = (pthread_spinlock_t*)malloc
				(sizeof(pthread_spinlock_t)*num_servers);
			for(int i = 0; i < num_servers; i++){
				pthread_spinlock_t l;
				pthread_spin_init(&l, 0);
				lock_queue[i] = l;
			}
			quene_objs.resize(num_servers);
			reassign_record.resize(num_servers);
			lock_reassign_counts = (pthread_spinlock_t*)malloc
			(sizeof(pthread_spinlock_t) * num_servers);
			for (int i = 0; i < num_servers; i++) {
				pthread_spinlock_t l;
				pthread_spin_init(&l, 0);
				lock_reassign_counts[i] = l;
			}

		}
		virtual ~ReassignQueue(){
			free((void*)lock_queue);
			free((void*)tri_locks);
			free((void*)lock_reassign_counts);
		}

		void push(int sid, sid_t obj){
			pthread_spin_lock(&lock_queue[sid]);

			pthread_spin_lock(&lock_reassign_counts[sid]); //may use read-write lock better
			//if (reassign_record[sid].count(obj) == 0) {
				objs[sid].insert(obj);
				quene_objs[sid].push(obj);
			//}
			pthread_spin_unlock(&lock_reassign_counts[sid]);

			pthread_spin_unlock(&lock_queue[sid]);
		}

		void push(int sid, vector<sid_t> obj){
			//printf("node %d  start push data to queue\n",sid);
			pthread_spin_lock(&lock_queue[sid]);
			printf("sid = %d , before orign data = %d , send data = %d\n", sid, quene_objs[sid].size(), obj.size());
			for (int i = 0; i < obj.size(); i++) {
				if (objs[sid].count(obj[i]) == 0) {
					quene_objs[sid].push(obj[i]);
					objs[sid].insert(obj[i]);
				}
			}
			printf("sid = %d ,after orign data = %d\n", sid, quene_objs[sid].size());
			pthread_spin_unlock(&lock_queue[sid]);
		}

		void push_weight_task(int sid, WeightUpdateTask* task){
			pthread_spin_lock(&tri_locks[sid]);
			//weight_tasks[sid].push_back(task);
			weight_tasks_queue[sid].push(task);
			pthread_spin_unlock(&tri_locks[sid]);
		}
		WeightUpdateTask* pop_weight_task(int sid){
			WeightUpdateTask *task = NULL;
			pthread_spin_lock(&tri_locks[sid]);
			if (!weight_tasks_queue[sid].empty()) {
				task = weight_tasks_queue[sid].front();
				weight_tasks_queue[sid].pop();
				if ((weight_tasks_queue[sid].size() % 10) == 0)
				{
					std::cout << "sid=" << sid << "weight_tasks=" << weight_tasks_queue[sid].size() << std::endl;
				}
			}
			//vector<triple_t>().swap(edges_to_weight[sid]);
			pthread_spin_unlock(&tri_locks[sid]);
			return task;
		}

		sid_t pop(int sid){
			sid_t r = 0;
			pthread_spin_lock(&lock_queue[sid]);

			if (!quene_objs[sid].empty()) {
				r = quene_objs[sid].front();
				quene_objs[sid].pop();
				objs[sid].erase(r);
				if (((quene_objs[sid]).size() % 1000) == 0) {
						std::cout << "sid=" << sid << " objs.size=" << quene_objs[sid].size() << std::endl;
				}
			}
			pthread_spin_unlock(&lock_queue[sid]);
			return r;		
		}

		vector<sid_t> pop1k(int sid) {
			vector<sid_t> ret;
			pthread_spin_lock(&lock_queue[sid]);
			int size = quene_objs[sid].size();
			for (int i = 0; i < min(1000,size); i++) {
				sid_t r = quene_objs[sid].front();
				ret.push_back(r);
				quene_objs[sid].pop();
				objs[sid].erase(r);
				
			}
			//if (((quene_objs[sid]).size() % 1000) == 0) {
			std::cout << "sid=" << sid << " objs.size=" << quene_objs[sid].size() << std::endl;
			//}
			pthread_spin_unlock(&lock_queue[sid]);
			return ret;

		}
		void end1Program(){
			pthread_spin_lock(&lock_status);
			endProgram++;
			pthread_spin_unlock(&lock_status);
		}

		int endPrograms(){
			return endProgram;
		}

		bool isEmpty(){
			bool flag = true;
			for(int i = 0; i < objs.size(); i++){
				if(objs[i].size() >0 )
					flag = false;
			}
			return flag;
		}

		int set_reaasignment_count(int sid)
		{
			pthread_spin_lock(&lock_reassign_counts[sid]);
			reassignment_count++;
			//std::cout << "sid=" << sid << " reassignment_count=" << reassignment_count << std::endl;
			pthread_spin_unlock(&lock_reassign_counts[sid]);
		}

		int set_reaasignment_count(int sid,int obj)
		{
			pthread_spin_lock(&lock_reassign_counts[sid]);
			reassignment_count++;
			//reassign_record[sid].insert(obj);
			//std::cout << "sid=" << sid << " reassignment_count=" << reassignment_count << std::endl;
			pthread_spin_unlock(&lock_reassign_counts[sid]);
		}

};

#endif
