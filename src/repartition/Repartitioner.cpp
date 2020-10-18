#include "Repartitioner.hpp"
#include "omp.h"
#include <sys/syscall.h>

void Repartitioner::run() {
	printf("repartitioner_thread \n");
	printf(" thread lwpid = %u currentNode=%d\n", syscall(SYS_gettid), sid);
#pragma omp parallel for num_threads(num_servers)
	for (int i = 0; i < num_servers; i++) {
		//	int i = sid;
		std::cout << "sid=" << sid << "tgt_s:" << i << "Repartitioner::run" << std::endl;
		while (1) {//queue()->endPrograms() != cfg->num_query_threads){
			sid_t obj = queue()->pop(i);
			if (obj == 0) {
				sleep(10); //no task sleep
				continue;
			}

			int tgt_node = db[i]->reassign_evaluate2(i, obj); 

			if (tgt_node == i)
				continue;
			if (tgt_node == -1)// vertex is reassigned to another host
				continue;
			printf("%d reassign %d to %d\n",i, obj, tgt_node);
			sid_t start = get_usec();
			int status = db[i]->client_reassign(i, obj, tgt_node);
			sid_t end = get_usec();
			//printf("%d reassign %d to %d spend %ld  m second\n", i, obj,tgt_node, (end - start) / 1000);
			if (status == 0) //request lock fail
			{
				printf("Fatal error,%ld reassign lock fail\n", obj); 
			}
			else if (status == -1) {
				printf("Fatal error, %ld reassign error!\n", obj);
			}
			else if (status == -2) {
				printf("Fatal error, %ld receive fails!\n", obj);
			}
			else if (status == -3) {
				printf("Fatal error, %ld deduplicate fails!\n", obj);
			}
			else
			{
				queue()->set_reaasignment_count(sid);
				printf("Succeed: @%d %d reassign %ld to %d\n", sid, i, obj, tgt_node);
			}
		}
		//TODO 未完待续
		//while (1)
		//{
		//	vector<sid_t> objs = queue()->pop1k(i);
		//	if (objs.size()== 0) {
		//		sleep(10); //没有任务睡眠10 second
		//		continue;
		//	}

		//	map<int,vector<sid_t>> reassign_map = db[i]->batch_reassign_evaluate2(i, objs);
		//	if(reassign_map == NULL || reassign_map[i].size() == objs.size())
		//		continue;
		//	sid_t start = get_usec();

		//	for (int tgt = 0; tgt < num_servers; tgt++) {
		//		if (i == tgt || reassign_map[i].size() == 0) continue;
		//		printf("%d reassign to %d size = %d\n", i, tgt, reassign_map[i].size());
		//		db[i]->client_reassign(i, reassign_map[i], tgt);
		//	}
		//	//int status = db[i]->client_reassign(i, obj, tgt_node);
		//	sid_t end = get_usec();
		//	printf("%d reassign %d to %d spend %ld  m second\n", i, obj, tgt_node, (end - start) / 1000);
		//	if (status == 0) //request lock fail
		//	{
		//		printf("Fatal error,%ld reassign lock fail\n", obj);
		//	}
		//	else if (status == -1) {
		//		printf("Fatal error, %ld reassign error!\n", obj);
		//	}
		//	else if (status == -2) {
		//		printf("Fatal error, %ld receive fails!\n", obj);
		//	}
		//	else if (status == -3) {
		//		printf("Fatal error, %ld deduplicate fails!\n", obj);
		//	}
		//	else
		//	{
		//		queue()->set_reaasignment_count(sid);
		//		printf("Succeed: @%d %d reassign %ld to %d\n", sid, i, obj, tgt_node);
		//	}
		//}
	}
}

void Repartitioner::up_edge_weight() {
	printf("metadata_update_thread \n");
	printf(" thread lwpid = %u currentNode=%d\n", syscall(SYS_gettid), sid);
#pragma omp parallel for num_threads(num_servers)
	for (int i = 0; i < num_servers; i++) {
		//int i = sid;
		std::cout << "sid=" << sid << "tgt_s:" << i << "Repartitioner::up_edge_weight begin" << std::endl;
		while (1) {//queue()->endPrograms() != cfg->num_query_threads){
			//TODO add update DoI index information
			WeightUpdateTask* task = queue()->pop_weight_task(i);
			if (task == NULL)
			{
				//printf("No update weight task! wait ... ");
				//std::cout << "sid" << sid << "No update weight task! wait ... " << std::endl;
				sleep(10); continue;
			}
			if (task->isUpdateEdge()) {
				printf("begin update edgeweight\n");
				vector<triple_t> edges = task->getEdge();
				if (edges.size() > 0)
					db[i]->batch_up_edgeweight(i, task->getRoot_v(), edges);
				printf("end update edgeweight \n");
			}
			else if(task->isUpdateVertex()){
				printf("begin update vertex weight\n");
				vector<sid_t> vs = task->getVertex();
				int incr_weight = task->getIncrWeight();
				sid_t start = get_usec();
				if (vs.size() > 0) {
					db[i]->batch_up_vertexweight(i, vs, incr_weight);
					queue()->push(i, vs); 
				}
				sid_t end = get_usec();
				printf("Test :update vertex weight spend Time  = %d second\n", (end - start) / 1000000);
				printf("end update vertex weight\n");
			}
			else {
				//TODO this update move to vertex
				printf("begin update DoI INDEX info node = %d\n",i);
				vector<triple_t> edges = task->getEdge();
				//
				sid_t start = get_usec();
				//db[i]->insert_DoI_index(edges, i);
				sid_t end = get_usec();
				//printf("Test : insert_DoI_index spend Time  = %d second\n", (end- start) / 1000000);
				/*start = get_usec();
				db[i]->insert_DoI_index2(edges, i);
				end = get_usec();
				printf("Test :insert_DoI_index2 spend Time  = %d second\n", (end - start) / 1000000);*/
				//
				/*start = get_usec();
				db[i]->insert_DoI_index3(edges, i);
				end = get_usec();
				printf("Test :insert_DoI_index3 spend Time  = %d second\n", (end - start) / 1000000);*/
				//
				start = get_usec();
				db[i]->insert_DoI_index4(edges, i);
				end = get_usec();
				printf("Test :insert_DoI_index4 spend Time  = %d second\n", (end - start) / 1000000);

				printf("end update DoI INDEX info node = %d\n",i);
			}
			delete task;

		}
	}
}

/*void Repartitioner::up_metadata_task(WeightUpdateTask* task){
	if(task == NULL)
	{
		return;
	}
	if(task->isUpdateEdge()){
		printf("begin update edgeweight\n");
		vector<triple_t> edges = task->getEdge();
		if(edges.size() > 0)
			db->batch_up_edgeweight(i, edges);
		printf("end update edgeweight \n");
	}else{
		printf("begin update vertex weight\n");
		vector<sid_t> vs = task->getVertex();
		int incr_weight = task->getIncrWeight();
		if(vs.size() > 0){
					db->batch_up_vertexweight(i, vs, incr_weight);
					queue()->push(i, vs);
				}
				printf("end update vertex weight\n");
			}
			delete task;

		}
	//}
}*/
