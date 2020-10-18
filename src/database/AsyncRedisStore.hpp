#ifndef ASYNC_REDIS_STORE_HPP_
#define ASYNC_REDIS_STORE_HPP_
#include <stdlib.h>
#include <stdio.h>
#include <cassert>
#include <map>
extern "C"{
#include <hiredis/hiredis.h>
//#include <hiredis/adapters/ae.h>
//#include <hiredis/adapters/libevent.h>
//#include <hiredis/adapters/libev.h>
#include "type.h"
}
#include <vector>
#include "triple_t.hpp"
#include "QueryEdge.hpp"
#include "AsyncCallBack.hpp"
#include "HeliosConfig.hpp"
using namespace std;
class AsyncRedisStore{
	private:
		int current_sid;
		const char *host;
		int 	   port;
		const char* sockfile;

		int tid;
		HeliosConfig* cfg;
		//aeEventLoop *loop;
	public:
		int connectto_sid;
		redisAsyncContext *c;

		int connect(){
			if(current_sid == connectto_sid && tid == 0)
				c = redisAsyncConnectUnix(sockfile);
			else 
				c = redisAsyncConnect(host, port);
		}

		AsyncRedisStore(int current_sid,  int connectto, int tid, const char *ip, int port, 
				const char* sockfile, HeliosConfig* cfg)
			:current_sid(current_sid),connectto_sid(connectto), tid(tid), host(ip), port(port), 
			sockfile(sockfile), cfg(cfg){
			
			/*if(current_sid == connectto_sid && tid == 0)
				c = redisAsyncConnectUnix(sockfile);
			else 
				c = redisAsyncConnect(host, port);*/
			connect();
			while(c == NULL || c->err){
				if(c){
					printf("async MY ERROR: %s \n", c->errstr);
					redisAsyncFree(c);
				}
				else printf("async MY ERROR: cannot allocate redis context!\n");
				connect();	
			}
		  	//loop = aeCreateEventLoop(64);	
			//redisAeAttach(loop,c);
			//redisLibevAttach(EV_DEFAULT_ c);
			redisAsyncSetConnectCallback(c, connectCallback);
			redisAsyncSetDisconnectCallback(c, disconnectCallback);
		}

		virtual ~AsyncRedisStore(){
			if(c != NULL && !c->err){
				redisAsyncDisconnect(c);
				//redisAsyncFree(c);
			}
		}

		void get_p_counter(sid_t p, vector<int> *buf);

		void get_reassign_info( sid_t v, vector<int> *buf, bool isTS);
	
		void batch_update_edgeweight(sid_t root_v, const vector<triple_t> &triples, int edgelog_len);
		void batch_update_vertexweight(const vector<sid_t> &vs, int incr_weight, int log_len);

		//void batch_update_DoI_index(const vector<triple_t>& triples, int edgelog_len);

		void get_server_info();

	//	void write_command(const char** command, int argc);
		void write_command(const vector<const char*> &params);
		void write_command(const vector<string> &params);

		void read_zset(string key, vector<sid_t> *results); 
		void read_zset(sid_t key, vector<sid_t> *results); 
		
		void read_set(const char* key, vector<sid_t> *results);
		void read_set(sid_t key, vector<sid_t> *results);
		
		void traverse_vertex(QueryNode *queryRoot, const vector<sid_t> &vs, QueryEdge* edge, 
		vector<triple_t> *results);

		void batch_traverse_vertex(QueryNode* queryRoot, const vector<sid_t>& vs, QueryEdge* edge, vector<triple_t>* results);

		void batch_insert_DoI_index(vector<map<sid_t, int>>& DoI_index);

		void batch_insert_DoI_index(map<sid_t, int>& DoI_index, int host);

		void batch_insert_DoI_index(vector<vector<sid_t>>& DoI_index);

		//void batch_read_test(const vector<sid_t>& vs, vector<int>* result);

		//void batch_write_test(const vector<sid_t>& vs);

		//void batch_insert_DoI_index_new(vector<vector<pair<sid_t, sid_t>>> DoI_index, dir_t d);

		void del_keys(vector<sid_t> keys);

		void updateDoI_index(vector<sid_t> vs, int loc, int tgt, sid_t value);

		void get_DoI_List(sid_t v, int excuted_num, vector<int>* results);

		void batch_get_DoI_List(vector<sid_t>& vs, int excuted_num, vector<int>* results);

		//void get_DoI_List_new(sid_t v, int excuted_num, vector<int>* results);

		int get_max_Di(sid_t val, sid_t sid, int num_servers);
		bool isExist(sid_t s);

		vector<sid_t> batch_isExist(vector<sid_t>& vs);

		bool remove_DoI_index(vector<string> params);

		void batch_getloc(const vector<sid_t> &vs, map<int, vector<sid_t>> *v_locs);

		void batch_getloc3(const vector<sid_t>& vs, map<sid_t, int>* v_locs);

		void batch_getloc2(const vector<sid_t>& vs, map<int, vector<sid_t>>* v_locs);

		//add function 
		void loadPredicates(sid_t s, dir_t d, vector<sid_t>* result);

		void loadNeighbors(sid_t s, sid_t p, dir_t d, vector<sid_t>* results);

		void loadEntities(sid_t p, dir_t d, vector<sid_t>* result);

		void loadEntitiesSet(vector<sid_t> pset, dir_t d, vector<sid_t>* result);
};


#endif
