#ifndef TRIPLEDB_HPP_
#define TRIPLEDB_HPP_

#include "RedisStore.hpp"
/*
 * TripleStore includes:
 * 	triple info:  
 * 		vpd-v, vd-p.
 * 	v-related meta data:
 * 		v+COUNTER - weight & edgenum  (Redis HashMap v+COUNTER - 0|1 - num)
 *		workload/topology locality degree(HashMap):
 *			v - Exists/TOP/WORK - #
 *	
 *	Edge_log: 
 *		vid_i - vid - weight (HashMap)
 *		MRA bucket "EdgeList - vid_vid_i" (List)
 * 
 * Local TripleStore keeps vertex-related info that are reassigned with vertex.
 * 
 * */

class TripleDB : public RedisStore{
	private:
		const int WEIGHT = 0, EDGENUM = 1;
		const int TOP = 0, WORKLOAD= 1, EXISTS = 2;

	public:
		TripleDB(const char* host, int port)
			: RedisStore(host, port){}

		virtual ~TripleDB(){};

		bool insert_triple(const triple_t &triple){
			return _insert_triple(triple.s, triple.p, triple.o, triple.d);
		}

		bool _insert_triple(sid_t s, sid_t p, sid_t o, dir_t d){
			
			redisAppendCommand(contxt, 
				"helios.insert_triple %ld %ld %ld %d", s, p, o, d);
			pip_command++;
			
			if(pip_command > 100)
				flush_command();
			return true;
		}
		
		int batch_set_vloc(sid_t v, int loc){
			sid_t vl_key = key_vpid_t(v, 0, LOC, (dir_t)0);
			redisAppendCommand(contxt,"SET %ld %d", vl_key, loc);
			pip_command++;
			
			if(pip_command > 100)
				flush_command();
			return true;
		}

		void batch_insert_DoI_index(vector<map<sid_t, int>> DoI_index) {
			int num = DoI_index.size();
			for (int i = 0; i < num; i++) {
				//string field = "node" + to_string(i);
				for (map<sid_t, int>::iterator iter = DoI_index[i].begin(); iter != DoI_index[i].end(); iter++) {
					redisAppendCommand(contxt, "HINCRBY %ld %d %d", key_vpid_t(iter->first, 0, DoI_INDEX, (dir_t)0), i, iter->second);
					pip_command++;
					if (pip_command > 1000)
						flush_command();
				}
			}
			flush_command();
		}

		void batch_insert_DoI_index(vector<vector<sid_t>> DoI_index) {
			int num = DoI_index.size();
			for (int i = 0; i < num; i++) {
				//string field = "node" + to_string(i);
				for (int obj : DoI_index[i]) {
					redisAppendCommand(contxt, "HINCRBY %ld %d %d", key_vpid_t(obj, 0, DoI_INDEX, (dir_t)0), i, 1);
					pip_command++;
					if (pip_command > 1000)
						flush_command();
				}
				
			}
			flush_command();
		}

		//restore [sd_key,p_list]
		bool reassign_vd(sid_t v, dir_t d, TripleDB* tgtDB){
			bool flag = true;
			sid_t sd_key = key_vpid_t(v, 0, NO_INDEX, d);
			//printf("TEST: reassign_vd start sd_key %ld %ld\n", v, sd_key);
			size_t len = 0;
			char* str2 = dump(sd_key, &len);
			//if (len < 0) flag = false; //  reply ==null
			if (str2 != NULL) {
				flag = flag && tgtDB->restore(sd_key, str2, len);
				free(str2);
			}
			if(!flag)
				printf("Error, reassign vd fail! v = %ld, d = %d, sd_key = %ld\n",v,d, sd_key);
			return flag;
		}

		bool migrate_vd(sid_t v, dir_t d, const char* hostname, int port) {
			bool flag = true;
			sid_t sd_key = key_vpid_t(v, 0, NO_INDEX, d);
			flag = flag && migrate(sd_key, hostname, port);
			if (!flag)
				printf("Error, migrate vd fail!\n");
			return flag;
		}

		//restore [sp_key ,o_vals]
		bool reassign_vpd(sid_t v, sid_t p,dir_t d, TripleDB* tgtDB){
			bool flag = true;
			sid_t sp_key = key_vpid_t(v, p, NO_INDEX, d);
			//printf("TEST: reassign_vpd start sp_key %ld %ld\n", v, sp_key);
			size_t len = 0;
			char* str = dump(sp_key, &len);
			//if (len < 0) flag = false; //  reply ==null
			if (str != NULL) {
				flag = flag && tgtDB->restore(sp_key, str, len);
				free(str); 
			}
			//assert(str != NULL);
			if(!flag)
				printf("Error, reassign vpd fail! %ld %ld %d %ld \n", v, p, d, sp_key);
			return flag;
		}

		bool migrate_vpd(sid_t v, sid_t p, dir_t d, const char* hostname, int port) {
			bool flag = true;
			sid_t sp_key = key_vpid_t(v, p, NO_INDEX, d);
		
			flag = flag && migrate(sp_key, hostname, port);
			if (!flag)
				printf("Error, migrate vpd fail!\n");
			return flag;
		}
		void get_neighbor(sid_t v, const vector<sid_t> &p_list, dir_t d, vector<sid_t>* neighbors){
			
			for(int i = 0; i < p_list.size(); i++){
				sid_t p = p_list[i];
				sid_t sp_key = key_vpid_t(v, p, NO_INDEX, d);
				redisAppendCommand(contxt,"GET %ld", sp_key);
			}
			
			redisReply* reply;
			for(int i = 0; i< p_list.size(); i++){
				reply = redis_get_reply(contxt);
				if(reply == NULL || reply -> type != REDIS_REPLY_STRING){
					printf("get neighbor sp error %ld %ld!\n",v,p_list[i]);
					continue;
				}
				deserial_vector(reply->str, reply->len, neighbors);
				freeReplyObject(reply);
			}
		}

		void get_neighbor(sid_t v, sid_t p, dir_t d, vector<sid_t>* neighbors) {

			sid_t sp_key = key_vpid_t(v, p, NO_INDEX, d);
			redisAppendCommand(contxt, "GET %ld", sp_key);

			redisReply* reply;
			reply = redis_get_reply(contxt);
			if (reply == NULL || reply->type != REDIS_REPLY_STRING) {
				printf("get neighbor sp error %ld %ld!\n", v, p);
				return;
			}
			deserial_vector(reply->str, reply->len, neighbors);
			freeReplyObject(reply);
		}

		//get vertex by p and d info
		void get_vs(sid_t p, dir_t d, vector<sid_t>* results) {
			
			sid_t pd_key = key_vpid_t(0, p, INDEX, d);
			redisAppendCommand(contxt, "SMEMBERS %ld", pd_key);
			redisReply* reply  = redis_get_reply(contxt);;
			if (reply == NULL || reply->type != REDIS_REPLY_ARRAY) {
				printf("get vertex sp error %ld %ld!\n", p, d);
				return;
			}
			for (int i = 0; i < reply->elements; i++) {
				if (reply->element[i]->type == REDIS_REPLY_STRING) {
					results->push_back(stringToNum<int>(reply->element[i]->str));
				}
				else if (reply->element[i]->type == REDIS_REPLY_INTEGER) {
					results->push_back(reply->element[i]->integer);
				}
				else {
					results->push_back(0);
				}
			}
			freeReplyObject(reply);
		}

		//get p set by s and d
		void get_pset(sid_t s, dir_t d, vector<sid_t>* results) {
			sid_t sd_key = key_vpid_t(s, 0, NO_INDEX, d);
			redisAppendCommand(contxt, "SMEMBERS %ld", sd_key);
			redisReply* reply = redis_get_reply(contxt);;
			if (reply == NULL || reply->type != REDIS_REPLY_ARRAY) {
				printf("get p set error %ld %ld!\n", s, d);
				return;
			}
			for (int i = 0; i < reply->elements; i++) {
				if (reply->element[i]->type == REDIS_REPLY_STRING) {
					results->push_back(stringToNum<int>(reply->element[i]->str));
				}
				else if (reply->element[i]->type == REDIS_REPLY_INTEGER) {
					results->push_back(reply->element[i]->integer);
				}
				else {
					results->push_back(0);
				}
			}
			freeReplyObject(reply);
		}

		//[sc_key,count] and [ew_key,EDGE_WEIGHT] 
		bool reassign_vcounter_weight(sid_t v, TripleDB* tgtDB, bool isTS){
			bool flag = true;
			sid_t sc_key = key_vpid_t(v, 0, COUNTER, (dir_t)0);
			//printf("start reassign_vcounter_weight sc_key = %ld\n", sc_key);
			size_t len = 0;
			char* str = dump(sc_key, &len);
			if (str != NULL) {
				flag = flag && tgtDB->restore(sc_key, str, len);
				free(str);
			}
			if (!flag) {
				printf("Error, sc_key error! v = %ld sc_key = %ld\n", v,sc_key);
			}
			if(!isTS){
				sid_t ew_key = key_vpid_t(v, 0, EDGE_WEIGHT, (dir_t)0);
				char *str2 = dump(ew_key, &len);
				if (str2 != NULL) {
					flag = flag && tgtDB->restore(ew_key, str2, len);
					free(str2);
				}
				if (!flag) {
					printf("Error, reassign ew_key error! v = %ld ew_key = %ld\n", v, ew_key);
				}
			}
			if(!flag)
				printf("Error, reassign counter & weight error! v = %ld\n",v);

			return flag;
		}
		
		//todo push D[tgt]--> target 
		bool reassign_DoI_index(sid_t v, TripleDB* tgtDB,int tgt) {
			bool flag = true;
			//1. get D[tgt] val
			sid_t doI_key = key_vpid_t(v, 0, DoI_INDEX, (dir_t)0);
			int value = get_hash_key(doI_key, tgt);
			if (value < 0) {
				printf("get_hash_key fail\n");
				flag = false;
			}
			//2. push value to tgtDB
			flag = flag && tgtDB->set_hash_map(doI_key, tgt, value);
			if (!flag) printf("v= %ld push value to %d fail \n", v, tgt);
			//3. del keys
			flag = flag && del_key(doI_key);
			return flag;
		}
		void getDoI_Index_size(sid_t v, int num_servers, vector<int>* result) {
			//printf("get_DoI_List_new start v = %ld\n", v);
			for (int i = 0; i < num_servers; i++) {
				sid_t doI_key = key_vpid_t(v, i, DoI_INDEX, (dir_t)OUT);
				//printf("DoI key = %ld\n", doI_key);
				result->push_back(get_zset_len(doI_key));
			}
			//printf("get_DoI_List_new end v = %ld, result size = %d\n", v, result->size());
		}
		//
		bool reassign_doI_index(sid_t v, int num_servers,TripleDB* tgtDB) {
			bool flag = true;
			for (int i = 0; i < num_servers; i++) {
				sid_t doI_key_out = key_vpid_t(v, i, DoI_INDEX, (dir_t)OUT);
				sid_t doI_key_in = key_vpid_t(v, i, DoI_INDEX, (dir_t)IN);
				size_t len1 = 0, len2 = 0;
				 char* str1 = dump(doI_key_out, &len1);
				if (str1 != NULL) {
					flag = flag && tgtDB->restore(doI_key_out, str1, len1);
					free(str1);
				}
				if (!flag) {
					printf("Error, doI_key_out error! v = %ld doI_key_out = %ld\n", v, doI_key_out);
				}
				char* str2 = dump(doI_key_in, &len2);
				if (str2 != NULL) {
					flag = flag && tgtDB->restore(doI_key_in, str2, len2);
					free(str2);
				}
				if (!flag) {
					printf("Error, doI_key_in error! v = %ld doI_key_in = %ld\n", v, doI_key_in);
				}
			}
		
			return flag;
		}
		bool migrate_vcounter_weight(sid_t v , bool isTS, const char* hostname, int port) {
			bool flag = true;
			sid_t sc_key = key_vpid_t(v, 0, COUNTER, (dir_t)0);
			//printf("start reassign_vcounter_weight sc_key = %ld\n", sc_key);
			flag = flag && migrate(sc_key, hostname, port);

			if (!isTS) {
				sid_t ew_key = key_vpid_t(v, 0, EDGE_WEIGHT, (dir_t)0);
				//printf("start reassign_vcounter_weight ew_key = %ld\n", ew_key);
				flag = flag &&  migrate(ew_key, hostname, port);
			}
			if (!flag)
				printf("Error, reassign counter & weight error!\n");
			return flag;
		}

		vector<int> get_server_info(){
			redisReply* reply;
			reply = (redisReply*)redisCommand(contxt, "HMGET %s %d %d %d", 
					SERVER_INFO, WEIGHT, EDGENUM, OBJNUM);
			vector<int> results = get_array<int>(reply);
			if(results.size() == 0){
				printf("REDIS ERROR: HMGET");
			}
			return results;	
		}

		int reassign(sid_t v, bool isTS){
			//printf("start reassign  : v = %ld, isTs = %d\n", v, isTS);

			redisReply *reply;
			if(isTS)
				reply = (redisReply*)redisCommand(contxt, 
					"helios.reassign %ld ", v);
			else
				reply = (redisReply*)redisCommand(contxt, 
					"helios.reassign_counter %ld", v);
			if (reply == NULL) {
				printf("Error : reassign replay is NULL v = %ld isTs = %d \n",v,isTS);
			}

			assert(reply != NULL);
			if (reply->type == REDIS_REPLY_ERROR) {
				printf("ERROR reassign v = %ld isTs = %d, type : %s\n", v, isTS, reply->str);
			}
			freeReplyObject(reply);
			//printf("end reassign  : v = %ld, isTs = %d\n", v, isTS);
			return 1;
		}

		int receive(sid_t v, bool isTS){
			//printf("start receive  : v = %ld, isTs = %d\n", v, isTS);
			int ret = 1; //Ä¬ÈÏÎª1(true)
			redisReply *reply;
			if(isTS)
				reply = (redisReply*)redisCommand(contxt, "helios.receive %ld", v);
			else 
				reply = (redisReply*)redisCommand(contxt, "helios.receive_counter %ld", v);
			if (reply == NULL) {
				printf("Error : receive  : v = %ld, isTs = %d\n", v, isTS);
				ret = 0;
				//return 1;
			}
			assert(reply != NULL);
			if (reply->type == REDIS_REPLY_ERROR) {
				printf("receive v = %ld isTs = %d, ERR %s\n", v, isTS ,reply->str);
				ret = 0;
			}
			freeReplyObject(reply);
			return ret;
		}

		int reassign_lock(sid_t v){
			redisReply *reply;
			reply = (redisReply*)redisCommand(contxt, 
				"helios.reassign_lock %ld ", v);
			if (reply == NULL) {
				printf("Error :reassign_lock reply is NULL key = %ld\n", v);
				//return -1;
			}
			assert(reply != NULL);
			int suc = 0;
			if(reply->type == REDIS_REPLY_INTEGER){
				suc = reply->integer;
			}
			freeReplyObject(reply);
			return suc;
		}

		int set_v_loc(sid_t v, int loc){
			sid_t vl_key = key_vpid_t(v, 0, LOC, (dir_t)0);
			redisReply *reply;
			reply = (redisReply*)redisCommand(contxt, "SET %ld %d", vl_key, loc);
			if (reply == NULL) {
				printf("Error set_v_loc v = %d loc = %d\n", v, loc);
				//return 0;
			}
			assert(reply != NULL);
			freeReplyObject(reply);
			return 1;
		}

		int get_v_loc(sid_t v){
			sid_t vl_key = key_vpid_t(v, 0, LOC, (dir_t)0);
			redisReply *reply;
			reply = (redisReply*)redisCommand(contxt, "GET %ld", vl_key);
			if (reply == NULL) {
				printf("Error get_v_loc fail v = %ld\n", v);
			}
			assert(reply != NULL);
			int server = -1;
			if(reply->type == REDIS_REPLY_STRING)
				server = stringToNum<int>(reply->str);
			freeReplyObject(reply);
			return server;
		}
		
		int del_v_loc(sid_t v){
			sid_t vl_key = key_vpid_t(v, 0, LOC, (dir_t)0);
			redisReply *reply;
			reply = (redisReply*)redisCommand(contxt, "DEL %ld", vl_key);
			assert(reply != NULL);
			freeReplyObject(reply);
			return 1;
		}
		
		void write_command(const vector<string> &params){
			int argc = params.size();
			const char** command = (const char**)malloc(argc*sizeof(char*));
			for(int i = 0; i < argc; i++){
				command[i] = params[i].c_str();
			}

			redisReply *reply;
			reply = (redisReply*)redisCommandArgv(contxt, argc, command, NULL);
			if(reply->type == REDIS_REPLY_ERROR)
				printf("command ERR %s\n", reply->str);
			if(reply->type == REDIS_REPLY_INTEGER)
				printf("command integer result: %ld\n", reply->integer);
			if(reply->type == REDIS_REPLY_STRING)
				printf("command string result: %s\n", reply->str);
			if(reply->type == REDIS_REPLY_ARRAY)
				printf("command array result: %d\n", reply->elements);
			freeReplyObject(reply);

		}


};
#endif
