#include "AsyncRedisStore.hpp"
#include <unistd.h>
#include "QueryNode.hpp"
#include <omp.h>
#include <assert.h>
void AsyncRedisStore::get_p_counter( sid_t p, vector<int> *buf){
	//redisLibevAttach(EV_DEFAULT_ c);
	int executed = 1;
	async_pam *targ = new async_pam(buf, &executed);	

	sid_t pc_key = key_vpid_t(0, p, COUNTER,(dir_t)0);
	redisAsyncCommand(c, rediscommand_callback, targ, 
			"HMGET %ld %d %d", pc_key, OUT, IN);

	while(executed > 0){
		redisAsyncHandleWrite(c); 
      		redisAsyncHandleRead(c); 
      		usleep(1000);
	}
	//ev_loop(EV_DEFAULT_ 0);
}

//fuction : 获取节点边权值信息和边权值信息
void AsyncRedisStore::get_reassign_info( sid_t v, vector<int> *buf, bool isTS){
	//redisLibevAttach(EV_DEFAULT_ c);
	int executed = 1;
	async_pam *targ = new async_pam(buf, &executed);
	
	if(isTS)
		redisAsyncCommand(c, rediscommand_callback, targ, 
				"helios.get_reassign_info_ts %ld", v);
	else
		redisAsyncCommand(c, rediscommand_callback, targ, 
				"helios.get_reassign_info_cs %ld", v);
			
	while(executed > 0){
		redisAsyncHandleWrite(c); 
      		redisAsyncHandleRead(c); 
      		usleep(1000);
	}
	//ev_loop(EV_DEFAULT_ 0);
}

////////////////////////////////// counter db///////////////////////////////////////////////////
void AsyncRedisStore::batch_update_edgeweight(sid_t root_v, const vector<triple_t> &triples, int edgelog_len){
	int executed = 0, c_num = 0;
if(root_v == 0){	
	for(int i = 0; i < triples.size(); i++){
		triple_t t = triples[i];
		
		if(t.o != 0){
		//	printf("update edge weight %ld %ld", t.s, t.o);
			redisAsyncCommand(c, rediswrite_callback, &executed, 
				"helios.update_edgeweight %ld %ld %d", t.s, t.o, edgelog_len);
			c_num ++;
		}
		for(int j = 0; j< t.o_vals.size(); j++){
		//	printf("update edge weight %ld %ld", t.s, (t.o_vals)[j]);
			redisAsyncCommand(c, rediswrite_callback, &executed, 
				"helios.update_edgeweight %ld %ld %d", t.s, (t.o_vals)[j], edgelog_len);
			c_num++;
		}
		if(c_num > 50000){
			while(executed < c_num){
				redisAsyncHandleWrite(c); 
      				redisAsyncHandleRead(c); 
      				usleep(1000);
			}
			executed = 0; c_num = 0;
			continue;
		}
	}
}else {
	for(int i = 0; i < triples.size(); i++){
		triple_t t = triples[i];

		if(t.o != 0){
			redisAsyncCommand(c, rediswrite_callback, &executed, 
				"helios.update_edgeweight %ld %ld %d", root_v, t.o, edgelog_len);
			c_num ++;
		}
		for(int j = 0; j< t.o_vals.size(); j++){
			redisAsyncCommand(c, rediswrite_callback, &executed, 
				"helios.update_edgeweight %ld %ld %d", root_v, (t.o_vals)[j], edgelog_len);
			c_num++;
		}
		if(c_num > 50000){
			while(executed < c_num){
				redisAsyncHandleWrite(c); 
      				redisAsyncHandleRead(c); 
      				usleep(1000);
			}
			executed = 0; c_num = 0;
			continue;
		}
	}
}
	
	while(executed < c_num){
		redisAsyncHandleWrite(c); 
      		redisAsyncHandleRead(c); 
      		usleep(1000);
	}
}

void AsyncRedisStore::batch_update_vertexweight(const vector<sid_t> &vs, int incr_weight, int log_len){
	int executed = 0, c_num = 0;
	
	int server_weight = vs.size() * incr_weight;
	redisAsyncCommand(c, rediswrite_callback, &executed, "HINCRBY %s %d %d", SERVER_INFO, WEIGHT, server_weight);
	c_num ++;

	for(int i = 0; i < vs.size(); i++){
		sid_t s = vs[i];
		redisAsyncCommand(c,rediswrite_callback, &executed, "HINCRBY %ld %d %d", 
			key_vpid_t(s, 0, COUNTER, (dir_t)0), WEIGHT, incr_weight);

		c_num++;
		//if(log_len > 0){
			redisAsyncCommand(c,rediswrite_callback, &executed, "helios.append_activev %ld %d", s, log_len);
			c_num++;
		//}

		if(c_num > 50000){
			//cout << "cuurent c_num = " << c_num << "; current excuted = " << executed << endl;
			while(executed < c_num){
				redisAsyncHandleWrite(c); 
      				redisAsyncHandleRead(c); 
      				usleep(1000);
			}
			executed = 0; c_num = 0;
			continue;
		}
	}
	
	while(executed < c_num){
		redisAsyncHandleWrite(c); 
      		redisAsyncHandleRead(c); 
      		usleep(1000);
	}
}
//TODO may del
void AsyncRedisStore::batch_update_DoI_index(const vector<triple_t>& triples, int edgelog_len)
{
}

///////////////////////////////////////////////////////////////////

void AsyncRedisStore::write_command(const vector<const char*> &params){
	int argc = params.size();
	const char** command = (const char**)malloc(argc*sizeof(char*));
	for(int i = 0; i < argc; i++)
		command[i] = params[i];
	
	/*for(int i = 0; i < argc;  i++)
		printf("%s %d ", command[i], strlen(command[i]));*/
	redisAsyncCommandArgv(c, NULL, NULL, argc, command, NULL);
	redisAsyncHandleWrite(c);
}

void AsyncRedisStore::write_command(const vector<string> &params){
	int argc = params.size();
	const char** command = (const char**)malloc(argc*sizeof(char*));
	for(int i = 0; i < argc; i++){
		char* array = new char[params[i].length()+1];
		strcpy(array, params[i].c_str());
		command[i] = array;
	}

	redisAsyncCommandArgv(c, NULL, NULL, argc, command, NULL);
	redisAsyncHandleWrite(c);
}

/*void AsyncRedisStore::write_command(const char** command, int argc){
	printf("commands: %d ", argc);
	for(int i = 0; i < argc;  i++)
		printf("%s l", command[i]);
	redisAsyncCommandArgv(c, NULL, NULL, argc, command, NULL);
	redisAsyncHandleWrite(c);
}*/

void AsyncRedisStore::read_zset(string key, vector<sid_t> *results){
	//redisLibevAttach(EV_DEFAULT_ c);
	int executed = 1;
	readset_pam *targ = new readset_pam(results, &executed);	
	//redisAsyncCommand(c,read_zset_callback, targ, "ZRANGE %s %d %d", key, 0, -1);

	//printf("Read zset %s\n", key.c_str());
	redisAsyncCommand(c, read_zset_callback, targ, "SMEMBERS %s", key.c_str());
	while(executed > 0){
		redisAsyncHandleWrite(c); // This sends the command.
      		redisAsyncHandleRead(c); // This calls the callback if the reply has been received.
      		usleep(1000);
	}
	//ev_loop(EV_DEFAULT_ 0);
}

void AsyncRedisStore::read_zset(sid_t key, vector<sid_t> *results){
	//redisLibevAttach(EV_DEFAULT_ c);
	int executed = 1;
	readset_pam *targ = new readset_pam(results, &executed);
	redisAsyncCommand(c, read_zset_callback, targ, "ZRANGE %ld %d %d", key, 0, -1);
	while(executed > 0){
		redisAsyncHandleWrite(c); // This sends the command.
      		redisAsyncHandleRead(c); // This calls the callback if the reply has been received.
      		usleep(1000);
	}
	//ev_loop(EV_DEFAULT_ 0);
}

void AsyncRedisStore::read_set(const char* key, vector<sid_t> *results){
	//redisLibevAttach(EV_DEFAULT_ c);
	
	int executed = 1;
	readset_pam *targ = new readset_pam(results, &executed);
	
	redisAsyncCommand(c,read_strset_callback, targ, "GET %s", key);
	
	//ev_loop(EV_DEFAULT_ 0);
	while(executed > 0){
		redisAsyncHandleWrite(c); 
      		redisAsyncHandleRead(c); 
      		usleep(1000);
	}
}
void AsyncRedisStore::read_set(sid_t key, vector<sid_t> *results){
	//redisLibevAttach(EV_DEFAULT_ c);
	int executed = 1;
	readset_pam *targ = new readset_pam(results, &executed);
	//printf("set key %s\n",key);
	redisAsyncCommand(c,read_strset_callback, targ, "GET %ld", key);
	
	//ev_loop(EV_DEFAULT_ 0);
	while(executed > 0){
		redisAsyncHandleWrite(c); 
      		redisAsyncHandleRead(c); 
      		usleep(1000);
	}
}


void AsyncRedisStore::traverse_vertex(QueryNode* queryRoot, const vector<sid_t> &vs, QueryEdge* edge, 
		vector<triple_t> *results){
	
	int executed = (edge->bind_val).size() * vs.size();
	int c_num = 0;
	uint64_t start= get_usec();
	for(int k = 0; k < (edge->bind_val).size(); k++){
		sid_t p = (edge->bind_val)[k];
		for(int i = 0; i < vs.size(); i++){
			sid_t s = vs[i]; 
			int s_weight = 0;
			traverse_pam *targ = new traverse_pam(s,p,edge->d,&executed);
			targ->node = edge->node;
			if(edge->towardsJoint == 1)
				targ->node = edge->node;
			else 
				targ->node = NULL;
			
			targ->root = queryRoot; targ->results = results;
			sid_t key_t = key_vpid_t(s, p, NO_INDEX,edge->d);
			c_num++;
			//TODO 优化策略使用MGET命令
			redisAsyncCommand(c, traverse_callback, targ, "GET %ld", key_t);
		}
	}
	//printf("c_num %d %ldusec \n", c_num, get_usec()-start);
	//start = get_usec();
	if(c_num > 0){
		
		executed = c_num;	
		while(executed > 0){
			redisAsyncHandleWrite(c); 
			redisAsyncHandleRead(c); 
      			usleep(1000);
		}
	}
	//printf("command time :%ldusec \n", get_usec()-start);
}

void AsyncRedisStore::batch_traverse_vertex(QueryNode* queryRoot, const vector<sid_t>& vs, QueryEdge* edge,
	vector<triple_t>* results) {
	int batch_num = 1000;
	int executed = 0;
	int c_num = 0;
	uint64_t start = get_usec();
	for (int k = 0; k < (edge->bind_val).size(); k++) {
		sid_t p = (edge->bind_val)[k];
		for (int i = 0; i < vs.size(); i += batch_num) {
			//batch traverse node
			vector<sid_t> batch_s;
			string traverse_command = "MGET";

			for (int b = i; b < min((int)vs.size(), batch_num); b++) {
				sid_t s = vs[b];
				batch_s.push_back(s);
				sid_t key_t = key_vpid_t(s, p, NO_INDEX, edge->d);
				traverse_command.append(" " + to_string(key_t));
			}
			batch_traverse_pam* targ = new batch_traverse_pam(batch_s, p, edge->d, &executed);
			if (edge->towardsJoint == 1)
				targ->node = edge->node;
			else
				targ->node = NULL;
			targ->root = queryRoot; targ->results = results;
			c_num++;
			redisAsyncCommand(c, batch_traverse_callback, targ, traverse_command.c_str());
		}
	}
	//printf("c_num %d %ldusec \n", c_num, get_usec()-start);
	//start = get_usec();
	if (c_num > 0) {
		while (executed < c_num) {
			redisAsyncHandleWrite(c);
			redisAsyncHandleRead(c);
			usleep(1000);
		}
	}
}
void AsyncRedisStore::batch_insert_DoI_index(vector<map<sid_t, int>>& DoI_index) {
	int num = DoI_index.size();
	int executed = 0, c_num = 0;
	for (int i = 0; i < num; i++) {
		//string field = "node" + to_string(i);
		for (map<sid_t, int>::iterator iter = DoI_index[i].begin(); iter != DoI_index[i].end(); iter++) {
			sid_t doi_key = key_vpid_t(iter->first, 0, DoI_INDEX, (dir_t)0);
			redisAsyncCommand(c, rediswrite_callback, &executed, "HINCRBY %ld %d %d",
				doi_key, i, iter->second);
			c_num++;
			//if (c_num > 50000) {
			//	//cout << "current c_num = " << c_num << ";current excuted = " << executed << endl;
			//	while (executed < c_num) {
			//		redisAsyncHandleWrite(c);
			//		redisAsyncHandleRead(c);
			//		usleep(1000);
			//	}
			//	executed = 0; c_num = 0;
			//	continue;
			//}
		}
		/*while (executed < c_num) {
			redisAsyncHandleWrite(c);
			redisAsyncHandleRead(c);
			usleep(1000);
		}*/
	}
	//cout << "current c_num = " << c_num << ";current excuted = " << executed << endl;
	while (executed < c_num) {
		redisAsyncHandleWrite(c);
		redisAsyncHandleRead(c);
		usleep(1000);
	}
}

void AsyncRedisStore::batch_insert_DoI_index(map<sid_t, int>& DoI_index,int host) {
	int num = DoI_index.size();
	int executed = 0, c_num = 0;

	//string field = "node" + to_string(i);
	for (map<sid_t, int>::iterator iter = DoI_index.begin(); iter != DoI_index.end(); iter++) {
		sid_t doi_key = key_vpid_t(iter->first, 0, DoI_INDEX, (dir_t)0);
		redisAsyncCommand(c, rediswrite_callback, &executed, "HINCRBY %ld %d %d",
			doi_key, host, iter->second);
		c_num++;
		//
		//if (c_num > 50000) {
		//	//cout << "current c_num = " << c_num << ";current excuted = " << executed << endl;
		//	while (executed < c_num) {
		//		redisAsyncHandleWrite(c);
		//		redisAsyncHandleRead(c);
		//		usleep(1000);
		//	}
		//	executed = 0; c_num = 0;
		//	continue;
		//}
	}
		
	//cout << "current c_num = " << c_num << ";current excuted = " << executed << endl;
	while (executed < c_num) {
		redisAsyncHandleWrite(c);
		redisAsyncHandleRead(c);
		usleep(1000);
	}
}
//默认值为1
void AsyncRedisStore::batch_insert_DoI_index(vector<vector<sid_t>>& DoI_index) {
	int num = DoI_index.size();
	int executed = 0, c_num = 0;
	for (int i = 0; i < num; i++) {
		//string field = "node" + to_string(i);
		for (int obj : DoI_index[i]) {
			sid_t doi_key = key_vpid_t(obj, 0, DoI_INDEX, (dir_t)0);
			redisAsyncCommand(c, rediswrite_callback, &executed, "HINCRBY %ld %d %d",
				doi_key, i, 1);
			c_num++;
			/*if (c_num > 50000) {
				while (executed < c_num) {
					redisAsyncHandleWrite(c);
					redisAsyncHandleRead(c);
					usleep(1000);
				}
				executed = 0; c_num = 0;
				continue;
			}*/
		}
		/*while (executed < c_num) {
			redisAsyncHandleWrite(c);
			redisAsyncHandleRead(c);
			usleep(1000);
		}*/
	}
	while (executed < c_num) {
		redisAsyncHandleWrite(c);
		redisAsyncHandleRead(c);
		usleep(1000);
	}

}

//add read write test
void AsyncRedisStore::batch_read_test(const vector<sid_t>& vs, vector<int>* result){
	int executed = 0,c_num = 0;
	uint64_t start = get_usec();
	for (sid_t s : vs) {
		async_pam* targ = new async_pam(result, &executed);
		sid_t doi_key = key_vpid_t(s, 0, DoI_INDEX, (dir_t)0);
		c_num++;
		redisAsyncCommand(c, read_hashset_callback, targ, "HGET %ld %d", doi_key ,0);
		//添加下面的部分就很慢了？？
		/*if (c_num > 1000) {
			executed = c_num;
			while (executed > 0) {
				redisAsyncHandleWrite(c);
				redisAsyncHandleRead(c);
				usleep(1000);
			}
		}*/
	}
	if (c_num > 0) {
		executed = c_num;
		while (executed > 0) {
			redisAsyncHandleWrite(c);
			redisAsyncHandleRead(c);
			usleep(1000);
		}
	}
	printf("read_test command time :%ldusec \n", (get_usec() - start) / 1000000);
}

void AsyncRedisStore::batch_write_test(const vector<sid_t>& vs) {
	int executed = 0, c_num = 0;
	uint64_t start = get_usec();
	//string field = "node" + to_string(i);
	for (int obj : vs) {
		sid_t doi_key = key_vpid_t(obj, 0, DoI_INDEX, (dir_t)0);
		redisAsyncCommand(c, rediswrite_callback, &executed, "HINCRBY %ld %d %d",
			doi_key, 0, 1);
		c_num++;
		/*if (c_num > 1000) {
			executed = c_num;
			while (executed > 0) {
				redisAsyncHandleWrite(c);
				redisAsyncHandleRead(c);
				usleep(1000);
			}
		}*/
	}
		
	while (executed < c_num) {
		redisAsyncHandleWrite(c);
		redisAsyncHandleRead(c);
		usleep(1000);
	}
	printf("write_test command time :%ld sec \n", (get_usec() - start) / 1000000);
}

//
void AsyncRedisStore::batch_insert_DoI_index_new(vector<vector<pair<sid_t, sid_t>>> DoI_index,dir_t d) {
	int num = DoI_index.size();
	int executed = 0, c_num = 0;
	for (int i = 0; i < num; i++) {
		//string field = "node" + to_string(i);
		//printf("i = %d , DoI_index size = %d\n",i, DoI_index[i].size());
		for (pair<sid_t, sid_t> obj : DoI_index[i]) {
			sid_t doI_key = key_vpid_t(obj.first, i, DoI_INDEX, d);
			// 改为使用ZSET数据存储
			//首先向redis插入 obj.second [s,filed,o]
			//printf("v = %ld, i = %d, zset key = %ld, value = %ld\n",obj.first,i,doI_key, obj.second);
			redisAsyncCommand(c, rediswrite_callback, &executed, "ZADD %ld %ld %ld",
				doI_key,obj.second, obj.second);
			c_num++;
			if (c_num > 50000) {
				cout << "insert_DoI_index current c_num = " << c_num << ";current excuted = " << executed << endl;
				while (executed < c_num) {
					redisAsyncHandleWrite(c);
					redisAsyncHandleRead(c);
					usleep(1000);
				}
				executed = 0; c_num = 0;
				continue;
			}
		}

	}

	while (executed < c_num) {
		redisAsyncHandleWrite(c);
		redisAsyncHandleRead(c);
		usleep(1000);
	}
}


void AsyncRedisStore::del_keys(vector<sid_t> keys) {
	int n = keys.size();
	int executed = 0, c_num = 0;
	for (int i = 0; i < n; i++) {
		redisAsyncCommand(c, rediswrite_callback, &executed, "del %ld",
			keys[i]);
		c_num++;
	}
	while (executed < c_num) {
		redisAsyncHandleWrite(c);
		redisAsyncHandleRead(c);
		usleep(1000);
	}
}
void AsyncRedisStore::updateDoI_index(vector<sid_t> vs,int loc, int tgt, sid_t value) {
	int num = vs.size();
	int executed = 0, c_num = 0;
	for (int i = 0; i < num; i++) {
		//1. del value in vs[loc] 
		sid_t doI_key_out = key_vpid_t(vs[i], loc, DoI_INDEX, OUT);

		redisAsyncCommand(c, rediswrite_callback, &executed, "zrem %ld %ld",
			doI_key_out, value);
		c_num++;
		//2. insert value in vs[tgt]
		doI_key_out = key_vpid_t(vs[i], tgt, DoI_INDEX, OUT);
		redisAsyncCommand(c, rediswrite_callback, &executed, "zadd %ld %ld",
			doI_key_out, value);
		c_num++;
		if (c_num > 50000) {
			//cout << "current c_num = " << c_num << ";current excuted = " << executed << endl;
			while (executed < c_num) {
				redisAsyncHandleWrite(c);
				redisAsyncHandleRead(c);
				usleep(1000);
			}
			executed = 0; c_num = 0;
			continue;
		}
	}
	while (executed < c_num) {
		redisAsyncHandleWrite(c);
		redisAsyncHandleRead(c);
		usleep(1000);
	}
}
void AsyncRedisStore::get_DoI_List(sid_t v, int excuted_num, vector<int>* results) {
	int executed = 1;
	async_pam* targ = new async_pam(results, &executed);
	sid_t doi_key = key_vpid_t(v, 0, DoI_INDEX, (dir_t)0);
	string command_HMGET = "HMGET " + to_string(doi_key);
	for (int i = 0; i < excuted_num; i++) {
		command_HMGET += " " + to_string(i);
	}
	redisAsyncCommand(c, rediscommand_callback, targ, command_HMGET.c_str());

	while (executed > 0) {
		redisAsyncHandleWrite(c);
		redisAsyncHandleRead(c);
		//usleep(1000);
	}
}
void AsyncRedisStore::batch_get_DoI_List(vector<sid_t>& vs, int excuted_num, vector<int>* results) {
	int executed = vs.size();
	string baseCommand;
	for (int i = 0; i < excuted_num; i++) {
		baseCommand += " " + to_string(i);
	}
	for (sid_t v : vs) {
		sid_t doi_key = key_vpid_t(v, 0, DoI_INDEX, (dir_t)0);
		async_pam* targ = new async_pam(results, &executed);
		string command_HMGET = "HMGET " + to_string(doi_key) + baseCommand;
		redisAsyncCommand(c, rediscommand_callback, targ, command_HMGET.c_str());
	}
	while (executed > 0) {
		redisAsyncHandleWrite(c);
		redisAsyncHandleRead(c);
		usleep(1000);
	}
}


void AsyncRedisStore::get_DoI_List_new(sid_t v, int excuted_num, vector<int>* results) {

	int executed = excuted_num;
	//printf("get_DoI_List_new start v = %ld\n", v);
	async_pam* targ = new async_pam(results, &executed);
	//这里执行了三次
	for (int i = 0; i < excuted_num; i++) {
		sid_t doI_key = key_vpid_t(v, i, DoI_INDEX, (dir_t)OUT);
		//printf("DoI key = %ld\n", doI_key);
		redisAsyncCommand(c, read_zset_len_callback, targ, "ZCARD %ld", doI_key);
	}
	//这里only执行了两次后爆出异常
	while (executed >= 0) {
		redisAsyncHandleWrite(c); // This sends the command.
		redisAsyncHandleRead(c); // This calls the callback if the reply has been received.
		//usleep(1000);
	}
	//printf("get_DoI_List_new end v = %ld, result size = %d\n", v,results->size());

}
//abandon
int AsyncRedisStore::get_max_Di(sid_t val, sid_t sid, int num_servers) {
	
	int executed = num_servers;
	vector<int> buf;
	async_pam* targ = new async_pam(&buf, &executed);
	sid_t sc_key = key_vpid_t(val, 0, COUNTER, (dir_t)0);
	for (int i = 0; i < num_servers; i++) {
		//cout << "get_max_Di loop i = " << i << endl;
		string field = "node" + to_string(i);
		//cout << "DoI index sid =" << sid << ";sc_key = " << sc_key << ";  field = " << field.c_str() << endl;
		redisAsyncCommand(c, read_hashset_callback, targ, "HGET %ld %s",
			(long long)sc_key, field.c_str());
	}
	while (executed > 0) {
		redisAsyncHandleWrite(c);
		redisAsyncHandleRead(c);
		//usleep(1000);
	}
	//cout << "buf size = " << buf.size() << endl; //由于字段不存在时会返回NULL,导致 buf size != num_servers
	assert (buf.size() == num_servers);
	int maxNode = sid; //默认为本地节点
	int maxDi = buf[sid];
	for (int i = 0; i < num_servers; i++) {
		if (buf[i] > maxDi) {
			maxDi = buf[i];
			maxNode = i;
		}
	}
	return maxNode;
}

//存在性判断
bool AsyncRedisStore::isExist(sid_t s) {
	//RedisModule_Call(ctx, "HSET", "lll", (long long)sl_key, (long long)EXISTS, (long long)1);
	int executed = 1;
	vector<int> results;
	async_pam* targ = new async_pam(&results, &executed);
	sid_t sl_key = key_vpid_t(s, 0, LOCALITY, (dir_t)0);

	redisAsyncCommand(c, read_hashset_callback, targ, "HGET %ld %ld", (long long)sl_key, (long long)EXISTS);
	while (executed > 0) {
		redisAsyncHandleWrite(c); // This sends the command.
		redisAsyncHandleRead(c); // This calls the callback if the reply has been received.
		usleep(1000);
	}
	if (results.size() != 1) {
		printf("Error :isExist function result error\n");
	}
	assert(results.size() == 1);
	//printf("node = %d, sl_key = %ld, result = %d\n",s, sl_key, results[0]);
	return results[0]==1;
}
//return exist node 
vector<sid_t> AsyncRedisStore::batch_isExist(vector<sid_t>& vs) {
	int executed = vs.size();
	vector<int> results;
	for (int i = 0; i < vs.size(); i++) {
		async_pam* targ = new async_pam(&results, &executed);
		sid_t sl_key = key_vpid_t(vs[i], 0, LOCALITY, (dir_t)0);
		redisAsyncCommand(c, read_hashset_callback, targ, "HGET %ld %ld", (long long)sl_key, (long long)EXISTS);
	}
	
	while (executed > 0) {
		redisAsyncHandleWrite(c); // This sends the command.
		redisAsyncHandleRead(c); // This calls the callback if the reply has been received.
		usleep(1000);
	}
	assert(results.size() == vs.size());
	//printf("node = %d, sl_key = %ld, result = %d\n",s, sl_key, results[0]);
	vector<sid_t> ret;
	for (int i = 0; i < vs.size(); i++) {
		if (results[i] > 0) ret.push_back(vs[i]);
	}
	return ret;

}
//TODO ADD remove DoI index info
bool AsyncRedisStore::remove_DoI_index(vector<string> params) {
	bool flag = true;
	vector<int> results;
	int executed = 1;
	async_pam* targ = new async_pam(&results, &executed);

	int argc = params.size(); //一个命令 HMGET sc_key node0 node1 node2;
	string command;
	for (int i = 0; i < argc; i++) {
		command += params[i] + " ";
	}
	//const char** command = (const char**)malloc(argc * sizeof(char*));
	//for (int i = 0; i < argc; i++) {
	//	char* array = new char[params[i].length() + 1];
	//	command[i] = array;
	//}
	////执行命令
	//redisAsyncCommandArgv(c, del_hashset_callback, targ, argc, command, NULL);
	redisAsyncCommand(c, del_hashset_callback, targ, command.c_str());
	redisAsyncHandleWrite(c);
	redisAsyncHandleRead(c);
	//判断结果
	if (results.size() > 0) return results[0] >= 0;

	return false;

}

void AsyncRedisStore::batch_getloc(const vector<sid_t> &vs, map<int, vector<sid_t>> *v_locs){
	//redisLibevAttach(EV_DEFAULT_ c);
	int executed = vs.size();
	
	for(int i = 0; i < vs.size(); i++){
		sid_t vid = vs[i];
		getloc_pam *targ = new getloc_pam(v_locs, &executed, 
				vid, connectto_sid);
		
		sid_t vloc = key_vpid_t(vid, 0, LOC, (dir_t)0);
		redisAsyncCommand(c,batch_getloc_callback,targ,
					"GET %ld", vloc);
	}
	while(executed > 0){
		redisAsyncHandleWrite(c); 
      		redisAsyncHandleRead(c); 
      		usleep(1000);
	}
	//ev_loop(EV_DEFAULT_ 0);
}

void AsyncRedisStore::batch_getloc3(const vector<sid_t>& vs, map<sid_t,int>* v_locs) {
	//redisLibevAttach(EV_DEFAULT_ c);
	int executed = vs.size();

	for (int i = 0; i < vs.size(); i++) {
		sid_t vid = vs[i];
		getloc_pam3* targ = new getloc_pam3(v_locs, &executed,
			vid, connectto_sid);

		sid_t vloc = key_vpid_t(vid, 0, LOC, (dir_t)0);
		redisAsyncCommand(c, batch_getloc3_callback, targ,
			"GET %ld", vloc);
	}
	while (executed > 0) {
		redisAsyncHandleWrite(c);
		redisAsyncHandleRead(c);
		usleep(1000);
	}
	//ev_loop(EV_DEFAULT_ 0);
}
//性能优化策略使用MGET方法替代GET方法 key num batch = [10,100] 过大时性能会急剧降低
void AsyncRedisStore::batch_getloc2(const vector<sid_t>& vs, map<int, vector<sid_t>>* v_locs) {
	int executed = vs.size();
	int batchNum = 100;
	for (int i = 0; i < vs.size(); i+= batchNum) {
		vector<sid_t> vids;
		string command = "HMGET";
		for (int j = i; j < i + batchNum; j++) {
			sid_t vid = vs[i];
			sid_t vloc = key_vpid_t(vid, 0, LOC, (dir_t)0);
			command += " " + to_string(vloc);
			vids.push_back(vid);
		}
		sid_t vid = vs[i];
		getloc_pam2* targ = new getloc_pam2(v_locs, &executed,
			vids, connectto_sid);

		redisAsyncCommand(c, batch_getloc2_callback, targ,
			command.c_str());
	}
	while (executed > 0) {
		redisAsyncHandleWrite(c);
		redisAsyncHandleRead(c);
		usleep(1000);
	}
}

//增加函数
/**
***  loadPredicates
*** function : Return entities hosted on the current node that have at least incoming (dir = IN) or outgoing (dir = OUT) edge labeled as p
**/
void AsyncRedisStore::loadPredicates(sid_t s, dir_t d, vector<sid_t>* results) {
	int executed = 1;
	readset_pam* targ = new readset_pam(results, &executed);
	sid_t sd_key = key_vpid_t(s, 0, NO_INDEX, d);
	printf("loadPredicates  %d", sd_key);
	redisAsyncCommand(c, read_zset_callback, targ, "SMEMBERS %ld", sd_key);
	while (executed > 0) {
		redisAsyncHandleWrite(c); // This sends the command.
		redisAsyncHandleRead(c); // This calls the callback if the reply has been received.
		usleep(1000);
	}
}

// loadNeighbors
void AsyncRedisStore::loadNeighbors(sid_t s, sid_t p, dir_t d, vector<sid_t>* results) {
	int executed = 1;
	readset_pam* targ = new readset_pam(results, &executed);
	sid_t sp_key = key_vpid_t(s, p, NO_INDEX, d);
	redisAsyncCommand(c, read_strset_callback, targ, "GET %ld", sp_key);
	while (executed > 0) {
		redisAsyncHandleWrite(c); // This sends the command.
		redisAsyncHandleRead(c); // This calls the callback if the reply has been received.
		usleep(1000);
	}
}
// loadEntities s_set
void AsyncRedisStore::loadEntities(sid_t p, dir_t d, vector<sid_t>* results) {
	sid_t pd_key = key_vpid_t(0, p, INDEX, d);
	int executed = 1;
	readset_pam* targ = new readset_pam(results, &executed);
	printf("loadPredicates  %d", pd_key);
	redisAsyncCommand(c, read_zset_callback, targ, "SMEMBERS %ld", pd_key);
	while (executed > 0) {
		redisAsyncHandleWrite(c); // This sends the command.
		redisAsyncHandleRead(c); // This calls the callback if the reply has been received.
		usleep(1000);
	}
}

void AsyncRedisStore::loadEntitiesSet(vector<sid_t> pset, dir_t d, vector<sid_t>* results) {
	int executed = pset.size();
	for (int i = 0; i < pset.size(); i++) {
		readset_pam* targ = new readset_pam(results, &executed);
		sid_t pd_key = key_vpid_t(0, pset[i], INDEX, d);
		redisAsyncCommand(c, read_zset_callback, targ, "SMEMBERS %ld", pd_key);
	}
	while (executed > 0) {
		redisAsyncHandleWrite(c);
		redisAsyncHandleRead(c);
		usleep(1000);
	}
}

// loadNodes



