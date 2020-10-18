#include "AsyncCallBack.hpp"
#include "QueryNode.hpp"
#include <set>
void connectCallback(const redisAsyncContext *c, int status){
	if(status != REDIS_OK){
		printf("ERR: %s\n", c->errstr);
		return;
	}
	printf("Redis Connected...\n");
}

void disconnectCallback(const redisAsyncContext *c, int status){
	if(status != REDIS_OK){
		printf("ERR: %s\n", c->errstr);
		return;
	}
	printf("Disconnected...\n");
	//aeStop(loop);
}

void rediswrite_callback(redisAsyncContext *c, void *r, void *privdata){
	redisReply *reply = (redisReply*)r;
	int* executed = (int*)privdata;
	if(reply == NULL){
		printf("ERR - rediscommand_callback async reply NULL \n");
	}
	if (reply->type == REDIS_REPLY_ERROR) {
		printf("ERR - rediscommand_callback async reply error \n");
	}
	//printf("reply->type = %d\n", reply->type);
	(*executed)++;
}

void rediscommand_callback(redisAsyncContext *c, void *r, void *privdata){
	redisReply *reply = (redisReply*)r;
	
	async_pam *targ = (async_pam*)privdata;
	vector<int>* buf = targ->buf;
	int * executed = targ->executed;
	delete targ;

	if(reply == NULL){
		printf("ERR - rediscommand_callback async reply NULL \n");
		goto end;
	}
	if(reply->type == REDIS_REPLY_ARRAY){
		//assert(reply->elements == 2 || reply->elements == 10);
		for(int i = 0; i<reply->elements; i++){
			if(reply->element[i]->type == REDIS_REPLY_STRING){
				//printf("buff str %s ", reply->element[i]->str);
				buf->push_back(stringToNum<int>(reply->element[i]->str));
			}
			else if(reply->element[i]->type == REDIS_REPLY_INTEGER){
				//printf("buff str %s ", reply->element[i]->integer);
				buf->push_back(reply->element[i]->integer);
			}
			else {
				//printf("error ele type %d \n", reply->element[i]->type);
				buf->push_back(0); }
		}	
	}else {
		printf("ERR - rediscommand_callback async reply Type is %d \n", reply->type);
		buf->push_back(-1);
	}
	goto end;
end:
	(*executed)--;	
	//if((*executed) <= 0) ev_break(EV_DEFAULT_ EVBREAK_ONE);
}

//格式为 HGET key field
void read_hashset_callback(redisAsyncContext* c, void* r, void* privdata) {
	//printf("call fuction read_hashset_callback \n");

	redisReply* reply = (redisReply*)r;
	async_pam* targ = (async_pam*)privdata;
	vector<int>* buf = targ->buf;
	int* executed = targ->executed;
	delete targ;

	if (reply == NULL) {
		printf("ERR - read_hashset_callback async reply NULL \n");
		buf->push_back(0);
		goto end;
	}
	if(reply->type == REDIS_REPLY_STRING) {
		//printf("read_hashset_callback data type is String \n");
		buf->push_back(stringToNum<int>(reply->str));
	}else if(reply->type == REDIS_REPLY_INTEGER){
		//printf("read_hashset_callback val = %d \n",reply->integer);
		buf->push_back(reply->integer);
	}
	else {
		//printf("read_hashset_callback return nil\n");
		buf->push_back(0);
	}
	goto end;
end:
	(*executed)--;
}

//执行 HDEL key field
void del_hashset_callback(redisAsyncContext* c, void* r, void* privdata) {
	//返回值为int类型
	redisReply* reply = (redisReply*)r;
	async_pam* targ = (async_pam*)privdata;
	vector<int>* buf = targ->buf;
	int* executed = targ->executed;
	delete targ;
	if (reply == NULL) {
		printf("ERR - del_hashset_callback async reply NULL \n");
		//buf->push_back(0);
		goto end;
	}
	if (reply->type == REDIS_REPLY_STRING) {
		buf->push_back(stringToNum<int>(reply->str));
	}
	else if (reply->type == REDIS_REPLY_INTEGER) {
		buf->push_back(reply->integer);
	}
	goto end;
end:
	(*executed)--;

}


void read_zset_callback(redisAsyncContext *c, void *r, void *privdata){
	readset_pam *targ = (readset_pam*)privdata;
	vector<sid_t> *results = targ->results;
	int * executed = targ->executed;
	delete targ;

	redisReply *reply = (redisReply*)r;
	
	if(reply == NULL){
		printf("ERROR - async reply NULL in read_zset\n");
		goto end;
	}

	if(reply->type != REDIS_REPLY_ARRAY){
		printf("EMPTY - read_zset - %s\n", reply->str);
		goto end;
	}
	//printf("reply size %d\n", reply->elements); //TODO add 查看打印结果测试
	for(int i = 0; i<reply->elements; i++){
		redisReply* r_ele = reply->element[i];

		if (r_ele->type == REDIS_REPLY_STRING) {
			results->push_back(stringToNum<int>(r_ele->str));
		}
		else if (r_ele->type == REDIS_REPLY_INTEGER) {
			results->push_back(r_ele->integer);
		}
		else {
			printf("ERROR- async zset type not string or integer!\n");
			continue;
		}
		//results->push_back(stringToNum<sid_t>(r_ele->str));
	}
	goto end;
end:
	(*executed)--;
	//if((*executed) <= 0) ev_break(EV_DEFAULT_ EVBREAK_ONE);
}

//add read_zset_len
void read_zset_len_callback(redisAsyncContext* c, void* r, void* privdata) {

	async_pam* targ = (async_pam*)privdata;
	vector<int>* results = targ->buf;
	int* executed = targ->executed;
	printf("start in read_zset_len_callback excuted = %d\n", *executed);

	delete targ;
	redisReply* reply = (redisReply*)r;

	if (reply == NULL) {
		printf("ERROR - async reply NULL in read_zset\n");
		goto end;
	}
	if (reply->type == REDIS_REPLY_STRING) {
		printf("res str = %s\n",reply->str);
		results->push_back(stringToNum<int>(reply->str));
	}
	else if (reply->type == REDIS_REPLY_INTEGER) {
		printf("res integer = %d\n", reply->integer);
		results->push_back(reply->integer);
	}
	else {
		results->push_back(-1); //
		printf("ERROR- async zset type not string or integer! type = %d\n", reply->type);
	}
	goto end;
end:
	(*executed)--;
	//if((*executed) <= 0) ev_break(EV_DEFAULT_ EVBREAK_ONE);
}
void read_strset_callback(redisAsyncContext *c, void *r, void *privdata){
	readset_pam *targ = (readset_pam*)privdata;
	vector<sid_t> *results = targ->results;
	int * executed = targ->executed;
	delete targ;

	redisReply *reply = (redisReply*)r;
	
	if(reply == NULL){
		printf("ERR - async reply NULL in read_strset\n");
		goto end;
	}

	if(reply->type != REDIS_REPLY_STRING){
		//printf("Empty- read_strset - %s\n", reply->str);
		goto end;
	}
	deserial_vector(reply->str, reply->len, results);
	goto end;
end:
	(*executed)--;
	//if((*executed) <= 0)  ev_break(EV_DEFAULT_ EVBREAK_ONE);
}

void traverse_callback(redisAsyncContext *c, void *r, void *privdata){
	traverse_pam*targ = (traverse_pam*)privdata;
	triple_t rt(targ->s, targ->p, targ->d);
	int * executed = targ->executed;
	QueryNode* node = targ->node;
	QueryNode* root = targ->root;
	vector<triple_t> *results = targ->results; 
	delete targ;
	std::set<sid_t> set_vals;
	std::vector<sid_t> o_vals;
	redisReply *reply = (redisReply*)r;
	
	if(reply == NULL){
		printf("ERR - async reply NULL in vertex traversal\n");
		goto end;
	}

	if(reply->type != REDIS_REPLY_STRING){
		//printf("EMPTY - vertex traversal - %s s=%d, p = %d\n", reply->str, rt.s, rt.p);
		root->insert_prune_bind(rt.s);
		goto end;
	}
	
	//o_vals = deserial_vector(reply->str, reply->len);
	rt.o_vals = deserial_vector(reply->str, reply->len);
	//if (node->bind_val.size() > 0) {
	//	set_vals.clear();
	//	set_vals.insert(o_vals.begin(), o_vals.end());
	//	//o_vals.assign(set_vals.begin(), set_vals.end());
	//	o_vals.clear();
	//	for (sid_t o : node->bind_val) {
	//		if (binary_search(set_vals.begin(), set_vals.end(), o)) {
	//			o_vals.push_back(o);
	//		}
	//	}
	//}

	results->push_back(rt);
	
	goto end;
end:
	(*executed)--;
	//if((*executed) <= 0)ev_break(EV_DEFAULT_ EVBREAK_ONE);
}

void batch_traverse_callback(redisAsyncContext* c, void* r, void* privdata) {
	batch_traverse_pam* targ = (batch_traverse_pam*)privdata;
	vector<sid_t> vs = targ->s; //here is deep copy or other
	sid_t p = targ->p;
	dir_t d = targ->d;
	int* executed = targ->executed;
	//QueryNode* node = targ->node;
	QueryNode* root = targ->root;
	vector<triple_t>* results = targ->results;
	delete targ;
	redisReply* reply = (redisReply*)r;

	if (reply == NULL || reply->type != REDIS_REPLY_ARRAY) {
		printf("ERR - async reply NULL in vertex traversal\n");
		goto end;
	}
	for (int i = 0; i < reply->elements; i++) {
		if (reply->element[i]->type == REDIS_REPLY_STRING) {
			triple_t rt(vs[i], p, d);
			rt.o_vals = deserial_vector(reply->element[i]->str, reply->element[i]->len);
			results->push_back(rt);
		}
		else {
			root->insert_prune_bind(vs[i]);
		}
	}
	goto end;
end:
	(*executed)++;
}
/*
 * getloc_reqlock_callback
 * batch_getloc_callback
 * */
void batch_getloc_callback(redisAsyncContext *c, void *r, void *privdata){
	getloc_pam *targ = (getloc_pam*)privdata;
	map<int, vector<sid_t>> *v_locs = targ->v_locs;
	int *executed = targ->executed;
	int vid = targ->vid;
	int connectto_sid = targ->connectto_sid;
	
	redisReply *reply = (redisReply*)r;
	if(reply == NULL){
		printf("ERR - async reply NULL in pattern evaluate\n");
		goto end;
	}

	int loc;
	if(reply->type == REDIS_REPLY_STRING)
		loc = stringToNum<int>(reply->str);
	else if(reply->type == REDIS_REPLY_INTEGER){
		loc = reply->integer;
		if(loc == -1)
			loc = connectto_sid;
	}else if (reply->type == REDIS_REPLY_NIL){
		loc = connectto_sid;
	}
	if(v_locs->find(loc) == v_locs->end()){
		v_locs->insert(pair<int,vector<sid_t>>(loc, vector<sid_t>()));
	}
	(*v_locs)[loc].push_back(vid);
	goto end;
end:
	delete targ;
	(*executed)--;	
	//if((*executed) <= 0) ev_break(EV_DEFAULT_ EVBREAK_ONE);
}


void batch_getloc2_callback(redisAsyncContext* c, void* r, void* privdata) {
	getloc_pam2* targ = (getloc_pam2*)privdata;
	map<int, vector<sid_t>>* v_locs = targ->v_locs;
	int* executed = targ->executed;
	vector<sid_t> vids = targ->vids;
	int connectto_sid = targ->connectto_sid;

	redisReply* reply = (redisReply*)r;
	if (reply == NULL) {
		printf("ERR - async reply NULL in pattern evaluate\n");
		goto end;
	}

	if (reply->type == REDIS_REPLY_ARRAY) {
		int loc;
		for (int i = 0; i < reply->elements; i++) {
			if (reply->element[i]->type == REDIS_REPLY_STRING) {
				loc = stringToNum<int>(reply->element[i]->str);
			}
			else if (reply->element[i]->type == REDIS_REPLY_INTEGER) {
				loc = reply->element[i]->integer;
				if (loc == -1)
					loc = connectto_sid;
			}
			else {
				loc = connectto_sid;
			}
			if (v_locs->find(loc) == v_locs->end()) {
				v_locs->insert(pair<int, vector<sid_t>>(loc, vector<sid_t>()));
			}
			(*v_locs)[loc].push_back(vids[i]);
		}
	}
	else {
		//Type Not Fit
		printf("ERR - async reply type err %d\n",reply->type);
	}
	goto end;
end:
	delete targ;
	(*executed)-= vids.size();
}


void batch_getloc3_callback(redisAsyncContext* c, void* r, void* privdata) {
	getloc_pam3* targ = (getloc_pam3*)privdata;
	map<sid_t, int>* v_locs = targ->v_locs;
	int* executed = targ->executed;
	int vid = targ->vid;
	int connectto_sid = targ->connectto_sid;

	redisReply* reply = (redisReply*)r;
	if (reply == NULL) {
		printf("ERR - async reply NULL in pattern evaluate\n");
		goto end;
	}

	int loc;
	if (reply->type == REDIS_REPLY_STRING)
		loc = stringToNum<int>(reply->str);
	else if (reply->type == REDIS_REPLY_INTEGER) {
		loc = reply->integer;
		if (loc == -1)
			loc = connectto_sid;
	}
	else if (reply->type == REDIS_REPLY_NIL) {
		loc = connectto_sid;
	}
	//if (v_locs->find(loc) == v_locs->end()) {
	//	v_locs->insert(pair<int, vector<sid_t>>(loc, vector<sid_t>()));
	//}
	(*v_locs)[vid] = loc;
	//(*v_locs)[loc].push_back(vid);
	goto end;
end:
	delete targ;
	(*executed)--;
	//if((*executed) <= 0) ev_break(EV_DEFAULT_ EVBREAK_ONE);
}


