#include "redis-module-helios/redismodule.h"
#include <string.h>
#include <stdlib.h>
#include <stdio.h>

#include "helios_structure.h"
//#include "helios_processor.h"
//#include "query_executor.h"
#include "data_operator.h"
#include "redis-module-helios/rmutil/vector.h"
#include "redis-module-helios/rmutil/util.h"
#include "utils/type.h"
#include "utils/HashFunction.h"
#include "pthread.h"
/*
 * input:
 * 	helios.load_dict dictfile num_servers sid 
 * output:
 * 	str - OK
 * */
/*int Helios_Load_Dict(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
	RedisModule_AutoMemory(ctx);
	if(argc < 4){
		return RedisModule_WrongArity(ctx);
	}
	
	const char* dictfile;
	long long num_servers;
	long long sid;
	RMUtil_ParseArgs(argv, argc, 1, "cll", &dictfile, &num_servers, &sid);

	FILE * fp;
	fp = fopen(dictfile, "r");
	if(fp == NULL)
	{
		return RedisModule_ReplyWithError(ctx, "-ERR: DICT FILE NOT FOUND.");
	}

	char str[100];
	int id, r;
	while((r = fscanf(fp, "%s %d\n", &str, &id)) != EOF){
		if(sid == jumpConsistentHash(id, num_servers)){
			helios_StringSetStr(ctx, id, str);
		}
		//char hash_str[80];
		//sprintf(hash_str, "%s%d", "str",str_hash);
		sid_t str_hash = hash64(str,0);
		if(sid == jumpConsistentHash(str_hash, num_servers)){
			RedisModule_Call(ctx, "SET", "cl", str, (long long)id);
	//		helios_StringSet(ctx, str_hash, id);
		}
	}
	fclose(fp);
	RedisModule_ReplyWithSimpleString(ctx, "OK1");
	return REDISMODULE_OK;
}*/

/*
 * input:
 * 	helios.load_data datafile num_servers sid 
 * output:
 * 	str - OK
 * */
/*int Helios_Load_Data(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
	RedisModule_AutoMemory(ctx);
	if(argc < 4){
		return RedisModule_WrongArity(ctx);
	}
	const char* datafile;
	long long num_servers;
	long long sid;
	RMUtil_ParseArgs(argv, argc, 1, "cll", &datafile, &num_servers, &sid);

	FILE * fp;
	fp = fopen(datafile, "r");
	if(fp == NULL)
	{
		return RedisModule_ReplyWithError(ctx, "-ERR: DATA FILE NOT FOUND.");
	}

	sid_t s, p, o;
	int r;
	while((r = fscanf(fp, "%d %d %d\n", (int*)&s, (int*)&p, (int*)&o)) != EOF){
		if(sid == jumpConsistentHash(s, num_servers)){
			RedisModule_Log(ctx, "verbose", "insert %d %d %d %d %d\n", (int)s, (int)p, (int)o, (int)OUT, (int)sid);
			_insert_triple(ctx, s, p, o, OUT, num_servers, sid);
		}

		if(sid == jumpConsistentHash(o, num_servers)){
			RedisModule_Log(ctx, "verbose", "insert %d %d %d %d %d\n",(int) o, (int)p, (int)s, (int)IN, (int)sid);
			_insert_triple(ctx, o, p, s, IN, num_servers, sid);
		}
	}
	fclose(fp);
	RedisModule_ReplyWithSimpleString(ctx, "OK");
	return REDISMODULE_OK;
}*/

/*
 * input:
 * 	helios.insert_triple s p o d
 * output:
 * 	str - OK
 * */
int Helios_Insert_Triple(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
	RedisModule_AutoMemory(ctx);
	if(argc < 5){
		return RedisModule_WrongArity(ctx);
	}
	long long s, p, o, d, num_servers, sid;
	RMUtil_ParseArgs(argv, argc, 1, "llll", &s, &p, &o, &d, &num_servers, &sid);
	//printf("start in Helios_Insert_Triple\n");
	_insert_triple(ctx, (sid_t)s, (sid_t)p, (sid_t)o, (dir_t)d, (int)num_servers, (int)sid);
	RedisModule_ReplyWithSimpleString(ctx, "OK");
	RedisModule_Log(ctx, "warning", "insert %d %d %d %d %d\n", (int)s, (int)p, (int)o, (int)OUT, (int)sid);
	return REDISMODULE_OK;
}

/*
 * input:
 * 	helios.get_reassign_info_ts v_id
 * output:
 *	array - [server_objNum, server_edgeNum, 
 *	obj_exists, obj_top_locality, 
 *	obj_edgeNum]
 *	reply->elements = 5
 * */
int Helios_Get_ReassignInfo_TS(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
	RedisModule_AutoMemory(ctx);
	if(argc < 2)
		return RedisModule_WrongArity(ctx);
	long long v;
	RMUtil_ParseArgs(argv, argc, 1, "l", &v);

	int arraylen = 0;
	RedisModuleCallReply *reply;

	RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
	
	reply = RedisModule_Call(ctx, "HMGET", "cll", SERVER_INFO, (long long)OBJNUM, 
			(long long)EDGENUM);
	if(RedisModule_CallReplyType(reply) == REDISMODULE_REPLY_ARRAY){
		long long items = RedisModule_CallReplyLength(reply);
		if(items != 2) goto fail;
		RedisModuleCallReply *item;
		for(int i = 0; i < items; i++){
			item = RedisModule_CallReplyArrayElement(reply, i);
			RedisModule_ReplyWithCallReply(ctx, item);
			RedisModule_FreeCallReply(item);
			arraylen++;
		}
	}
	RedisModule_FreeCallReply(reply);
	
	sid_t sc_key = key_vpid_t(v, 0, COUNTER, (dir_t)0);
	sid_t sl_key = key_vpid_t(v, 0, LOCALITY, (dir_t)0);
	reply = RedisModule_Call(ctx, "HMGET", "lll", (long long)sl_key, (long long)EXISTS,
			(long long)TOP);
	if(RedisModule_CallReplyType(reply) == REDISMODULE_REPLY_ARRAY){
		long long items = RedisModule_CallReplyLength(reply);
		if(items != 2) goto fail;
		RedisModuleCallReply *item;
		for(int i = 0; i < items; i++){
			item = RedisModule_CallReplyArrayElement(reply, i);
			RedisModule_ReplyWithCallReply(ctx, item);
			RedisModule_FreeCallReply(item);
			arraylen++;
		}
	}
	RedisModule_FreeCallReply(reply);
	
	reply = RedisModule_Call(ctx, "HGET", "ll", (long long)sc_key, (long long)EDGENUM);
	RedisModule_ReplyWithCallReply(ctx, reply);
	arraylen++;
	RedisModule_FreeCallReply(reply);

	RedisModule_ReplySetArrayLength(ctx, arraylen);
	return REDISMODULE_OK;
fail:
	RedisModule_FreeCallReply(reply);
	return RedisModule_ReplyWithError(ctx, "-ERR: get REASSIGN INFO");
}
/*
 * input:
 * 	helios.get_reassign_info_cs v_id
 * output:
 *	array - [server_weight, server_active_objnum, 
 *	 obj_workload_locality, 
 *	obj_weight, obj_prev_reassign_weight]
 *	reply->elements = 5 
 * */
int Helios_Get_ReassignInfo_CS(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
	RedisModule_AutoMemory(ctx);
	if(argc < 2)
		return RedisModule_WrongArity(ctx);
	long long v;
	RMUtil_ParseArgs(argv, argc, 1, "l", &v);

	int arraylen = 0;
	RedisModuleCallReply *reply;

	RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
	
	reply = RedisModule_Call(ctx, "HGET", "cl", SERVER_INFO, (long long)WEIGHT);
	RedisModule_ReplyWithCallReply(ctx, reply);
	arraylen++;
	RedisModule_FreeCallReply(reply);
	
	reply= RedisModule_Call(ctx, "LLEN", "c", ACTIVE_OBJECT);
	RedisModule_ReplyWithCallReply(ctx, reply);
	arraylen++;
	RedisModule_FreeCallReply(reply);

	sid_t sc_key = key_vpid_t(v, 0, COUNTER, (dir_t)0);
	sid_t sl_key = key_vpid_t(v, 0, LOCALITY, (dir_t)0);
	reply = RedisModule_Call(ctx, "HGET", "ll", (long long)sl_key,  (long long)WORKLOAD);
	RedisModule_ReplyWithCallReply(ctx, reply);
	arraylen++;
	RedisModule_FreeCallReply(reply);
	
	reply = RedisModule_Call(ctx, "HMGET", "lll", (long long)sc_key, 
			(long long)WEIGHT, (long long)PREV_REASSIGN_WEIGHT);
	if(RedisModule_CallReplyType(reply) == REDISMODULE_REPLY_ARRAY){
		long long items = RedisModule_CallReplyLength(reply);
		if(items != 2) goto fail;
		RedisModuleCallReply *item;
		for(int i = 0; i < items; i++){
			item = RedisModule_CallReplyArrayElement(reply, i);
			RedisModule_ReplyWithCallReply(ctx, item);
			RedisModule_FreeCallReply(item);
			arraylen++;
		}
	}
	RedisModule_FreeCallReply(reply);

	RedisModule_ReplySetArrayLength(ctx, arraylen);
	return REDISMODULE_OK;
fail:
	RedisModule_FreeCallReply(reply);
	return RedisModule_ReplyWithError(ctx, "-ERR: get REASSIGN INFO");
}

/* helios.find_vcounter_ts vid
 * */
int Helios_Find_Vcounter_TS(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
	RedisModule_AutoMemory(ctx);
	if(argc < 2)
		return RedisModule_WrongArity(ctx);
	long long v;
	RMUtil_ParseArgs(argv, argc, 1, "l", &v);

	int arraylen = 0;
	RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
	sid_t sl_key = key_vpid_t(v, 0, LOCALITY, (dir_t)0);
	RedisModuleCallReply *reply = RedisModule_Call(ctx, "HMGET", "lll", (long long)sl_key, (long long)EXISTS,
			(long long)TOP);
	RedisModule_ReplyWithCallReply(ctx, reply);
	RedisModule_FreeCallReply(reply);
	arraylen++;

	sid_t sc_key = key_vpid_t(v, 0, COUNTER, (dir_t)0);
	reply = RedisModule_Call(ctx, "HGET", "ll", (long long)sc_key, (long long)EDGENUM);
	RedisModule_ReplyWithCallReply(ctx, reply);
	RedisModule_FreeCallReply(reply);
	arraylen++;
	RedisModule_ReplySetArrayLength(ctx, arraylen);
	return REDISMODULE_OK;
}

/* helios.find_vcounter_cs vid
 * */
int Helios_Find_Vcounter_CS(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
	RedisModule_AutoMemory(ctx);
	if(argc < 2)
		return RedisModule_WrongArity(ctx);
	long long v;
	RMUtil_ParseArgs(argv, argc, 1, "l", &v);

	int arraylen = 0;
	RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
	sid_t sl_key = key_vpid_t(v, 0, LOCALITY, (dir_t)0);
	RedisModuleCallReply *reply = RedisModule_Call(ctx, "HGET", "ll", (long long)sl_key, (long long)WORKLOAD);
	RedisModule_ReplyWithCallReply(ctx, reply);
	RedisModule_FreeCallReply(reply);
	arraylen++;

	sid_t sc_key = key_vpid_t(v, 0, COUNTER, (dir_t)0);
	reply = RedisModule_Call(ctx, "HMGET", "lll", (long long)sc_key, 
			(long long)WEIGHT, (long long)PREV_REASSIGN_WEIGHT);
	RedisModule_ReplyWithCallReply(ctx, reply);
	RedisModule_FreeCallReply(reply);
	arraylen++;
	RedisModule_ReplySetArrayLength(ctx, arraylen);
	return REDISMODULE_OK;
}

/*
 * helios.find_key vid pid index_t dir 
 * */
int Helios_Find_Key(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
	RedisModule_AutoMemory(ctx);
	if(argc < 5)
		return RedisModule_WrongArity(ctx);
	long long v, p, i, d, type;
	RMUtil_ParseArgs(argv, argc, 1, "lllll", &v, &p, &i, &d, &type);
	sid_t key = key_vpid_t(v, p, i, d);
	
	RedisModule_ReplyWithLongLong(ctx, key);	
	return REDISMODULE_OK;
}

/*
 * helios.find_edgeweight vid
 * */
int Helios_Find_EdgeWei(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
	RedisModule_AutoMemory(ctx);
	if(argc < 2)
		return RedisModule_WrongArity(ctx);
	long long v;
	RMUtil_ParseArgs(argv, argc, 1, "l", &v);
	sid_t ew_key = key_vpid_t(v, 0, EDGE_WEIGHT, (dir_t)0);
	RedisModuleCallReply *reply = RedisModule_Call(ctx, "HMGETALL", 
			"l", (long long)ew_key);
	RedisModule_ReplyWithCallReply(ctx, reply);	
	RedisModule_FreeCallReply(reply);
	return REDISMODULE_OK;
}


/*
 * input:
 * 	helios.pattern_evaluation num_t [s_t p_t o_t d joint_point]*num_t num_b ...
 * output:
 * 	array - [triple0, triple1, ...]
 * */
/*int Helios_Pattern_Evaluate(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
	RedisModule_AutoMemory(ctx);
	
	if(argc < 8){ // at least 8 aruguments
		return RedisModule_WrongArity(ctx);
	}
	//printf("pattern evalute");
	RedisModule_Log(ctx, "warning", "%s", "query evaluate");
	Node* query_root = parse_query_tree(ctx, argv, argc);
	printNode(ctx, query_root, 0);
	
	if(query_root == NULL)
		return RedisModule_ReplyWithError(ctx, "-ERR: query syntax error");
	
	int flag = pattern_evaluation(ctx, query_root);
	FreeNode(ctx, query_root);
	if(flag)
		return REDISMODULE_OK;
	else{
		RedisModule_ReplyWithSimpleString(ctx, "ERR");
		return REDISMODULE_OK;
	}*/
	/*pthread_t tid;

	RedisModuleBlockedClient *bc = RedisModule_BlockClient(ctx,NULL,NULL,NULL,0);
	void **targ = RedisModule_Alloc(sizeof(void*)*2);
	targ[0] = bc;
	targ[1] = query_root;

	if(pthread_create(&tid,NULL,Helios_Pattern_Evaluate_Thread, targ) != 0){
		RedisModule_AbortBlock(bc);
		return RedisModule_ReplyWithError(ctx, "-ERR Can't start reassign_thread");
	}
	return REDISMODULE_OK;*/
/*}*/
/*
 * input:
 * 	helios.find_v v_id [dir] [predicate]
 * output:
 * 	array - [triple0, triple1, ...]
 * */
int Helios_Find_Vertex(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
	RedisModule_AutoMemory(ctx);
	
	if(argc != 2 && argc != 3 && argc != 4){ 
		return RedisModule_WrongArity(ctx);
	}
	long long v;
	RMUtil_ParseArgs(argv, argc, 1, "l", (long long *)&v);
	
	RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
	int arraylen = 0;
	if(argc == 2)
	{
		helios_find_v(ctx, (sid_t)v, &arraylen);
	}
	else if(argc == 3){
		long long d;
		RMUtil_ParseArgs(argv, argc, 2, "l", &d);
		helios_find_vd(ctx, (sid_t)v, (dir_t)d, &arraylen);
	}
	else if(argc == 4){
		long long d, p;
		RMUtil_ParseArgs(argv, argc, 2, "ll", &d, &p);
		helios_find_vdp(ctx, (sid_t)v, (dir_t)d, (sid_t)p, &arraylen);
	}
	RedisModule_ReplySetArrayLength(ctx, arraylen);
	return REDISMODULE_OK;
}

/*
 * input:
 * 	helios.find_p p_id [dir]
 * output:
 * 	array - [triple0, triple1, ...]
 * */
int Helios_Find_Predicate(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
	RedisModule_AutoMemory(ctx);
	
	if(argc < 2 || argc > 3 ){ // at least 2 aruguments
		return RedisModule_WrongArity(ctx);
	}

	long long p;
	RMUtil_ParseArgs(argv, argc, 1, "l", &p);
	
	RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
	int arraylen = 0;
	if(argc == 2)
	{
		helios_find_p(ctx, (sid_t)p, &arraylen);
	}
	else if(argc == 3){
		long long d;
		RMUtil_ParseArgs(argv, argc, 2, "l", &d);
		helios_find_pd(ctx, (sid_t)p, (dir_t)d, &arraylen);
	}
	RedisModule_ReplySetArrayLength(ctx, arraylen);
	return REDISMODULE_OK;
}
/*
 * input:
 * 	helios.find_index v_id p dir
 * output:
 * 	array - [triple0, triple1, ...]
 * */
int Helios_Find_Index(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
	RedisModule_AutoMemory(ctx);
	
	if(argc != 4 ){ // at least 2 aruguments
		return RedisModule_WrongArity(ctx);
	}

	long long v, p, d;
	RMUtil_ParseArgs(argv, argc, 1, "lll", &v, &p, &d);

	int arraylen = 0;
	RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
	helios_find_index(ctx, (sid_t)v, (sid_t)p, (dir_t)d, &arraylen);
	RedisModule_ReplySetArrayLength(ctx, arraylen);
	return REDISMODULE_OK;
}
/*
 * input:
 * 	helios.reassign v_id 
 * output:
 * 	str - OK
 * */
int Helios_Reassign(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
	RedisModule_AutoMemory(ctx);
	if(argc != 2){ // at least 2 aruguments
		return RedisModule_WrongArity(ctx);
	}

	long long v;
	RMUtil_ParseArgs(argv, argc, 1, "l", &v);
	//RedisModule_Log(ctx, "warning", "Helios_Reassign %ld \n", v);
	pthread_t tid;

	RedisModuleBlockedClient *bc = RedisModule_BlockClient(ctx,NULL,NULL,NULL,0);
	void **targ = RedisModule_Alloc(sizeof(void*)*2);
	targ[0] = bc;
	targ[1] = (void*)(sid_t)v;
	//targ[2] = (void *)&argc;

	if(pthread_create(&tid,NULL,Helios_Reassign_Thread, targ) != 0){
		RedisModule_AbortBlock(bc);
		RedisModule_Log(ctx, "error", "-ERR Can't start receive_thread");
		return RedisModule_ReplyWithError(ctx, "-ERR Can't start reassign_thread");
	}
	return REDISMODULE_OK;
}

/*
 * input:
 * 	helios.reassign_counter v_id 
 * output:
 * 	str - OK
 * */
int Helios_ReassignCounter(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
	RedisModule_AutoMemory(ctx);
	if(argc != 2){ // at least 2 aruguments
		return RedisModule_WrongArity(ctx);
	}

	long long v;
	RMUtil_ParseArgs(argv, argc, 1, "l", &v);
	//RedisModule_Log(ctx, "warning", "Helios_Reassign %ld \n", v);

	pthread_t tid;

	RedisModuleBlockedClient *bc = RedisModule_BlockClient(ctx,NULL,NULL,NULL,0);
	void **targ = RedisModule_Alloc(sizeof(void*)*2);
	targ[0] = bc;
	targ[1] = (void*)(sid_t)v;
	//targ[1] = argv;
	//targ[2] = (void *)&argc;

	if(pthread_create(&tid,NULL,Helios_ReassignCounter_Thread, targ) != 0){
		RedisModule_AbortBlock(bc);
		RedisModule_Log(ctx, "error", "-ERR Can't start receive_thread");
		return RedisModule_ReplyWithError(ctx, "-ERR Can't start reassigncounter_thread");
	}
	return REDISMODULE_OK;
}

/*
 * input:
 * 	helios.receive v_id
 * output:
 * 	str - OK
 * */
int Helios_Receive(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
	RedisModule_AutoMemory(ctx);
	if(argc != 2){ 
		return RedisModule_WrongArity(ctx);
	}

	long long v;
	RMUtil_ParseArgs(argv, argc, 1, "l", &v);
	//RedisModule_Log(ctx, "warning", "start Helios_Receive v = %ld", v); 
	pthread_t tid;
	RedisModuleBlockedClient *bc = RedisModule_BlockClient(ctx,NULL,NULL,NULL,0);
	void **targ = RedisModule_Alloc(sizeof(void*)*2);
	targ[0] = bc;
	targ[1] = (void *)(sid_t)v;
	//创建的进程没有及时释放的原因
	if(pthread_create(&tid,NULL,Helios_Receive_Thread, targ) != 0){
		RedisModule_AbortBlock(bc);
		RedisModule_Log(ctx, "error", "-ERR Can't start receive_thread v= %ld", v);
		return RedisModule_ReplyWithError(ctx, "-ERR Can't start receive_thread");
	}
	//1.阻塞模式使用join方法释放线程
	// pthread_join(tid, NULL);  //Error,开启第一个进程就直接卡死了 原因分析：
	//2.非阻塞模式 在线程函数内使用 head add : pthread_detach(pthread_self()); tail add : pthread_exit(0); 
	//3. 设置线程为分离属性 2类似于3 下面为示例 
	//pthread_t thread;
	//pthread_attr_t attr;
	//pthread_attr_init(&attr);
	//pthread_attr_setdetachstate(&attr, 1);
	//pthread_create(&thread, &attr, run, 0); //第二个参数决定了分离属性

	return REDISMODULE_OK;
}

/*
 * input:
 * 	helios.receive_counter v_id
 * output:
 * 	str - OK
 * */
int Helios_ReceiveCounter(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
	RedisModule_AutoMemory(ctx);
	if(argc != 2){ 
		return RedisModule_WrongArity(ctx);
	}

	long long v;
	RMUtil_ParseArgs(argv, argc, 1, "l", &v);

	pthread_t tid;
	RedisModuleBlockedClient *bc = RedisModule_BlockClient(ctx,NULL,NULL,NULL,0);
	void **targ = RedisModule_Alloc(sizeof(void*)*2);
	targ[0] = bc;
	targ[1] = (void *)(sid_t)v;

	if(pthread_create(&tid,NULL,Helios_ReceiveCounter_Thread, targ) != 0){
		RedisModule_AbortBlock(bc);
		RedisModule_Log(ctx, "error", "-ERR Can't start receive_thread v= %ld", v);
		return RedisModule_ReplyWithError(ctx, "-ERR Can't start receivecounter_thread");
	}
	return REDISMODULE_OK;
}
/*
 * input:
 * 	helios.update_edgeweight src_id tgt_id edgelog_len [release_rw_lock] 
 * output:
 * 	str - OK
 * */
int Helios_Update_EdgeWeight(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
	RedisModule_AutoMemory(ctx);
	if(argc != 4){ 
		return RedisModule_WrongArity(ctx);
	}
	long long src, o, edgelog_len;
	RMUtil_ParseArgs(argv, argc, 1, "lll", &src, &o, &edgelog_len);
	RedisModule_Log(ctx,"warning", "Update Edge weight %ld %ld begin", src, o);
	int flag = up_edge_weight(ctx, src, o, edgelog_len);
	if(flag){
		RedisModule_ReplyWithSimpleString(ctx, "OK");
		return REDISMODULE_OK;
	}else{
		RedisModule_ReplyWithSimpleString(ctx, "ERR");
		return REDISMODULE_ERR;
	}
	
	/*pthread_t tid;
	
	long long src, o, edgelog_len;
	RMUtil_ParseArgs(argv, argc, 1, "lll", &src, &o, &edgelog_len);
	

	
	RedisModuleBlockedClient *bc = RedisModule_BlockClient(ctx,NULL,NULL,NULL,0);
	void **targ = RedisModule_Alloc(sizeof(void*)*3);
	targ[0] = bc;
	targ[1] = argv;
	targ[2] = (void *)&argc;

	if(pthread_create(&tid,NULL,Up_EdgeWeight_Thread, targ) != 0){
		RedisModule_AbortBlock(bc);
		return RedisModule_ReplyWithError(ctx, "-ERR Can't start reassign_thread");
	}
	return REDISMODULE_OK;*/
}

/*
 * helios.append_activev vid vlog_len
 * */
int Helios_Append_ActiveV(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
	RedisModule_AutoMemory(ctx);
	if(argc != 3){ 
		return RedisModule_WrongArity(ctx);
	}
	long long vid, vlog_len;
	RMUtil_ParseArgs(argv, argc, 1, "ll", &vid, &vlog_len);
	int flag = append_activeV(ctx, vid, vlog_len);
	if(flag){
		//RedisModule_Log(ctx, "warning", "Append Active Vertex %ld succeed", vid); 
		RedisModule_ReplyWithSimpleString(ctx, "OK");
		return REDISMODULE_OK;
	}else{
		RedisModule_Log(ctx, "warning", "Append Active Vertex %ld succeed", vid);
		//RedisModule_ReplyWithSimpleString(ctx, "ERR");
		return REDISMODULE_ERR;
	}
	
	/*pthread_t tid;
	RedisModuleBlockedClient *bc = RedisModule_BlockClient(ctx,NULL,NULL,NULL,0);
	void **targ = RedisModule_Alloc(sizeof(void*)*3);
	targ[0] = bc;
	targ[1] = argv;
	targ[2] = (void *)&argc;

	if(pthread_create(&tid,NULL,Append_ActiveV_Thread, targ) != 0){
		RedisModule_AbortBlock(bc);
		return RedisModule_ReplyWithError(ctx, "-ERR Can't start reassign_thread");
	}
	return REDISMODULE_OK;*/
}

/*
 * helios.reassign_lock vid
 * fuction : 把本地节点的EXIST属性设置为0，如果该属性为NULL或0则返回ret = 0,否则返回ret = 1
 * */
int Helios_Reassign_Lock(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
	RedisModule_AutoMemory(ctx);
	if(argc != 2){ 
		return RedisModule_WrongArity(ctx);
	}
	long long vid;
	RMUtil_ParseArgs(argv, argc, 1, "l", &vid);
	
	sid_t sl_key = key_vpid_t(vid, 0, LOCALITY, (dir_t)0);
	RedisModuleString *val = helios_HashGet(ctx, sl_key, (int)EXISTS);
	if(val != NULL){
		long long v;
		RedisModule_StringToLongLong(val, &v);
		if(v == 1){
			RedisModule_ReplyWithLongLong(ctx, 1);
			RedisModule_Call(ctx, "HSET", "lll", 
				(long long)sl_key, (long long)EXISTS, (long long)0);
			return REDISMODULE_OK;
		}
	}
	RedisModule_ReplyWithLongLong(ctx, 0);
	return REDISMODULE_OK;

}

/*int _request_lock(RedisModuleCtx *ctx, sid_t v, lock_t l_t){
	sid_t vl_key = key_vpid_t(v, 0, LOCK, (dir_t)0);
	long long reassign = 0, rw = 0;
	helios_HashGetTwo(ctx, vl_key, RW_LOCK, REASSIGN_LOCK, &rw, &reassign);
	
	if(l_t == RW_LOCK){

		if(reassign == 0){
			RedisModule_Call(ctx, "HINCRBY","lll", vl_key, RW_LOCK, 1);
			return 1;
		}else
			return 0;
	}else if(l_t == REASSIGN_LOCK){
		if(reassign == 0 && rw == 0)
		{
			RedisModule_Call(ctx, "HINCRBY","lll", vl_key, REASSIGN_LOCK, 1);
			return 1;
		}else return 0;
	}
	return 0;
}*/


/*
 * input:
 * 	helios.request_v_lock vid lock_t 
 * output:
 * 	integer: 1 - OK; 0 - false
 * */
/*int Helios_Request_Vlock(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
	RedisModule_AutoMemory(ctx);
	if(argc != 3){ 
		return RedisModule_WrongArity(ctx);
	}
	long long vid, lock_t;
	RMUtil_ParseArgs(argv, argc, 1, "ll", &vid, &lock_t);
	if(_request_lock(ctx, vid, lock_t))
		RedisModule_ReplyWithLongLong(ctx, 1);
	else 
		RedisModule_ReplyWithLongLong(ctx, 0);
	return REDISMODULE_OK;

}*/

/*
 * input:
 * 	helios.getloc_reqlock vid lock_t 
 * output:
 * 	integer: -1 - initial server; -2 - request lock error; other string - hosting server
 * */
/*int Helios_GetLoc_ReqLock(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
	RedisModule_AutoMemory(ctx);
	if(argc != 3){ 
		return RedisModule_WrongArity(ctx);
	}
	long long vid, lock_t;
	RMUtil_ParseArgs(argv, argc, 1, "ll", &vid, &lock_t);

	if(_request_lock(ctx, vid, lock_t)){
		sid_t v_loc = key_vpid_t(vid, 0, LOC, (dir_t)0);
		RedisModuleCallReply *reply = RedisModule_Call(ctx, "GET", "l", v_loc);
		if(RedisModule_CallReplyType(reply) == REDISMODULE_REPLY_STRING)
			RedisModule_ReplyWithCallReply(ctx,reply);
		else
			RedisModule_ReplyWithLongLong(ctx, -1);
	}
	else {
		RedisModule_ReplyWithLongLong(ctx, -2);
	}
	return REDISMODULE_OK;
}

int _release_lock(RedisModuleCtx *ctx, sid_t v, lock_t l_t){
	sid_t vl_key = key_vpid_t(v, 0, LOCK, (dir_t)0);
	//long long reassign = 0, rw = 0;
	//helios_HashGetTwo(ctx, vl_key, RW_LOCK, REASSIGN_LOCK, &rw, &reassign);
	if(l_t == RW_LOCK){
		RedisModule_Call(ctx, "HINCRBY","lll", vl_key, RW_LOCK, (long long)-1);
		return 1;
	}else if(l_t == REASSIGN_LOCK){
		RedisModule_Call(ctx, "HINCRBY","lll", vl_key, REASSIGN_LOCK, (long long)-1);
		return 1;
	}
	return 0;
}*/
/*
 * input:
 * 	helios.release_v_lock vid lock_t 
 * output:
 * 	integer: 1 - OK; 0 - false
 * */
/*int Helios_Release_Vlock(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
	RedisModule_AutoMemory(ctx);
	if(argc != 3){ 
		return RedisModule_WrongArity(ctx);
	}
	long long vid, lock_t;
	RMUtil_ParseArgs(argv, argc, 1, "ll", &vid, &lock_t);
	if(_release_lock(ctx, vid, lock_t))
		RedisModule_ReplyWithLongLong(ctx, 1);
	else 
		RedisModule_ReplyWithLongLong(ctx, 0);
	return REDISMODULE_OK;
}*/

/*
 * input:
 * 	helios.travser_vertex inter_set_key num_predicate p1 p2 ...
 * output:
 * 	integer: 1 - OK; 0 - false
 * */
/*int Helios_Traverse_Vertex(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
	RedisModule_AutoMemory(ctx);
	RedisModule_ReplyWithSimpleString(ctx, "OK");
	return REDISMODULE_OK;
}*/


int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){

	if(RedisModule_Init(ctx, "helios", 1, REDISMODULE_APIVER_1)
		== REDISMODULE_ERR) return REDISMODULE_ERR;

	
	/*if(RedisModule_CreateCommand(ctx, "helios.load_dict",
		Helios_Load_Dict, "write",1,1,1) == REDISMODULE_ERR)
		return REDISMODULE_ERR;*/
	
	/*if(RedisModule_CreateCommand(ctx, "helios.load_data",
		Helios_Load_Data, "write",1,1,1) == REDISMODULE_ERR)
		return REDISMODULE_ERR;*/
	
	if(RedisModule_CreateCommand(ctx, "helios.insert_triple",
		Helios_Insert_Triple, "write",1,1,1) == REDISMODULE_ERR)
		return REDISMODULE_ERR;

	

	/*if(RedisModule_CreateCommand(ctx, "helios.pattern_evaluate",
		Helios_Pattern_Evaluate, "readonly",1,1,1) == REDISMODULE_ERR)
		return REDISMODULE_ERR;*/
	
	if(RedisModule_CreateCommand(ctx, "helios.find_v",
		Helios_Find_Vertex, "readonly",1,1,1) == REDISMODULE_ERR)
		return REDISMODULE_ERR;


	if(RedisModule_CreateCommand(ctx, "helios.find_p",
		Helios_Find_Predicate, "readonly",1,1,1) == REDISMODULE_ERR)
		return REDISMODULE_ERR;
	
	if(RedisModule_CreateCommand(ctx, "helios.find_index",
		Helios_Find_Index, "readonly",1,1,1) == REDISMODULE_ERR)
		return REDISMODULE_ERR;

	if(RedisModule_CreateCommand(ctx, "helios.reassign",
		Helios_Reassign, "write",1,1,1) == REDISMODULE_ERR)
		return REDISMODULE_ERR;
	
	if(RedisModule_CreateCommand(ctx, "helios.receive",
		Helios_Receive, "write",1,1,1) == REDISMODULE_ERR)
		return REDISMODULE_ERR;
	
	if(RedisModule_CreateCommand(ctx, "helios.reassign_lock",
		Helios_Reassign_Lock, "write",1,1,1) == REDISMODULE_ERR)
		return REDISMODULE_ERR;
	
//////////////////////////// Commands on Counter DB////////////////////////////////////////	

	if(RedisModule_CreateCommand(ctx, "helios.reassign_counter",
		Helios_ReassignCounter, "write",1,1,1) == REDISMODULE_ERR)
		return REDISMODULE_ERR;
	
	//add for DoI INDEX info
	/*if (RedisModule_CreateCommand(ctx, "helios.reassign_DoI",
		Helios_ReassignCounter, "write", 1, 1, 1) == REDISMODULE_ERR)
		return REDISMODULE_ERR;*/

	if(RedisModule_CreateCommand(ctx, "helios.receive_counter",
		Helios_ReceiveCounter, "write",1,1,1) == REDISMODULE_ERR)
		return REDISMODULE_ERR;

	if(RedisModule_CreateCommand(ctx, "helios.update_edgeweight",
		Helios_Update_EdgeWeight, "write",1,1,1) == REDISMODULE_ERR)
		return REDISMODULE_ERR;
	
	if(RedisModule_CreateCommand(ctx, "helios.append_activev",
		Helios_Append_ActiveV, "write",1,1,1) == REDISMODULE_ERR)
		return REDISMODULE_ERR;

	if(RedisModule_CreateCommand(ctx, "helios.find_edgeweight",
		Helios_Find_EdgeWei, "readonly",1,1,1) == REDISMODULE_ERR)
		return REDISMODULE_ERR;

	
	
///////////////////////////// Both /////////////////////////////////////////////
	if(RedisModule_CreateCommand(ctx, "helios.get_reassign_info_ts", //triple store
		Helios_Get_ReassignInfo_TS, "write", 1,1,1) == REDISMODULE_ERR)
		return REDISMODULE_ERR;
	
	if(RedisModule_CreateCommand(ctx, "helios.get_reassign_info_cs", //counter store
		Helios_Get_ReassignInfo_CS, "write", 1,1,1) == REDISMODULE_ERR)
		return REDISMODULE_ERR;

	if(RedisModule_CreateCommand(ctx, "helios.find_vcounter_ts",
		Helios_Find_Vcounter_TS, "readonly",1,1,1) == REDISMODULE_ERR)
		return REDISMODULE_ERR;	
	
	if(RedisModule_CreateCommand(ctx, "helios.find_vcounter_cs",
		Helios_Find_Vcounter_CS, "readonly",1,1,1) == REDISMODULE_ERR)
		return REDISMODULE_ERR;
	
	if(RedisModule_CreateCommand(ctx, "helios.find_key",
		Helios_Find_Key, "readonly",1,1,1) == REDISMODULE_ERR)
		return REDISMODULE_ERR;
	/*if(RedisModule_CreateCommand(ctx, "helios.request_v_lock",
		Helios_Request_Vlock, "write",1,1,1) == REDISMODULE_ERR)
		return REDISMODULE_ERR;
	
	if(RedisModule_CreateCommand(ctx, "helios.getloc_reqlock",
		Helios_GetLoc_ReqLock, "write",1,1,1) == REDISMODULE_ERR)
		return REDISMODULE_ERR;

	if(RedisModule_CreateCommand(ctx, "helios.release_v_lock",
		Helios_Release_Vlock, "write",1,1,1) == REDISMODULE_ERR)
		return REDISMODULE_ERR;

	if(RedisModule_CreateCommand(ctx, "helios.travser_vertex",
		Helios_Traverse_Vertex, "write",1,1,1) == REDISMODULE_ERR)
		return REDISMODULE_ERR;*/
	
	return REDISMODULE_OK;
}
