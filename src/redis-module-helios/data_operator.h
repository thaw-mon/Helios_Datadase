#ifndef HELIOS_DATA_OPERATOR_H
#define HELIOS_DATA_OPERATOR_H

#include <stdio.h>
#include <string.h>
#include <stddef.h>
#include "redismodule.h"
#include "utils/type.h"
#include "utils/HashFunction.h"
#include "redis-module-helios/rmutil/util.h"
#include "pthread.h"

/*
 * input: s p o d
 * update:
 * 	SET:  spd - [o] 
 * 	SET:  sd - [p]  
 * 	HASH: s + counter - EDGENUM + 1
 * 	HASH: s + locality - EXISTS = 1
 * 	HASH: o + locality - TOP + 1
 *	HASH: SERVER_INFO - EDGENUM + 1
 *	HASH: SERVER_INFO - OBJNUM + 1
 *	SET:  pd - [s]
 *	HASH: p + counter - d + 1
 * */
int _insert_triple(RedisModuleCtx *ctx, sid_t s, sid_t p, sid_t o, dir_t d, int num_servers, int sid){
	//printf("start in _insert_triple s = %ld, p = %ld o = %ld\n ",s,p,o);
	RedisModule_AutoMemory(ctx);
	sid_t sp_key = key_vpid_t(s, p, NO_INDEX, d);
	sid_t sd_key = key_vpid_t(s, 0, NO_INDEX, d);
	RedisModuleString* v_str = RedisModule_CreateString(ctx, serial_vid(o), NBYTES_VID);
	RedisModule_Call(ctx, "APPEND", "ls", (long long)sp_key, v_str);
	RedisModule_Call(ctx, "SADD", "ll", (long long)sd_key, (long long)p);
	
	//printf("start insert node info \n ");
	/* vertex counter and locality*/
	sid_t sc_key = key_vpid_t(s, 0, COUNTER, (dir_t)0);
	RedisModule_Call(ctx, "HINCRBY", "lll", (long long)sc_key, (long long)EDGENUM, (long long)1);

	sid_t sl_key = key_vpid_t(s, 0, LOCALITY, (dir_t)0);
	RedisModule_Call(ctx, "HSET", "lll", (long long)sl_key, (long long)EXISTS, (long long)1);

//#ifdef REASSIGN_ON_TOP
	sid_t ol_key = key_vpid_t(o, 0, LOCALITY, (dir_t)0);
	RedisModule_Call(ctx, "HINCRBY", "lll", (long long)ol_key, (long long)TOP, (long long)1);
//#endif 

	//printf("start insert serverInfo \n ");
	/*SERVER counter info*/
	RedisModuleCallReply *reply ;
	reply = RedisModule_Call(ctx, "SADD", "cl",SERVER_ALL, (long long)s);
	if(RedisModule_CallReplyInteger(reply) == 1){
		RedisModule_Call(ctx, "HINCRBY", "cll", SERVER_INFO, (long long)OBJNUM, (long long)1);
	}
	RedisModule_FreeCallReply(reply);
	
	RedisModule_Call(ctx, "HINCRBY", "cll", SERVER_INFO, (long long)EDGENUM, (long long)1);

	////////////////////////////////////////////////////
	
	/* Query required index*/
	//printf("start index indexInfo \n ");
	//update local index
	sid_t pd_key = key_vpid_t(0, p, INDEX, d);
	reply = RedisModule_Call(ctx, "SADD", "ll", (long long)pd_key, (long long)s);
	if(RedisModule_CallReplyInteger(reply) == 1){
		sid_t pd_key = key_vpid_t(0, p, COUNTER, (dir_t)0);
		RedisModule_Call(ctx, "HINCRBY", "lll", pd_key, (long long)d, (long long)1);
	}
	RedisModule_FreeCallReply(reply);
	//printf("start index p==1 indexInfo \n ");
	//Question p==1 d==IN情形如何处理？？
	if(p == 1 && d == OUT){		
		sid_t tdi_key = key_vpid_t(0, p, INDEX, IN);
		reply = RedisModule_Call(ctx, "SADD", "ll", (long long)tdi_key, (long long)o);
		if(RedisModule_CallReplyInteger(reply) == 1){
			sid_t pd_key = key_vpid_t(0, p, COUNTER, (dir_t)0);
			RedisModule_Call(ctx, "HINCRBY", "lll", pd_key, (long long)IN, (long long)1);
		}
		RedisModule_FreeCallReply(reply);
	}
	//printf("start index p != 1 indexInfo \n ");
	if(p != 1)
	{
		sid_t op_key = key_vpid_t(o, p, INDEX, inverse_dir(d));
		RedisModule_Call(ctx, "SADD", "ll", (long long)op_key, (long long)s);
	}
	else if(p == 1 && d == OUT){
		sid_t op_key = key_vpid_t(0, o, INDEX, d);
		reply = RedisModule_Call(ctx, "SADD", "ll", (long long)op_key, (long long)s);
		if(RedisModule_CallReplyInteger(reply) == 1){
			sid_t pd_key = key_vpid_t(0, o, COUNTER, (dir_t)0);
			RedisModule_Call(ctx, "HINCRBY", "lll", pd_key, (long long)d, (long long)1);
		}
		RedisModule_FreeCallReply(reply);
	}
	//printf("succ in _insert_triple s = %ld, p = %ld o = %ld\n ", s, p, o);
	return 1;
}

int helios_find_index(RedisModuleCtx *ctx, sid_t v, sid_t p, dir_t d, int * arraylen){
	sid_t op_key = key_vpid_t(v, p,INDEX, d);
	RedisModule_ReplyWithLongLong(ctx, (long long) v);
	RedisModule_ReplyWithLongLong(ctx, (long long) p);
	RedisModule_ReplyWithLongLong(ctx, (long long) d);
	RedisModuleCallReply* reply = RedisModule_Call(ctx,"SMEMBERS", "l", (long long)op_key);
	RedisModule_ReplyWithCallReply(ctx, reply);
	RedisModule_FreeCallReply(reply);
	(*arraylen) += 4;
	return 1;
}

int helios_find_vdp(RedisModuleCtx *ctx, sid_t v, sid_t p, dir_t d,int * arraylen){
	sid_t sp_key = key_vpid_t(v, p, NO_INDEX, d);
	RedisModule_ReplyWithLongLong(ctx, (long long) v);
	RedisModule_ReplyWithLongLong(ctx, (long long) p);
	RedisModule_ReplyWithLongLong(ctx, (long long) d);
	RedisModuleCallReply *reply = RedisModule_Call(ctx, "GET", "l", sp_key);
	RedisModule_ReplyWithCallReply(ctx, reply);
	RedisModule_FreeCallReply(reply);
	(*arraylen) += 4;
	return 1;
}

int helios_find_vd(RedisModuleCtx *ctx, sid_t v, dir_t d, int * arraylen){
	sid_t sd_key = key_vpid_t(v, 0, NO_INDEX, d);
	Vector* p_list = helios_SetGet(ctx, sd_key);
	if(p_list == NULL)
		return 0;
	int p_num = Vector_Cap(p_list);
	for(int i = 0; i < p_num; i++){
		sid_t p;
		Vector_Get(p_list, i, &p);
		helios_find_vdp(ctx, v, p, d, arraylen);
	}
	Vector_Free(p_list);
	return 1;
}

int helios_find_v(RedisModuleCtx *ctx, sid_t v, int * arraylen){
	helios_find_vd(ctx, v, IN, arraylen);
	helios_find_vd(ctx, v, OUT, arraylen);
	return 1;
}

int helios_find_pd(RedisModuleCtx *ctx, sid_t p, dir_t d, int *arraylen){
	sid_t pd_key = key_vpid_t(0, p, INDEX, d);
	Vector* o_list = helios_SetGet(ctx, pd_key);
	if(o_list == NULL)
		return 0;
	int o_num = Vector_Cap(o_list);
	for(int i = 0; i < o_num; i++){
		sid_t o;
		Vector_Get(o_list, i, &o);
		RedisModule_ReplyWithLongLong(ctx, (long long)o);
		(*arraylen)++;
	}
	Vector_Free(o_list);
	return 1;
}

int helios_find_p(RedisModuleCtx *ctx, sid_t p, int *arraylen){
	helios_find_pd(ctx, p, IN, arraylen);
	helios_find_pd(ctx, p, OUT, arraylen);
	return 1;
}

//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
int mvout_edgelog(RedisModuleCtx *ctx, sid_t s, sid_t t){
	RedisModule_AutoMemory(ctx);
	
	char str_s[30]; char str_t[30];
	sprintf(str_s, "%ld", s);
	sprintf(str_t, "%ld", t);
	char edge[strlen(str_s) + strlen(str_t) + 2];
	char* sep = "-";
	strcpy(edge, str_s); strcat(edge, sep); strcat(edge, str_t); 

	RedisModuleString *edge_str = RedisModule_CreateString(ctx, edge, strlen(edge));
	RedisModule_RetainString(ctx, edge_str);

	RedisModule_ThreadSafeContextLock(ctx);
	RedisModule_Call(ctx, "LREM", "cls", EDGELOG, (long long)1, edge_str);
	RedisModule_ThreadSafeContextUnlock(ctx);
	return 1;
}

int mvin_edgelog(RedisModuleCtx *ctx, sid_t s, sid_t t){
	RedisModule_AutoMemory(ctx);
	char str_s[30]; char str_t[30];
	sprintf(str_s, "%ld", s);
	sprintf(str_t, "%ld", t);
	char edge[strlen(str_s) + strlen(str_t) + 2];
	char* sep = "-";
	strcpy(edge, str_s); strcat(edge, sep); strcat(edge, str_t); 

	RedisModuleString *edge_str = RedisModule_CreateString(ctx, edge, strlen(edge));
	RedisModule_RetainString(ctx, edge_str);

	RedisModule_ThreadSafeContextLock(ctx);
	RedisModule_Call(ctx, "LREM", "cls", EDGELOG, (long long)0, edge_str);
	RedisModule_Call(ctx, "LPUSH", "cs", EDGELOG, edge_str);//push to the head
	RedisModule_ThreadSafeContextUnlock(ctx);
	return 1;
}
/*
 * input: s tgthost  tgtport 
 * migrate:
 * 	migrate: sd - [p]
 *	For each p: 
 *		spd - [o]
 *		edgeweitght list
 *		for each o: o + locality - TOP -1
 *
 *		remove: pd - s
 *		decr: p + counter - d-1
 *
 * 	migrate: s + counter 
 * update:	
 *      s + locality - EXISTS = 0
 *	SERVER_INFO - EDGENUM - 
 *	SERVER_INFO - OBJNUM - 1
 *	SERVER_INFO - WEIGHT - 1
 * edgelog:
 * 	
 * */

int _reassign_vd(RedisModuleCtx *ctx, sid_t v, dir_t d){//, const char* tgt_host, int tgt_port){
	RedisModule_AutoMemory(ctx);
	
	sid_t sd_key = key_vpid_t(v, 0, NO_INDEX, d);
	Vector* p_list =  helios_SetGet(ctx, sd_key);
	//RedisModule_Log(ctx, "warning", "Reassign_vd sd_key = %ld", sd_key);
	if(p_list == NULL)
		return 1;
	for(int i = 0; i < Vector_Cap(p_list); i++){
		sid_t p;
		Vector_Get(p_list, i, &p);
		sid_t sp_key = key_vpid_t(v, p, NO_INDEX, d);
		Vector* o_list = helios_StringGet(ctx, sp_key);
		if(o_list == NULL)
			continue;
		for(int j = 0; j < Vector_Cap(o_list); j++){
			sid_t o;
			Vector_Get(o_list, j, &o);
			sid_t ol_key = key_vpid_t(o, 0, LOCALITY, (dir_t)0);
			//printf("%ld locality %ld\n", o, ol_key);
			RedisModule_ThreadSafeContextLock(ctx);
			RedisModule_Call(ctx, "HINCRBY", "lll", (long long)ol_key, 
					(long long)TOP, (long long)-1);
			RedisModule_ThreadSafeContextUnlock(ctx);		 		
			if(p != 1){
				sid_t oi_key = key_vpid_t(o, p, INDEX, inverse_dir(d));
				RedisModule_ThreadSafeContextLock(ctx);
				RedisModule_Call(ctx, "SREM", "ll", oi_key, v);
				RedisModule_ThreadSafeContextUnlock(ctx);
				continue;
			}

			sid_t pd_key = key_vpid_t(0, o, INDEX, d);
			RedisModule_ThreadSafeContextLock(ctx);
			RedisModule_Call(ctx, "SREM", "ll", pd_key, v);
			RedisModule_ThreadSafeContextUnlock(ctx);
			
			sid_t pc_key = key_vpid_t(0, o, COUNTER, (dir_t)0);
			RedisModule_ThreadSafeContextLock(ctx);
			RedisModule_Call(ctx, "HINCRBY", "lll", pc_key, (long long)d, (long long)-1);
			RedisModule_ThreadSafeContextUnlock(ctx);
		}
		//if(p != 1){
			sid_t pd_key = key_vpid_t(0, p, INDEX, d);
			RedisModule_ThreadSafeContextLock(ctx);
			RedisModule_Call(ctx, "SREM", "ll", pd_key, v);
			RedisModule_ThreadSafeContextUnlock(ctx);

			sid_t pc_key = key_vpid_t(0, p, COUNTER, (dir_t)0);
			RedisModule_ThreadSafeContextLock(ctx);
			RedisModule_Call(ctx, "HINCRBY", "lll", pc_key, (long long)d, (long long)-1);
			RedisModule_ThreadSafeContextUnlock(ctx);
		//}
				
		RedisModule_ThreadSafeContextLock(ctx);
		//RedisModule_Call(ctx, "MIGRATE", "cllllcc", tgt_host, tgt_port, sp_key, 0, 1000, "COPY", "REPLACE");
		RedisModule_Call(ctx, "DEL", "l", sp_key);
		//RedisModule_Log(ctx, "warning", "MIGRATE %ld", sp_key);
		RedisModule_ThreadSafeContextUnlock(ctx);
	}
	RedisModule_ThreadSafeContextLock(ctx);
	//RedisModule_Call(ctx, "MIGRATE", "cllllcc", tgt_host, tgt_port, sd_key, 0, 1000, "COPY", "REPLACE");
	//RedisModule_Call(ctx, "MIGRATE", "cllllcc", tgt_host, tgt_port, 
	//		key_vpid_t(v,0,NO_INDEX,d), 0, 1000, "COPY", "REPLACE");
	RedisModule_Call(ctx, "DEL", "l", sd_key);
	RedisModule_ThreadSafeContextUnlock(ctx);
	return 1;
}

int _reassign_data(RedisModuleCtx *ctx, sid_t v){//, const char* tgt_host, int tgt_port){
	RedisModule_AutoMemory(ctx);

	int flag = 1;
	flag = flag && _reassign_vd(ctx, v, OUT);//, tgt_host, tgt_port);
	flag = flag && _reassign_vd(ctx, v, IN);//, tgt_host, tgt_port);

	sid_t sc_key = key_vpid_t(v, 0, COUNTER, (dir_t)0);
	long long v_edgenum = 0;//, v_weight = 0;
	RedisModule_ThreadSafeContextLock(ctx);
	helios_HashGetOne(ctx, sc_key, (long long)EDGENUM, &v_edgenum);
	RedisModule_ThreadSafeContextUnlock(ctx);
	
	RedisModule_ThreadSafeContextLock(ctx);
	RedisModule_Call(ctx, "DEL", "l", sc_key);
	RedisModule_ThreadSafeContextUnlock(ctx);
	
	sid_t sl_key = key_vpid_t(v, 0, LOCALITY, (dir_t)0);
	RedisModule_ThreadSafeContextLock(ctx);
	RedisModule_Call(ctx, "HSET", "lll", (long long)sl_key, (long long)EXISTS, (long long)0);
	RedisModule_ThreadSafeContextUnlock(ctx);

	RedisModule_ThreadSafeContextLock(ctx);
	RedisModule_Call(ctx, "SREM", "cl", SERVER_ALL, v);
	RedisModule_ThreadSafeContextUnlock(ctx);

	RedisModule_ThreadSafeContextLock(ctx);
	RedisModule_Call(ctx, "HINCRBY", "cll", SERVER_INFO, (long long)OBJNUM, (long long)-1);
	RedisModule_ThreadSafeContextUnlock(ctx);
	
	RedisModule_ThreadSafeContextLock(ctx);
	RedisModule_Call(ctx, "HINCRBY", "cll", SERVER_INFO, (long long)EDGENUM, (long long)-v_edgenum);
	RedisModule_ThreadSafeContextUnlock(ctx);
	
	return flag;
}
//TODO 修改为类似于Reveive函数的模块
void *Helios_Reassign_Thread(void *arg){
	pthread_detach(pthread_self());
	void **targ = arg;
	RedisModuleBlockedClient *bc = targ[0];
	sid_t vid = (sid_t)targ[1];
	//RedisModuleString **argv = targ[1];
	//int argc = *(int*)targ[2];
	RedisModule_Free(targ);

	RedisModuleCtx *ctx = RedisModule_GetThreadSafeContext(bc);

	//RMUtil_ParseArgs(argv, argc, 1, "l", &vid);//, &tgt_host, &tgt_port);
	//RedisModule_Log(ctx, "warning", "Reassign %ld Start\n", vid);//, tgt_host, tgt_port);

	int flag = _reassign_data(ctx, vid);//, tgt_host, tgt_port);

	if(flag){
		//RedisModule_Log(ctx,"warning", "Reassign %ld succeed", vid);
		RedisModule_ReplyWithSimpleString(ctx, "OK");
	}
	else{
		RedisModule_Log(ctx,"warning", "Reassign %ld failed", vid); 
		RedisModule_ReplyWithSimpleString(ctx, "ERR");
	}

	RedisModule_FreeThreadSafeContext(ctx);
	RedisModule_UnblockClient(bc, NULL);
	pthread_exit(0);
	return NULL;
}

/////////////////////////////////////////////////////////////////////////////////////////

int _reassign_weight_locality(RedisModuleCtx *ctx, sid_t v){//, const char* tgt_host, int tgt_port){
	RedisModule_AutoMemory(ctx);
	sid_t ew_key = key_vpid_t(v, 0, EDGE_WEIGHT, (dir_t)0);
 	RedisModuleCallReply *reply;

	RedisModule_ThreadSafeContextLock(ctx);
	reply = RedisModule_Call(ctx, "HGETALL", "l", (long long)ew_key);
	RedisModule_ThreadSafeContextUnlock(ctx);

	if(RedisModule_CallReplyType(reply) == REDISMODULE_REPLY_ARRAY){
		long long items = RedisModule_CallReplyLength(reply);
		long long i = 0;
		while(i < items){
			long long o, edge_weight;
			RedisModuleCallReply *item; RedisModuleString *istr;
			item = RedisModule_CallReplyArrayElement(reply, i);
			istr = RedisModule_CreateStringFromCallReply(item);
			if(istr != NULL) RedisModule_StringToLongLong(istr, &o);
			RedisModule_FreeString(ctx, istr);
			RedisModule_FreeCallReply(item);

			i++;
			item = RedisModule_CallReplyArrayElement(reply, i);
			istr = RedisModule_CreateStringFromCallReply(item);
			if(istr != NULL) RedisModule_StringToLongLong(istr, &edge_weight);
			RedisModule_FreeString(ctx, istr);
			RedisModule_FreeCallReply(item);
			i++;

			sid_t ol_key = key_vpid_t(o, 0, LOCALITY, (dir_t)0);
			RedisModule_ThreadSafeContextLock(ctx);
			RedisModule_Call(ctx, "HINCRBY", "lll", (long long)ol_key, 
					(long long)WORKLOAD, (long long)-edge_weight);
			RedisModule_ThreadSafeContextUnlock(ctx);
			/*Edge_LOG*/
			mvout_edgelog(ctx,v,o);
		}
		RedisModule_ThreadSafeContextLock(ctx);
		//RedisModule_Call(ctx, "MIGRATE", "cllllcc", tgt_host, tgt_port, ew_key, 
		//		0, 1000, "COPY", "REPLACE");
		RedisModule_Call(ctx, "DEL", "l", ew_key);
		RedisModule_ThreadSafeContextUnlock(ctx);
	}else{
		RedisModule_Log(ctx, "warning", "Edge weight not exists or type wrong");
	}
	RedisModule_FreeCallReply(reply);
	return 1;
}

int _reassign_counter(RedisModuleCtx *ctx, sid_t v){//, const char* tgt_host, int tgt_port){
	RedisModule_AutoMemory(ctx);

	int flag = 1;
	sid_t sc_key = key_vpid_t(v, 0, COUNTER, (dir_t)0);
	long long v_weight = 0;
	long long v_log_weight = 0;
	RedisModule_ThreadSafeContextLock(ctx);
	helios_HashGetOne(ctx, sc_key, (long long)WEIGHT, &v_weight);
	helios_HashGetOne(ctx, sc_key, (long long)VERTEX_WEIGHT, &v_log_weight);

	RedisModule_ThreadSafeContextUnlock(ctx);
	
	RedisModule_ThreadSafeContextLock(ctx);
	RedisModule_Call(ctx, "DEL", "l", sc_key);
	RedisModule_ThreadSafeContextUnlock(ctx);
	
	//这个命令太耗时了 
	RedisModule_ThreadSafeContextLock(ctx);
	//TODO 不能使用LREM 命令实在是太耗时了需要修改
	sid_t rd_key = key_vpid_t(v, 0, REASSIGN_DEL, (dir_t)0); //need to remove node number in log 
	RedisModule_Call(ctx, "HINCRBY", "lll", rd_key, (long long)VERTEX_WEIGHT, v_log_weight);
	//add need delete key
	//RedisModule_Call(ctx, "LREM", "cll", ACTIVE_OBJECT, (long long)0,v);
	RedisModule_Call(ctx, "DECR", "c", ACTIVE_OBJECT_SIZE);
	RedisModule_ThreadSafeContextUnlock(ctx);
	
	RedisModule_ThreadSafeContextLock(ctx);
	RedisModule_Call(ctx, "HINCRBY", "cll", SERVER_INFO, (long long)WEIGHT, (long long)-v_weight);
	RedisModule_ThreadSafeContextUnlock(ctx);

	//RedisModule_Log(ctx, "warning", "_reassign_weight_locality start v = %ld\n",v);
	//flag = flag && _reassign_weight_locality(ctx, v);//, tgt_host, tgt_port);
	return flag;
}

void *Helios_ReassignCounter_Thread(void *arg){
	pthread_detach(pthread_self());
	void **targ = arg;
	RedisModuleBlockedClient *bc = targ[0];
	sid_t vid = (sid_t)targ[1];
	//RedisModuleString **argv = targ[1];
	//int argc = *(int*)targ[2];
	RedisModule_Free(targ);

	RedisModuleCtx *ctx = RedisModule_GetThreadSafeContext(bc);

	//RedisModule_Log(ctx, "warning", "Reassign counter %ld start\n", vid);//, tgt_host, tgt_port);

	int flag = _reassign_counter(ctx, vid);//, tgt_host, tgt_port);

	if(flag){
		//RedisModule_Log(ctx,"warning", "Reassign counter %ld succeed", vid);
		RedisModule_ReplyWithSimpleString(ctx, "OK");
	}
	else{
		RedisModule_Log(ctx,"warning", "Reassign counter %ld failed", vid); 
		RedisModule_ReplyWithSimpleString(ctx, "ERR");
	}

	RedisModule_FreeThreadSafeContext(ctx);
	RedisModule_UnblockClient(bc, NULL);
	pthread_exit(0);
	return NULL;
}

//////////////////////////////////////////////////////////////////////////////////////
//Fuction 根据 sp_key 和sd_key创建其他的关联数据
int _receive_vd(RedisModuleCtx *ctx, sid_t v, dir_t d){
	RedisModule_AutoMemory(ctx);
	sid_t sd_key = key_vpid_t(v, 0, NO_INDEX, d);
	Vector* p_list =  helios_SetGet(ctx, sd_key);
	if(p_list == NULL)
		return 1;
	for(int i = 0; i < Vector_Cap(p_list); i++){
		sid_t p;
		Vector_Get(p_list, i, &p);
		sid_t sp_key = key_vpid_t(v, p, NO_INDEX, d);
		Vector* o_list = helios_StringGet(ctx, sp_key);
		if(o_list == NULL)
			continue;
		for(int j = 0; j < Vector_Cap(o_list); j++){
			sid_t o;
			Vector_Get(o_list, j, &o);
			sid_t ol_key = key_vpid_t(o, 0, LOCALITY, (dir_t)0);
			RedisModule_ThreadSafeContextLock(ctx);
			RedisModule_Call(ctx, "HINCRBY", "lll", (long long)ol_key, 
					(long long)TOP, (long long)1);
			RedisModule_ThreadSafeContextUnlock(ctx);

			if(p != 1){
				sid_t oi_key = key_vpid_t(o, p, INDEX, inverse_dir(d));
				RedisModule_ThreadSafeContextLock(ctx);
				RedisModule_Call(ctx, "SADD", "ll", oi_key, v);
				//RedisModule_Call(ctx, "SADD", "lll", oi_key, v, v);
				RedisModule_ThreadSafeContextUnlock(ctx);
				continue;
			}
			sid_t pd_key = key_vpid_t(0, o, INDEX, d);
			RedisModule_ThreadSafeContextLock(ctx);
			RedisModule_Call(ctx, "SADD", "ll", pd_key, v);
			//RedisModule_Call(ctx, "SADD", "lll", pd_key, v, v);
			RedisModule_ThreadSafeContextUnlock(ctx);

			sid_t pc_key = key_vpid_t(0, o, COUNTER, (dir_t)0);
			RedisModule_ThreadSafeContextLock(ctx);
			RedisModule_Call(ctx, "HINCRBY", "lll", pc_key, (long long)d, (long long)1);
			RedisModule_ThreadSafeContextUnlock(ctx);
		}
		//if(p != 1){
			sid_t pd_key = key_vpid_t(0, p, INDEX, d);
			RedisModule_ThreadSafeContextLock(ctx);
			RedisModule_Call(ctx, "SADD", "ll", pd_key, v);
			//RedisModule_Call(ctx, "SADD", "lll", pd_key, v, v);
			RedisModule_ThreadSafeContextUnlock(ctx);

			sid_t pc_key = key_vpid_t(0, p, COUNTER, (dir_t)0);
			RedisModule_ThreadSafeContextLock(ctx);
			RedisModule_Call(ctx, "HINCRBY", "lll", pc_key, (long long)d, (long long)1);
			RedisModule_ThreadSafeContextUnlock(ctx);
		//}
	}
	return 1;
}

int _receive_data(RedisModuleCtx *ctx, sid_t v){
	RedisModule_AutoMemory(ctx);
	sid_t sl_key = key_vpid_t(v, 0, LOCALITY, (dir_t)0);
	RedisModule_ThreadSafeContextLock(ctx);
	RedisModule_Call(ctx, "HSET", "lll", (long long)sl_key, (long long)EXISTS, (long long)1);
	RedisModule_ThreadSafeContextUnlock(ctx);
	
	int flag = 1;
	flag = flag && _receive_vd(ctx, v, OUT);
	flag = flag && _receive_vd(ctx, v, IN);
	//printf("succ reveive vd v = %ld\n", v); 
	if(!flag)
		RedisModule_Log(ctx, "warning", "error receive vd v = %ld", v);
		
	sid_t sc_key = key_vpid_t(v, 0, COUNTER, (dir_t)0);
	long long v_edgenum = 0;
	RedisModule_ThreadSafeContextLock(ctx);
	helios_HashGetOne(ctx, sc_key, (long long)EDGENUM, &v_edgenum);
	RedisModule_ThreadSafeContextUnlock(ctx);

	RedisModule_ThreadSafeContextLock(ctx);
	RedisModule_Call(ctx, "SADD", "cl", SERVER_ALL, (long long)v);
	//RedisModule_Call(ctx, "SADD", "cll", SERVER_ALL, (long long)v, (long long)v); 
	RedisModule_ThreadSafeContextUnlock(ctx);
	
	RedisModule_ThreadSafeContextLock(ctx);
	RedisModule_Call(ctx, "HINCRBY", "cll", SERVER_INFO, (long long)OBJNUM, (long long)1);
	RedisModule_ThreadSafeContextUnlock(ctx);
	
	RedisModule_ThreadSafeContextLock(ctx);
	RedisModule_Call(ctx, "HINCRBY", "cll", SERVER_INFO, (long long)EDGENUM, (long long)v_edgenum);
	RedisModule_ThreadSafeContextUnlock(ctx);
	
	return flag;
}
void* Helios_Receive_Thread(void *arg){
	pthread_detach(pthread_self());
	void **targ = arg;
	RedisModuleBlockedClient *bc = targ[0];
	sid_t vid = (sid_t)targ[1];
	
	if (bc == NULL) {
		printf("Helios_Receive_Thread arg error\n");
		//RedisModule_Log(ctx, "warning", "Receive %ld start", vid);
	}
	RedisModule_Free(targ); 
		
	RedisModuleCtx *ctx = RedisModule_GetThreadSafeContext(bc);

	//RedisModule_Log(ctx, "warning", "Receive %ld start", vid); 
	int flag = _receive_data(ctx, vid);

	//printf("succ reveive all data v = %ld\n", vid);
	if(flag){
		//RedisModule_Log(ctx,"warning", "Receive %ld succeed", vid);
		RedisModule_ReplyWithSimpleString(ctx, "OK");
	}
	else{ 
		RedisModule_Log(ctx,"warning", "Receive %ld failed", vid);
		RedisModule_ReplyWithSimpleString(ctx, "ERR");

	}

	RedisModule_FreeThreadSafeContext(ctx);
	RedisModule_UnblockClient(bc, NULL);
	pthread_exit(0);
	return NULL;
}

//////////////////////////////////////////////////////////////////////////////////

int _receive_weight_locality(RedisModuleCtx *ctx, sid_t v){
	
	RedisModule_AutoMemory(ctx);
	sid_t ew_key = key_vpid_t(v, 0, EDGE_WEIGHT, (dir_t)0);
 	RedisModuleCallReply *reply;

	RedisModule_ThreadSafeContextLock(ctx);
	reply = RedisModule_Call(ctx, "HGETALL", "l", (long long)ew_key);
	RedisModule_ThreadSafeContextUnlock(ctx);
	//很可能是这里出现了数组越界问题
	if(RedisModule_CallReplyType(reply) == REDISMODULE_REPLY_ARRAY){
		long long items = RedisModule_CallReplyLength(reply);
		long long i = 0;
		if (items % 3 > 0) {
			RedisModule_Log(ctx, "error ", "_receive_weight_locality items = %ld", items);
		}
		else {
			while (i < items) {
				//这里items 需要时3的倍数否则会出错
				long long o, edge_weight;
				RedisModuleCallReply* item; RedisModuleString* istr;
				item = RedisModule_CallReplyArrayElement(reply, i);
				istr = RedisModule_CreateStringFromCallReply(item);
				if (istr != NULL) RedisModule_StringToLongLong(istr, &o);
				RedisModule_FreeString(ctx, istr);
				RedisModule_FreeCallReply(item);

				i++;
				item = RedisModule_CallReplyArrayElement(reply, i);
				istr = RedisModule_CreateStringFromCallReply(item);
				if (istr != NULL) RedisModule_StringToLongLong(istr, &edge_weight);
				RedisModule_FreeString(ctx, istr);
				RedisModule_FreeCallReply(item);
				i++;

				sid_t ol_key = key_vpid_t(o, 0, LOCALITY, (dir_t)0);
				RedisModule_ThreadSafeContextLock(ctx);
				RedisModule_Call(ctx, "HINCRBY", "lll", (long long)ol_key,
					(long long)WORKLOAD, (long long)edge_weight);
				RedisModule_ThreadSafeContextUnlock(ctx);
				/*Edge_LOG*/
				mvin_edgelog(ctx, v, o);
			}

		}
	}
	RedisModule_FreeCallReply(reply);

 	return 1;
}

int _receive_counter(RedisModuleCtx *ctx, sid_t v){
	RedisModule_AutoMemory(ctx);
	
	int flag = 1;
	
	sid_t sc_key = key_vpid_t(v, 0, COUNTER, (dir_t)0);
	long long v_weight = 0;
	RedisModule_ThreadSafeContextLock(ctx);
	helios_HashGetOne(ctx, sc_key, (long long)WEIGHT, &v_weight);
	RedisModule_ThreadSafeContextUnlock(ctx);

	RedisModule_ThreadSafeContextLock(ctx);
	RedisModule_Call(ctx, "HSET", "lll", (long long)sc_key, (long long)PREV_REASSIGN_WEIGHT, v_weight);
	RedisModule_ThreadSafeContextUnlock(ctx);
	
	RedisModule_ThreadSafeContextLock(ctx);
	RedisModule_Call(ctx, "LPUSH", "cl", ACTIVE_OBJECT, (long long)v);
	RedisModule_ThreadSafeContextUnlock(ctx);

	RedisModule_ThreadSafeContextLock(ctx);
	RedisModule_Call(ctx, "HINCRBY", "cll", SERVER_INFO, (long long)WEIGHT, (long long)v_weight);
	RedisModule_ThreadSafeContextUnlock(ctx);

	//flag = flag && _receive_weight_locality(ctx, v); //this is for update edge_weight
	return flag;
}

void* Helios_ReceiveCounter_Thread(void *arg){
	pthread_detach(pthread_self());
	void **targ = arg;
	RedisModuleBlockedClient *bc = targ[0];
	sid_t vid = (sid_t)targ[1];
	RedisModule_Free(targ);

	RedisModuleCtx *ctx = RedisModule_GetThreadSafeContext(bc);
	//RedisModule_Log(ctx, "warning", "Receive Counter %ld start", vid);
	int flag = _receive_counter(ctx, vid);

	if(flag){
		//RedisModule_Log(ctx,"warning", "Receive Counter %ld succeed", vid);
		RedisModule_ReplyWithSimpleString(ctx, "OK");
	}
	else{ 
		RedisModule_Log(ctx,"warning", "Receive Counter %ld failed", vid);
		RedisModule_ReplyWithSimpleString(ctx, "ERR");
	}

	RedisModule_FreeThreadSafeContext(ctx);
	RedisModule_UnblockClient(bc, NULL);
	pthread_exit(0);
	return NULL;
}
////////////////////////////////////////////////////////////////////


/* On Counter db*/
int append_activeV(RedisModuleCtx *ctx, sid_t vid, int log_len){
	RedisModule_AutoMemory(ctx);	
	//RedisModule_ThreadSafeContextLock(ctx);
	//RedisModule_Call(ctx, "LREM", "cll", ACTIVE_OBJECT, (long long)1, vid);	
	//RedisModule_ThreadSafeContextUnlock(ctx);
	
	RedisModuleCallReply *reply;
	//RedisModule_ThreadSafeContextLock(ctx);
	sid_t sc_key = key_vpid_t(vid, 0, COUNTER, (dir_t)0);
	reply = RedisModule_Call(ctx, "HGET", "ll", sc_key, (long long) VERTEX_WEIGHT);

	if(RedisModule_CallReplyType(reply) == REDISMODULE_REPLY_NULL)
		RedisModule_Call(ctx, "INCR", "c", ACTIVE_OBJECT_SIZE);//ACTIVE_OBJECT_SIZE ++
	
	RedisModule_Call(ctx, "HINCRBY", "lll", sc_key, (long long)VERTEX_WEIGHT, (long long)1);
	RedisModule_Call(ctx, "LPUSH", "cl", ACTIVE_OBJECT, vid);//push to the head

	//RedisModule_ThreadSafeContextUnlock(ctx);

	if(log_len <=0)
		return 1;

	reply = RedisModule_Call(ctx, "GET", "c", ACTIVE_OBJECT_SIZE);
	if(RedisModule_CallReplyType(reply) != REDISMODULE_REPLY_STRING)
		return 0;
	RedisModuleString* active_object_size = RedisModule_CreateStringFromCallReply(reply);
	long long int real_log_len = 0;
	if (active_object_size != NULL)
		RedisModule_StringToLongLong(active_object_size, &real_log_len);
	while(real_log_len > log_len){
		//RedisModule_ThreadSafeContextLock(ctx);
		reply = RedisModule_Call(ctx, "RPOP", "c", ACTIVE_OBJECT); // pop the last one
		//RedisModule_ThreadSafeContextUnlock(ctx);
		
		RedisModuleString *drop_v = RedisModule_CreateStringFromCallReply(reply);
		long long drop_vid;
		if(drop_v == NULL)
			return 0;
		RedisModule_StringToLongLong(drop_v, &drop_vid);
		sid_t rd_key = key_vpid_t(drop_vid, 0, REASSIGN_DEL, (dir_t)0); //need to remove node number in log 
		long long v_del_log = 0;
		helios_HashGetOne(ctx, rd_key, (long long)VERTEX_WEIGHT, &v_del_log);
		if (v_del_log > 0) { //here is need del before
			RedisModule_Call(ctx, "HINCRBY", "lll", rd_key, (long long)VERTEX_WEIGHT, (long long)-1);
			continue;
		}
		//RedisModule_Call(ctx, "HINCRBY", "lll", rd_key, (long long)VERTEX_WEIGHT, v_log_weight);
		sid_t sc_key = key_vpid_t(drop_vid, 0, COUNTER, (dir_t)0);
		reply = RedisModule_Call(ctx, "HINCRBY", "lll", sc_key, (long long)VERTEX_WEIGHT,(long long)-1);
		if (reply == NULL || RedisModule_CallReplyType(reply) != REDISMODULE_REPLY_INTEGER) {
			//TODO ADD LOG
			RedisModule_Log(ctx, "warning", "vertext log del %ld failed", drop_vid);
			return 0;
		}
		/*
		RedisModuleString* v_count = RedisModule_CreateStringFromCallReply(reply);
		long long v_count_ll;
		RedisModule_StringToLongLong(v_count, &v_count_ll);
		*/
		if (RedisModule_CallReplyInteger(reply) ==0) {
			RedisModule_Call(ctx, "DECR", "c", ACTIVE_OBJECT_SIZE);
			real_log_len--;
			//RedisModule_ThreadSafeContextLock(ctx);
			RedisModule_Call(ctx, "HMSET", "lllll", (long long)sc_key, (long long)WEIGHT, (long long)0,
				(long long)PREV_REASSIGN_WEIGHT, (long long)0);
			//RedisModule_ThreadSafeContextUnlock(ctx);
			long long v_edgenum = 0, v_weight = 0;
			//RedisModule_ThreadSafeContextLock(ctx);
			helios_HashGetTwo(ctx, sc_key, (long long)EDGENUM, (long long)WEIGHT, &v_edgenum, &v_weight);
			//RedisModule_ThreadSafeContextUnlock(ctx);

			//RedisModule_ThreadSafeContextLock(ctx);
			RedisModule_Call(ctx, "HINCRBY", "cll", SERVER_INFO, (long long)WEIGHT, (long long)-v_weight);
			//RedisModule_ThreadSafeContextUnlock(ctx);

			//ADD DELETE DOI_INDEX INFO
			sid_t doi_key = key_vpid_t(drop_vid, 0, DoI_INDEX, (dir_t)0);
			RedisModule_Call(ctx, "DET", "l", (long long)doi_key);
		}

	}
	return 1;
}

void *Append_ActiveV_Thread(void *arg){
	void **targ = arg;
	RedisModuleBlockedClient *bc = targ[0];
	RedisModuleString **argv = targ[1];
	int argc = *(int*)targ[2];
	RedisModule_Free(targ);

	RedisModuleCtx *ctx = RedisModule_GetThreadSafeContext(bc);

	long long vid, vlog_len;
	RMUtil_ParseArgs(argv, argc, 1, "ll", &vid, &vlog_len);
	int flag = append_activeV(ctx, vid, vlog_len);

	RedisModule_Log(ctx,"warning", "Append Active Vertex %ld", vid);

	if(flag){
		RedisModule_Log(ctx,"warning", "Append Active Vertex %ld succeed", vid);
		RedisModule_ReplyWithSimpleString(ctx, "OK");
	}else{
		RedisModule_Log(ctx,"warning", "Append Active Vertex %ld failed", vid);
		RedisModule_ReplyWithSimpleString(ctx, "ERR");
	}
	RedisModule_FreeThreadSafeContext(ctx);
	RedisModule_UnblockClient(bc, NULL);
	return NULL;
}
// vertex weight is updated when the vertex is read
// vertex weight
// server weight
/*int _up_v_weight(RedisModuleCtx *ctx, sid_t src){
	sid_t sc_key = key_vpid_t(src, 0, COUNTER, (dir_t)0);
	//RedisModule_ThreadSafeContextLock(ctx);
	RedisModule_Call(ctx, "HINCRBY", "lll", (long long)sc_key, (long long)WEIGHT, (long long)1);
	RedisModule_Call(ctx, "HINCRBY", "cll", SERVER_INFO, (long long)WEIGHT, (long long)1);
	//RedisModule_Call(ctx, "LPUSH", "cl", ACTIVE_OBJECT, src);
	append_activeV(ctx, src, 0);
	//RedisModule_ThreadSafeContextUnlock(ctx);
	return 1;
}*/

void get_edge_name(sid_t s, sid_t t){
	char str_s[30]; char str_t[30];
	sprintf(str_s, "%ld", s);
	sprintf(str_t, "%ld", t);
	char edge[strlen(str_s) + strlen(str_t) + 2];
	char* sep = "-";
	strcpy(edge, str_s); strcat(edge, sep); strcat(edge, str_t);
}

//add new active edge to EdgeLog
int _append_edge(RedisModuleCtx *ctx, sid_t s, sid_t t, int log_len){
	RedisModule_AutoMemory(ctx);
	char str_s[30]; char str_t[30];
	sprintf(str_s, "%ld", s);
	sprintf(str_t, "%ld", t);
	char edge[strlen(str_s) + strlen(str_t) + 2];
	char* sep = "-";
	strcpy(edge, str_s); strcat(edge, sep); strcat(edge, str_t); 

	RedisModuleString *edge_str = RedisModule_CreateString(ctx, edge, strlen(edge));
	RedisModule_RetainString(ctx, edge_str);

	//RedisModule_ThreadSafeContextLock(ctx);
	RedisModule_Call(ctx, "LREM", "cls", EDGELOG, (long long)1, edge_str);
	//RedisModule_ThreadSafeContextUnlock(ctx);
	
	RedisModuleCallReply *reply;

	//RedisModule_ThreadSafeContextLock(ctx);
	reply = RedisModule_Call(ctx, "LPUSH", "cs", EDGELOG, edge_str);//push to the head
	//RedisModule_ThreadSafeContextUnlock(ctx);

	RedisModule_FreeString(ctx, edge_str);

	if(log_len <= 0)
		return 1;

	if(RedisModule_CallReplyType(reply) != REDISMODULE_REPLY_INTEGER)
		return 0;
	if(RedisModule_CallReplyInteger(reply) > log_len){
		//RedisModule_ThreadSafeContextLock(ctx);
		reply = RedisModule_Call(ctx, "RPOP", "c", EDGELOG); // pop the last one
		//RedisModule_ThreadSafeContextUnlock(ctx);
		
		RedisModuleString *drop_edge = RedisModule_CreateStringFromCallReply(reply);
		size_t len;
		const char* drop_edge_str = RedisModule_StringPtrLen(drop_edge, &len);
		char* running = RedisModule_Strdup(drop_edge_str);
		assert(running != NULL);
		RedisModule_Log(ctx, "warning", "Drop Edge weight %s %s ", drop_edge_str, running);
		char *src, *tgt, *p;
		src = strsep(&running, sep);
		tgt = strsep(&running, sep);
		sid_t src_t = strtol(src,&p,10), tgt_t = strtol(tgt,&p,10);
		
		RedisModule_Log(ctx, "warning", "SPlit %s %s %ld %ld", src, tgt, src_t, tgt_t);
		long long drop_l = 0;
		
		sid_t drop_ew_key = key_vpid_t(src_t, 0, EDGE_WEIGHT, (dir_t)0);
	
		//RedisModule_ThreadSafeContextLock(ctx);
		reply = RedisModule_Call(ctx, "HGET", "ll", (long long)drop_ew_key, (long long)tgt_t);
		//RedisModule_ThreadSafeContextUnlock(ctx);
		
		if(reply != NULL && RedisModule_CallReplyType(reply) != REDISMODULE_REPLY_STRING)
			return 0;
		RedisModuleString *drop_edge_weight = RedisModule_CreateStringFromCallReply(reply);
		if(drop_edge_weight != NULL)
			RedisModule_StringToLongLong(drop_edge_weight, &drop_l);

		//RedisModule_ThreadSafeContextLock(ctx);
		RedisModule_Call(ctx, "HDEL", "ll", (long long)drop_ew_key, (long long)tgt_t);
		//RedisModule_ThreadSafeContextUnlock(ctx);
		
		sid_t drop_tl_key = key_vpid_t(tgt_t, 0, LOCALITY, (dir_t)0);
		//RedisModule_ThreadSafeContextLock(ctx);
		RedisModule_Call(ctx, "HINCRBY", "lll", (long long)drop_tl_key, (long long)WORKLOAD, (long long)-drop_l);
		//RedisModule_ThreadSafeContextUnlock(ctx);
	}
	return 1;
}
// edge weight is updated when it is joint point
// edge weight
// locality
int up_edge_weight(RedisModuleCtx *ctx, sid_t src, sid_t tgt, int log_len){
	RedisModule_AutoMemory(ctx);
	
	sid_t tl_key = key_vpid_t(tgt, 0, LOCALITY, (dir_t)0);
	
	//RedisModule_ThreadSafeContextLock(ctx);
	RedisModule_Call(ctx, "HINCRBY", "lll", (long long)tl_key, (long long)WORKLOAD, (long long)1);
	//RedisModule_ThreadSafeContextUnlock(ctx);
	
	sid_t ew_key = key_vpid_t(src, 0, EDGE_WEIGHT, (dir_t)0);
	
	//RedisModule_ThreadSafeContextLock(ctx);
	RedisModule_Call(ctx, "HINCRBY", "lll", (long long)ew_key, (long long)tgt, (long long)1);
	//RedisModule_ThreadSafeContextUnlock(ctx);
	
	_append_edge(ctx, src, tgt, log_len);
	return 1;
}

void *Up_EdgeWeight_Thread(void *arg){
	void **targ = arg;
	RedisModuleBlockedClient *bc = targ[0];
	RedisModuleString **argv = targ[1];
	int argc = *(int*)targ[2];
	RedisModule_Free(targ);

	RedisModuleCtx *ctx = RedisModule_GetThreadSafeContext(bc);
	long long src, o, edgelog_len;
	RMUtil_ParseArgs(argv, argc, 1, "lll", &src, &o, &edgelog_len);
	
	RedisModule_Log(ctx,"warning", "Update Edge weight %ld %ld begin", src, o);
	
	int flag = up_edge_weight(ctx, src, o, edgelog_len);
	if(flag){
		RedisModule_Log(ctx,"warning", "Update Edge weight %ld  %ld succeed", src, o);
		RedisModule_ReplyWithSimpleString(ctx, "OK");
	}else{
		RedisModule_Log(ctx,"warning", "Update Edge weight %ld %ld failed", src, o);
		RedisModule_ReplyWithSimpleString(ctx, "ERR");
	}
	RedisModule_FreeThreadSafeContext(ctx);
	RedisModule_UnblockClient(bc, NULL);
	return NULL;
}
#endif
