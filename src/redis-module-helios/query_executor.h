#ifndef QUERY_EXECUTOR_H
#define QUERY_EXECUTOR_H

#include "helios_processor.h"
#include <assert.h>
#include "time.h"

/*
 * Union: may have duplication
 * add set2 to set1
 * */
int sort_rem_dup(Vector* set){
	int set_size = Vector_Cap(set);
	if(set_size < 1)
		return set_size;

	qsort(set->data, set_size, sizeof(sid_t), cmp);

	int prev_index = 0;
	sid_t prev_ele; Vector_Get(set, prev_index, &prev_ele);	
	for(int i = 1; i < Vector_Cap(set); i++){
		sid_t cur_ele; Vector_Get(set, i, &cur_ele);
		if(cur_ele == prev_ele)
			continue;
		prev_index++;
		if(i != prev_index){
			Vector_Put(set, prev_index, cur_ele);
		}
	}
	prev_index++;
	Vector_Resize(set, prev_index);
	return prev_index;
}

/*
 * set1 and set2 are already sorted
 * keep the intersection in set1
 * */
int intersect(Vector* set1, Vector* set2){
	int len = 0, m = 0;
	int len1 = Vector_Cap(set1);
	int len2 = Vector_Cap(set2);
	for(int i = 0; i < len1; i++){
		sid_t v_1;	
		Vector_Get(set1, i, &v_1);
		for(int j = m; j < len2; j++){
			sid_t v_2;
			Vector_Get(set2, j, &v_2);
			if(v_1 < v_2)
				break;
			if(v_1 == v_2){
				Vector_Put(set1, len, v_1);
				len++;
				m = j + 1;
				break;
			}
		}
	}
	Vector_Resize(set1, len);
	return len;
}

/*
 * set1 and set2 are already sorted
 * return a new set which is the intersect
 * */
Vector* intersect_new(Vector* set1, Vector* set2){
	int len = 0, m = 0;
	int len1 = Vector_Cap(set1), len2 = Vector_Cap(set2);
	Vector* results = NewVector(sid_t, 0);
	Vector *smaller, *larger;
	int s_len, l_len;
	if(len1 > len2){
		s_len = len2; l_len = len1;
		smaller = set2; larger = set1;
	}else{
		s_len = len1; l_len = len2;
		smaller = set1; larger = set2;
	}

	for(int i = 0; i < s_len; i++){
		sid_t v_1;	
		Vector_Get(smaller, i, &v_1);
		for(int j = m; j < l_len; j++){
			sid_t v_2;
			Vector_Get(larger, j, &v_2);
			if(v_1 < v_2)
				break;
			if(v_1 == v_2){
				Vector_Put(results, len, v_1);
				len++;
				m = j + 1;
				break;
			}
		}
	}
	if(len == 0){
		Vector_Free(results);
		return NULL;
	}else{
		return results;
	}
}

/*
 * Now only consider predicate canbe 1 or more values, but cannot be variable 
 * */
int evaluate_edge(RedisModuleCtx *ctx, Edge *edge){
	if(edge->joint != 1 && edge->joint != 3)
		return 0;
	if(edge->bind_val == NULL)  goto fail;
	
	int bind_num = Vector_Cap(edge->bind_val);
	if(bind_num < 1) goto fail;

	Vector* union_set = NewVector(sid_t, 0);
	int num_sets = 0;
	for(int i = 0; i<bind_num; i++){
		sid_t p;
		Vector_Get(edge->bind_val, i, &p);
		
		if(p == 1 && edge->node->bind_val == NULL) goto fail;
		if(p != 1 && edge->node->bind_val == NULL){
			sid_t key_t = key_vpid_t(0, p, INDEX, edge->d);
			helios_SetUnion(ctx, key_t, union_set);
			num_sets++;
			continue;
		}
		int ov_num = Vector_Cap(edge->node->bind_val);
		for(int k = 0; k <ov_num; k++){
			sid_t o;
			Vector_Get(edge->node->bind_val, k, &o);
			// use the type value as the new preicate
			sid_t key_t;
			if(p == 1)
				key_t = key_vpid_t(0, o, INDEX, edge->d);
			else{ 
				key_t = key_vpid_t(o, p, INDEX, inverse_dir(edge->d));
				RedisModule_Log(ctx, "warning", "%ld %ld %ld", o, p, key_t);
			}
			helios_SetUnion(ctx, key_t, union_set);
			num_sets ++;
		} 
	}
	if(num_sets < 1)
		return 0;
	if(num_sets > 1)
		sort_rem_dup(union_set);
	edge->candi_src_bind = union_set;
	edge->edge_evaluated = 1;
	return 1;
fail:
	RedisModule_Log(ctx, "warning", "%s", 
			"More than 1 predicate OR predicate not well binded");
	return 0;
}


Vector* _traverse_vertex(RedisModuleCtx *ctx, sid_t src, sid_t predicate,
		dir_t d, Edge *edge){
	RedisModule_AutoMemory(ctx);
	_up_v_weight(ctx, src);
	sid_t sp_key = key_vpid_t(src, predicate, NO_INDEX, d);

	RedisModuleCallReply *reply = RedisModule_Call(ctx, "GET", "l", sp_key);
	RedisModule_ReplyWithCallReply(ctx, reply);
	RedisModule_FreeCallReply(reply);
	return NULL;
}

/* 
 * Assume predicate have one and only one binding
 * */
int traverse_edge(RedisModuleCtx *ctx, sid_t src, Vector *edges, int *arraylen){
	if(edges == NULL) goto end;//no binding in predicate
	(*arraylen)++;
	RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
	int v_arraylen = 0;
	RedisModule_ReplyWithLongLong(ctx, src);
	v_arraylen++;
	int e_size = Vector_Cap(edges);
	if(e_size == 0) goto end;
	for(int j =0; j< e_size; j++){
		Edge *edge;
		Vector_Get(edges, j, &edge);
		if(edge->joint != 2 && edge->joint != 3)
			continue;
		int e_bind_num = Vector_Cap(edge->bind_val);
		for (int i = 0; i < e_bind_num; i++){
			sid_t p; 
			Vector_Get(edge->bind_val, i, &p);
			RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
			int p_arraylen = 0;
			RedisModule_ReplyWithLongLong(ctx, p);
			p_arraylen ++;
			_traverse_vertex(ctx, src, p, edge->d,edge);
			p_arraylen++;
			RedisModule_ReplySetArrayLength(ctx, p_arraylen);
			v_arraylen++;
		}
	}	
	RedisModule_ReplySetArrayLength(ctx, v_arraylen);
	return 1;
end:
	return 0;
}

int preprocess_edges(RedisModuleCtx* ctx, Node* root){
	int e_size = Vector_Cap(root->edges);
	if(e_size < 1) goto fail;

	int min_src_size = -1, min_src_index = -1;
	int num_edges = 0;

	//For each predicate, get the union set
	for(int i = 0; i < e_size; i++){
		Edge* edge;
		Vector_Get(root->edges, i, &edge);
		int flag = evaluate_edge(ctx, edge);
		if(!flag)
			continue;
		num_edges ++;
		if(min_src_size == -1 
			|| Vector_Cap(edge->candi_src_bind) < min_src_size){
			min_src_index = i;
			min_src_size = Vector_Cap(edge->candi_src_bind);
		}
	}
	if(num_edges <= 1)
		return min_src_index;
	
	//Get the intersection set
	Edge *min_edge; Vector_Get(root->edges, min_src_index, &min_edge);
	Vector* min_set = min_edge->candi_src_bind;
	for(int i = 0; i < e_size; i++){
		Edge* edge;
		Vector_Get(root->edges, i, &edge);
		if(edge->edge_evaluated != 1 || i == min_src_index)
			continue;
		intersect(min_set, edge->candi_src_bind);
	}
	return min_src_index;
fail:
	RedisModule_Log(ctx, "notice", "%s", "No predicate");
	return -1;
}

int pattern_evaluation(RedisModuleCtx* ctx, Node* root){
	//check if the edge has bindings

	clock_t start = clock();
	Vector *src_bind;
	if(root->bind_val != NULL){
		src_bind = root->bind_val;
		goto without_preprocess;
	}
	int min_src_index = preprocess_edges(ctx, root);
	RedisModule_Log(ctx, "warning", "step1: %d", (int)(clock()-start));

	if(min_src_index < 0) goto end;

	//RedisModule_ThreadSafeContextLock(ctx);
	Edge *min_edge; Vector_Get(root->edges, min_src_index, &min_edge);
	src_bind = min_edge->candi_src_bind;
	RedisModule_Log(ctx, "warning", "min src size %d", Vector_Cap(src_bind));
	//RedisModule_ThreadSafeContextUnlock(ctx);

	if(src_bind == NULL) goto end;
	
	RedisModule_Log(ctx, "warning", "step2: %d",(int)(clock()-start));
	
without_preprocess:
	//Given the binding of the src, start to travse each edge, and find the tgt object
	// For each value in src_bind, scan all edges and check tgt binding
	RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
	int arraylen = 0;
	int bind_num = Vector_Cap(src_bind); 
	RedisModule_Log(ctx, "warning","bind-num %d", bind_num);
	for(int i = 0; i < bind_num; i++){
		sid_t src;
		Vector_Get(src_bind, i, &src);
		traverse_edge(ctx, src, root->edges, &arraylen);
	}
	RedisModule_Log(ctx, "warning", "step3: %d ", (int)(clock()-start));
	RedisModule_ReplySetArrayLength(ctx, arraylen);
	return 1; 
end:
	RedisModule_Log(ctx, "warning", "%s", "Traverse Fails");
	return 0;
}

void* Helios_Pattern_Evaluate_Thread(void *arg){
	void **targ = arg;
	RedisModuleBlockedClient *bc = targ[0];
	Node *query_root = (Node *)targ[1];
	RedisModule_Free(targ);
	
	RedisModuleCtx *ctx = RedisModule_GetThreadSafeContext(bc);

	int flag = pattern_evaluation(ctx, query_root);
	FreeNode(ctx, query_root);
	
	if(!flag)
		RedisModule_ReplyWithSimpleString(ctx, "ERR");

	RedisModule_FreeThreadSafeContext(ctx);
	RedisModule_UnblockClient(bc, NULL);
	return NULL;
}
#endif
