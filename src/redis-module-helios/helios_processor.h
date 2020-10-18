#ifndef HELIOS_PROCESSOR_H
#define HELIOS_PROCESSOR_H

#include <stdlib.h>
#include "redismodule.h"
#include "redis-module-helios/rmutil/vector.h"
#include "redis-module-helios/rmutil/util.h"
#include "helios_structure.h"
#include "utils/type.h"

int cmp(const void *a, const void *b){
	return *(sid_t *)a - *(sid_t *)b;
}
typedef struct {
	int tag;
	Vector *bind_val;
	Vector* edges;
	
	char *src_inter_key;
	int new_inter_key;
}Node;

typedef struct{
	int tag;
	dir_t d;
	int joint;
	Vector *bind_val;
	Node *node;//Each edge will only have 1 node
	Vector *candi_src_bind;
	int edge_evaluated;

	char* candi_src_key;
	int new_union_key;
}Edge;

Node* NewNode(int tag){
	Node* node = RedisModule_Alloc(sizeof(Node));
	node->tag = tag;
	node->edges = NewVector(Edge*, 0);
	node->bind_val = NULL;
	node->src_inter_key = NULL;
	node->new_inter_key = 0;
	return node;
}

Edge* NewEdge(int tag, dir_t d, int joint){
	Edge* edge = RedisModule_Alloc(sizeof(Edge));
	edge->tag = tag;
	edge->d = d;
	edge->joint = joint;
	edge->node = NULL;
	edge->bind_val = NULL;
	edge->candi_src_bind = NULL;
	edge->edge_evaluated = 0;
	edge->candi_src_key = NULL;
	edge->new_union_key = 0;
	return edge;
}

int FreeEdge(RedisModuleCtx *ctx, Edge* edge);
int FreeNode(RedisModuleCtx *ctx, Node* node);
int printNode(RedisModuleCtx *ctx, Node *node, int level);
int printEdge(RedisModuleCtx *ctx, Edge *edge, int level);

int FreeEdge(RedisModuleCtx *ctx, Edge* edge){
	if(edge->bind_val != NULL){
		Vector_Free(edge->bind_val);
	}
	if(edge->candi_src_bind != NULL)
		Vector_Free(edge->candi_src_bind);
	if(edge->candi_src_key != NULL){
		if(edge->new_union_key == 1)
			helios_delKey(ctx, edge->candi_src_key);
		RedisModule_Free(edge->candi_src_key);
	}

	if(edge->node != NULL)
		FreeNode(ctx, edge->node);
	
	RedisModule_Free(edge);
	return 1;
}

int FreeNode(RedisModuleCtx *ctx, Node* node){
	if(node->bind_val != NULL){
		Vector_Free(node->bind_val);
	}

	if(node->src_inter_key != NULL){
		if(node->new_inter_key == 1)
			helios_delKey(ctx, node->src_inter_key);
		RedisModule_Free(node->src_inter_key);
	}

	int edge_num = Vector_Size(node->edges);
	for(int i = 0; i < edge_num; i++){
		Edge* edge;
		Vector_Get(node->edges, i, &edge);
		FreeEdge(ctx, edge);	
	}
	Vector_Free(node->edges);
	RedisModule_Free(node);
	return 1;
}

int printNode(RedisModuleCtx *ctx, Node* node, int level){
	RedisModule_Log(ctx, "warning", "level: %d, tag: %d ", level, node->tag);

	if(node->bind_val != NULL){
		int size = Vector_Cap(node->bind_val);
		RedisModule_Log(ctx, "warning", "%s", "{");
		for(int i = 0; i < size; i++){
			sid_t e;
			Vector_Get(node->bind_val, i, &e);
			RedisModule_Log(ctx, "warning","%d ", (int)e);
		}
		RedisModule_Log(ctx, "warning", "%s", "}");
	}

	level++;
	int edge_num = Vector_Cap(node->edges);
	RedisModule_Log(ctx, "warning", "edge num: %d", edge_num);
	for(int i = 0; i < edge_num; i++){
		Edge* edge;
		Vector_Get(node->edges, i, &edge);
		printEdge(ctx, edge, level);
	}
	return 1;
}

int printEdge(RedisModuleCtx *ctx, Edge* edge, int level){
	RedisModule_Log(ctx, "warning","tag: %d, dir: %d, joint: %d", edge->tag, (int)edge->d, edge->joint);
	
	if(edge->bind_val != NULL){
		int bind_num = Vector_Cap(edge->bind_val);
		RedisModule_Log(ctx, "warning","%d {", bind_num);
		for(int i = 0; i< bind_num; i++){
			sid_t e; Vector_Get(edge->bind_val, i, &e);
			RedisModule_Log(ctx, "warning","%d ", (int)e);
		}
		RedisModule_Log(ctx, "warning","%s ","}");
	}
	if(edge->candi_src_bind != NULL){
		printf("src_bind_num: %d ", Vector_Cap(edge->candi_src_bind));
	}
	if(edge->candi_src_key != NULL)
		RedisModule_Log(ctx, "warning","candi_src_key: %s, ", edge->candi_src_key);
	if(edge->node != NULL)	
		printNode(ctx, edge->node, level);
	return 1;
}

Node* parse_query_tree( RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
	long long num_t;//num of triple patterns
	RMUtil_ParseArgs(argv, argc, 1, "l", &num_t);

	int offset = 2 + num_t * 5;
	long long num_b;
	RMUtil_ParseArgs(argv, argc, offset, "l", &num_b);
	Vector* nodes = NewVector(void*, num_b);
	Vector* bind_vals = NewVector(Vector*, num_b);

	offset++;
	for(int i = 0; i < num_b; i++){
		long long b_num; 
		RMUtil_ParseArgs(argv, argc, offset, "l", &b_num);
		if(b_num == 0)
		{
			offset ++;
			continue;
		}
		Vector* b_val = NewVector(sid_t, b_num);
		for(int j = 0; j < b_num; j++){
			offset ++;
			long long b_v;
			RMUtil_ParseArgs(argv, argc, offset, "l", &b_v);
			Vector_Put(b_val, j, b_v);
		}
		qsort(b_val->data, b_num, sizeof(sid_t), cmp);
		Vector_Put(bind_vals, i, b_val);
		offset++;
	}

	Node* query_root;
	int query_syntax_error = 0;
	for(int i = 0; i < num_t; i++){
		long long s_t, p_t, o_t, dir, joint;
		RMUtil_ParseArgs(argv, argc, 2 + (i * 5), "lllll", &s_t,
				&p_t, &o_t, &dir, &joint);
		Node *s_n;
		Node *o_n;
		Edge *edge;
		if(Vector_Get(nodes, s_t, &s_n) == 0 || s_n == NULL){
			s_n = NewNode(s_t);
			Vector_Put(nodes, s_t, s_n);
			Vector* bind_val;
			if(Vector_Get(bind_vals, s_t, &bind_val) == 1){
				s_n->bind_val = bind_val;
			}
		}
		if(Vector_Get(nodes, o_t, &o_n) == 0 || o_n == NULL){
			o_n = NewNode(o_t);
			Vector_Put(nodes, o_t, o_n);
			Vector* bind_val;
			if(Vector_Get(bind_vals, o_t, &bind_val) == 1){
				o_n->bind_val = bind_val;
			}
		}
		if(Vector_Get(nodes, p_t, &edge) == 0 || edge == NULL){
			edge = NewEdge(p_t, dir, joint);
			Vector_Put(nodes, p_t, edge);
			Vector* bind_val;
			if(Vector_Get(bind_vals, p_t, &bind_val) == 1){
				edge->bind_val = bind_val;
			}else 
				query_syntax_error = 1;//predicate cannot be variable
		}
		if(edge->node != NULL) query_syntax_error = 1;
		if((dir_t)dir == OUT){
			edge->node = o_n;
			Vector_Push(s_n->edges, edge);
			query_root = s_n;
		}
		if((dir_t)dir == IN){
			query_root = o_n;
			edge->node = s_n;
			Vector_Push(o_n->edges, edge);
		}
	}
	Vector_Resize(query_root->edges, Vector_Size(query_root->edges));
	/*Not Vector_Free, just free the vector ptr*/
	free(nodes);
	free(bind_vals);
	
	if(query_syntax_error){
		RedisModule_Log(ctx, "notice", "%s", "Query syntax error- predicate no bind");
		FreeNode(ctx, query_root);
		return NULL;
	}else{
		return query_root;
	}
}
#endif
