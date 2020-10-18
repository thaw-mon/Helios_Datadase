#include "Database.hpp"
#include <math.h>
#include <omp.h>

//#include "RDF.hpp"
bool Database::_add_triple(int initial_node, const triple_t &triple){
	triple_store[initial_node]->insert_triple(triple);
	return true;	
}
map<int, vector<sid_t>> Database::get_initial_server(const vector<sid_t> &v){
	map<int, vector<sid_t>> initial_servers;
	for(int i = 0; i < v.size(); i++){
		int init_s = jumpConsistentHash(v[i], num_servers);
		if(initial_servers.find(init_s) == initial_servers.end()){
			initial_servers.insert(pair<int, vector<sid_t>>(init_s, vector<sid_t>()));
		}
		initial_servers[init_s].push_back(v[i]);
	}
	return initial_servers;
}

/*Load data*/
//this fuction do nothing need del
bool Database::load_data(const vector<itriple_t> &spo, dir_t d){
	uint64_t s = 0;
        while (s < spo.size()) {
            // predicate-based key (subject + predicate)
            uint64_t e = s + 1;
            while ((e < spo.size())
                    && (spo[s].s == spo[e].s)
                    && (spo[s].p == spo[e].p))  { e++; }
            
	    // insert vertex
            sid_t key = key_vpid_t(spo[s].s, spo[s].p, NO_INDEX,d);

	    int sz = e - s;
	    int* val = (int*)malloc(sizeof(int) * sz);
	    //setobj* val = (setobj*)malloc(sizeof(setobj*));

	    int off = 0;
            // insert edges
            for (uint64_t i = s; i < e; i++)
            	val[off++] = spo[i].o;

	   //insert to redis 
            s = e;
        }
}
/*************vertex location******************/
int Database::get_global_vloc(sid_t v){
	int initial_server = jumpConsistentHash(v, num_servers);
	int host_server = triple_store[initial_server]->get_v_loc(v);
	if(host_server == -1)
		return initial_server; // not reassigned, on the initial server
	else return host_server;
}

int Database::set_v_loc(sid_t v, int loc){
	int initial_server = jumpConsistentHash(v, num_servers);
	if(loc == initial_server){
		//printf("Vertex %ld Loc = initial %d\n", v, initial_server);
		triple_store[initial_server]->del_v_loc(v);
	}
	else {
		//printf("Vertex %ld Loc initial = %d, current = %d\n", v, initial_server,loc);
		triple_store[initial_server]->set_v_loc(v, loc);
	}
}

map<int, vector<sid_t>> Database::batch_get_global_vloc(const vector<sid_t> &v){
	//sid_t start = get_usec();
	map<int, vector<sid_t>> initial_servers = get_initial_server(v);
	map<int, vector<sid_t>> real_locs;
	for(int i = 0; i < num_servers; i++){
		if(initial_servers.find(i) == initial_servers.end()){
			continue;
		}
		async_store[i][0]->batch_getloc(initial_servers[i], &real_locs);
	}
	//sid_t end = get_usec();
	//printf("batch_get_global_vloc spend time = %ld s, size = %d\n", (end - start) / 1000000, v.size());
	return real_locs;
}
//add new batch_get_global_vloc
map<sid_t, int>  Database::batch_get_global_vloc2(const vector<sid_t>& v) {
	map<int, vector<sid_t>> initial_servers = get_initial_server(v);
	map<sid_t, int> real_locs;
	for (int i = 0; i < num_servers; i++) {
		if (initial_servers.find(i) == initial_servers.end()) {
			continue;
		}
		async_store[i][0]->batch_getloc3(initial_servers[i], &real_locs);
	}
	return real_locs;
}

/*****************************predicate info************************************/
int Database::get_p_counter(sid_t p, int *scounter, int *ocounter){
	return mem->get_p_counter(p, scounter, ocounter, this);
}

vector<int> Database::get_p_loc(sid_t p, dir_t d){
	return mem->get_p_loc(p, d);
}

int Database::get_global_pcounter(sid_t p, int *counters, 
		vector<int> *p_out_locs, vector<int> *p_in_locs){

	vector<vector<int>> pcounters; pcounters.resize(num_servers);
	#pragma omp parallel for num_threads(num_servers)
	for(int i = 0; i < num_servers; i++){
		async_store[i][0]->get_p_counter(p, &pcounters[i]);
		assert(pcounters[i].size() == 2);
	}

	for(int i = 0; i < num_servers; i++){
		int scounter = pcounters[i][0], ocounter = pcounters[i][1];
		printf("pcounter %d %d %d \n", p, scounter, ocounter);
		if(scounter > 0){
			counters[0] += scounter;
			p_out_locs->push_back(i);
		}
		if(ocounter > 0){
			counters[1] += ocounter;
			p_in_locs->push_back(i);
		}
	}

	return 1; 
}

/************************** Begin: execute patterns on client-side *****************************************/
QueryNode* Database::parse_patterns(const vector<triple_t> &triples, const vector<vector<sid_t>> &binds){
	int triple_num = triples.size();
	int tag_num = binds.size();
	vector<void*> query_nodes; query_nodes.resize(tag_num);
	QueryNode* queryRoot;

	for(int i = 0; i < triple_num; i++){
		int s_t = triples[i].s;
		QueryNode *snode = (QueryNode*)query_nodes[s_t];
		if(snode == NULL){
			snode = new QueryNode(s_t, binds[s_t]);
			query_nodes[s_t] = snode;
		}

		int p_t = triples[i].p;
		QueryEdge* edge;
		if(query_nodes[p_t] == NULL){
			edge = new QueryEdge(p_t, triples[i].d, 1, binds[p_t]);
query_nodes[p_t] = edge;
		}
else printf("ERR: duplicate predicate tag in query pattern\n");

int o_t = triples[i].o;
QueryNode* onode = (QueryNode*)query_nodes[o_t];
if (onode == NULL) {
	onode = new QueryNode(o_t, binds[o_t]);
	query_nodes[o_t] = onode;
}

if (triples[i].d == OUT) {
	queryRoot = snode;
	(snode->edges).push_back(edge);
	edge->node = onode;
}
else {
	queryRoot = onode;
	(onode->edges).push_back(edge);
	edge->node = snode;
}
	}
	return queryRoot;
}

//获取local对应的scr_bind数据
int Database::evaluate_root_binding_local(QueryNode* queryRoot, vector<sid_t>* src_bind) {
	queryRoot->init_interunion_key(num_servers);
	int flag = queryRoot->preprocess(sid, async_store[sid][0]);
	if (flag) {
		async_store[sid][0]->read_zset((queryRoot->src_inter_key[sid]), src_bind);
		return 1;
	}
	return 0;
}

int Database::_client_pattern_evaluate(int tgt_s, vector<sid_t> src_bind, QueryNode* queryRoot,
	vector<triple_t>* results) {

	vector<sid_t> inter_sets;   
	sid_t start = get_usec();

	int flag = queryRoot->preprocess(tgt_s, async_store[tgt_s][0]);
	if (flag) {
		async_store[tgt_s][0]->read_zset((queryRoot->src_inter_key[tgt_s]), &inter_sets);
	}
	
	//打印inter_sets
	//cout << "node = " << queryRoot->name.c_str() << "inter_sets size = " << inter_sets.size() << "src_bind.size() = " << src_bind.size() << endl;
	if (src_bind.size() != 0)
		if (inter_sets.size() != 0) {
			//printf("@%d inter_sets size %d src_bind size %d\n", sid, inter_sets.size(), src_bind.size());
			//获取集合的交集
			inter_sets = intersect(inter_sets, src_bind);
			if (inter_sets.size() != src_bind.size()) {
				vector<sid_t> bind_prune = difference(src_bind, inter_sets);
				//printf("@%d src_bind %d src_num %d prune bind size %d\n", sid, src_bind.size(), inter_sets.size(), bind_prune.size());
				queryRoot->insert_prune_bind(bind_prune);
			}
		}
		else
			inter_sets = src_bind;
	else if (inter_sets.size() != 0)
		queryRoot->insert_bind(inter_sets);

	if (inter_sets.size() == 0) {
		printf("Node is not bound or preprocess doesnot find ant binding!\n ");
		return 0;
	}
	//sid_t end1 = get_usec();
	//printf("node %s , preprocess spend time %d ms\n", queryRoot->name.c_str(),(end1 - start) / 1000);
	int thread_num;
	int edge_sz = (queryRoot->edges).size();
	if (edge_sz < NUM_CONNECT)   thread_num = edge_sz;
	else thread_num = NUM_CONNECT;
	vector<vector<triple_t>> res; res.resize(edge_sz);
#pragma omp parallel for num_threads(thread_num)
	for (int j = 0; j < edge_sz; j++) {
		QueryEdge* edge = (queryRoot->edges)[j];

		int aid = j % thread_num;
		//printf("@%d connect %d thread %d run edge %d\n", sid, tgt_s, aid, j);
;		async_store[tgt_s][aid]->traverse_vertex(queryRoot, inter_sets,
			edge, &(res[j]));
		//TODO use batch_traverse_vertex test
		/*async_store[tgt_s][aid]->batch_traverse_vertex(queryRoot, inter_sets,
			edge, &(res[j]));*/
		//printf("@%d thread %d edge %d local_results %d and node name = %s, node size = %d\n", sid, aid, j, res[j].size(), queryRoot->name.c_str(),inter_sets.size());
	}
	for (int j = 0; j < edge_sz; j++) {
		if (res[j].size() <= 0)
			continue;
		if ((queryRoot->edges)[j]->towardsJoint) {
			//(queryRoot->edges)[j]->result = res[j];
			//优化策略：基类results的位置 
			//(queryRoot->edges)[j]->result_begin = results->size();
			//(queryRoot->edges)[j]->result_size = res[j].size();
			(queryRoot->edges)[j]->result.insert((queryRoot->edges)[j]->result.end(), res[j].begin(), res[j].end());
		}
		results->insert(results->end(), res[j].begin(), res[j].end());

		//reassign 
		if (cfg->enable_reassign && (queryRoot->edges)[j]->towardsJoint){  // && res[j].size() <= 500000){
			//mem->reassign_queue->push_weight_task(sid, new WeightUpdateTask(queryRoot->root_vertex, res[j]));
			mem->reassign_queue->push_weight_task(tgt_s, new WeightUpdateTask(res[j]));
			//updateDoIEdges.insert(updateDoIEdges.end(), res[j].begin(), res[j].end());
		}
	}

	//sid_t end3 = get_usec();
	//printf("node %s , merge data process spend time %d ms\n", queryRoot->name.c_str(), (end3 - end2) / 1000);
	if (cfg->enable_reassign) {
		//here may should update bind size not inter_sets
		//printf("node = %s ,inter_sets size = %d, src_bind size = %d, edge size = %d\n", queryRoot->name.c_str(),inter_sets.size(), src_bind.size(), results->size());
		mem->reassign_queue->push_weight_task(tgt_s, new WeightUpdateTask(src_bind, edge_sz));
	}
		
	sid_t end = get_usec();
	printf("src_bind size= %d, inte_set size = %d\n", src_bind.size(), inter_sets.size());
	printf("server %d node %s get %d pattern evaluate time %lf s\n", tgt_s, queryRoot->name.c_str(),
			results->size(), (float)(end -start) /1000000);

	return 1;
}

//
/*
******************** insert DoI index info**************************
for one edge [i,j] we just both update DoI[i] and DoI[j]
and we need record v[i] all node
*/
void Database::insert_DoI_index(vector<triple_t> tripple_res, int tgt_s) {
	//1.创建DoI index 格式  sid_t sc_key = key_vpid_t(tri.s, 0, COUNTER, (dir_t)0);
	//call RedisModule_Call(ctx, "HINCRBY", "lll", (long long)sc_key, (long long)tgt_s, (long long)1);
	vector<vector<vector<sid_t>>> DoI_index_node_target; //DoI[i][j] 标识存储node i的数据 x 的的第DoI FIELD j add 1
	vector<vector<map<sid_t, int>>> DoI_index_node_map;
	DoI_index_node_target.resize(num_servers);
	DoI_index_node_map.resize(num_servers);
	for (int i = 0; i < num_servers; i++) {
		DoI_index_node_target[i].resize(num_servers);
		DoI_index_node_map[i].resize(num_servers);
	}

	sid_t start = get_usec();
	for (triple_t tri : tripple_res) {
		//we first get the Info veretex s
		int sLoc = get_global_vloc(tri.s);
		//1.get o_vals loc 
		vector<sid_t> o_vals = tri.o_vals;
		//for source DoI
		if (DoI_index_node_map[sLoc][sLoc].find(tri.s) == DoI_index_node_map[sLoc][sLoc].end()) {
			DoI_index_node_map[sLoc][sLoc][tri.s] = o_vals.size();
		}
		else {
			int tmp = DoI_index_node_map[sLoc][sLoc][tri.s];
			DoI_index_node_map[sLoc][sLoc][tri.s] = o_vals.size() + tmp;
		}
		map<int, vector<sid_t>> v_loc_map = batch_get_global_vloc(o_vals); //获取o_vals对应节点的位置
		for (int i = 0; i < num_servers; i++) {
			if (v_loc_map.find(i) == v_loc_map.end()) {
				continue;
			}
			//for target DoI 
			DoI_index_node_target[i][sLoc].insert(DoI_index_node_target[i][sLoc].end(), v_loc_map[i].begin(), v_loc_map[i].end());
		}
	}
	sid_t end = get_usec();
	printf("Test : insert_DoI_index get all data  spend Time  = %d second\n", (end - start) / 1000000);

	start = get_usec();
	//Hincrby sc_key node increase HINCRBY KEY_NAME FIELD_NAME INCR_BY_NUMBER 
	//int thread_num;
	//if (num_servers < NUM_CONNECT)   thread_num = num_servers;
	//else thread_num = NUM_CONNECT;
	//#pragma omp parallel for num_threads(thread_num)
	for (int i = 0; i < num_servers; i++) {
		sid_t start1 = get_usec();
		counter_store[i]->batch_insert_DoI_index(DoI_index_node_map[i]);
		counter_store[i]->batch_insert_DoI_index(DoI_index_node_target[i]);
		sid_t end1 = get_usec();
		int mapSize = 0, targetSize = 0;
		for (int j = 0; j < num_servers; j++) {
			mapSize += DoI_index_node_map[i][j].size();
			targetSize += DoI_index_node_target[i][j].size();
		}
		printf("Tests exp1 spend  %d second: sid = %d, map size = %d target all size = %d\n", (end1 - start1) / 1000000, i, mapSize, targetSize);
	}
	end = get_usec();
	printf("Test : insert_DoI_index to redis spend Time  = %d second\n", (end - start) / 1000000);
}

//this way get all o_vals node direct
//this way get all loc spend 40 second and last way spend 300 second, 有十倍的提升
//but in insert data to redis spend to mush time need improve
void Database::insert_DoI_index2(vector<triple_t> tripple_res, int tgt_s) {
	//1.创建DoI index 格式  sid_t sc_key = key_vpid_t(tri.s, 0, COUNTER, (dir_t)0);
	//call RedisModule_Call(ctx, "HINCRBY", "lll", (long long)sc_key, (long long)tgt_s, (long long)1);
	vector<vector<vector<sid_t>>> DoI_index_node_target; //DoI[i][j] 标识存储node i的数据 x 的的第DoI FIELD j add 1
	vector<vector<map<sid_t, int>>> DoI_index_node_map;
	DoI_index_node_target.resize(num_servers);
	DoI_index_node_map.resize(num_servers);
	for (int i = 0; i < num_servers; i++) {
		DoI_index_node_target[i].resize(num_servers);
		DoI_index_node_map[i].resize(num_servers);
	}
	sid_t start = get_usec();
	//
	vector<sid_t> o_vals_all;
	for (triple_t triple : tripple_res) {
		o_vals_all.push_back(triple.s);
		o_vals_all.insert(o_vals_all.end(), triple.o_vals.begin(), triple.o_vals.end());
	}
	sort_rem_dup(&o_vals_all);
	map<int, vector<sid_t>> all_loc_map = batch_get_global_vloc(o_vals_all); //get o_vals loc

	//push loc_map --> map [key = v, value = loc]
	map<sid_t, int> value_map;
	for (int i = 0; i < num_servers; i++) {
		if (all_loc_map.find(i) == all_loc_map.end()) {
			continue;
		}
		vector<sid_t> o_loc_vals = all_loc_map[i];
		for (sid_t o : o_loc_vals) {
			value_map[o] = i;
		}
	}
	sid_t end = get_usec();
	printf("Test : insert_DoI_index2 get loc  spend Time  = %d second\n", (end - start) / 1000000);
	for (triple_t tri : tripple_res) {
		//we first get the Info veretex s
		int sLoc = value_map[tri.s];
		//1.get o_vals loc 
		vector<sid_t> o_vals = tri.o_vals;
		//for source DoI
		if (DoI_index_node_map[sLoc][sLoc].find(tri.s) == DoI_index_node_map[sLoc][sLoc].end()) {
			DoI_index_node_map[sLoc][sLoc][tri.s] = o_vals.size();
		}
		else {
			int tmp = DoI_index_node_map[sLoc][sLoc][tri.s];
			DoI_index_node_map[sLoc][sLoc][tri.s] = o_vals.size() + tmp;
		}

		for (sid_t o : o_vals) {
			int oLoc = value_map[o];
			DoI_index_node_target[oLoc][sLoc].push_back(o);
		}
	}

	end = get_usec();
	printf("Test : insert_DoI_index2 get all data  spend Time  = %d second\n", (end - start) / 1000000);

	start = get_usec();
	for (int i = 0; i < num_servers; i++) {
		sid_t start1 = get_usec();
		counter_store[i]->batch_insert_DoI_index(DoI_index_node_map[i]);
		counter_store[i]->batch_insert_DoI_index(DoI_index_node_target[i]);
		sid_t end1 = get_usec();
		int mapSize = 0, targetSize = 0;
		for (int j = 0; j < num_servers; j++) {
			mapSize += DoI_index_node_map[i][j].size();
			targetSize += DoI_index_node_target[i][j].size();
		}
		printf("Tests exp2 spend  %d second: sid = %d, map size = %d target all size = %d\n",(end1 - start1) / 1000000,i, mapSize, targetSize);
	}
	end = get_usec();
	printf("Test : insert_DoI_index2 to redis spend Time  = %d second\n", (end - start) / 1000000);
}

void Database::insert_DoI_index3(vector<triple_t> tripple_res, int tgt_s) {
	//1.创建DoI index 格式  sid_t sc_key = key_vpid_t(tri.s, 0, COUNTER, (dir_t)0);
	//call RedisModule_Call(ctx, "HINCRBY", "lll", (long long)sc_key, (long long)tgt_s, (long long)1);
	vector<vector<vector<sid_t>>> DoI_index_node_target; //DoI[i][j] 标识存储node i的数据 x 的的第DoI FIELD j add 1
	vector<vector<map<sid_t, int>>> DoI_index_node_map;
	DoI_index_node_target.resize(num_servers);
	DoI_index_node_map.resize(num_servers);
	for (int i = 0; i < num_servers; i++) {
		DoI_index_node_target[i].resize(num_servers);
		DoI_index_node_map[i].resize(num_servers);
	}
	sid_t start = get_usec();
	//
	vector<sid_t> o_vals_all;
	for (triple_t triple : tripple_res) {
		o_vals_all.push_back(triple.s);
		o_vals_all.insert(o_vals_all.end(), triple.o_vals.begin(), triple.o_vals.end());
	}
	sort_rem_dup(&o_vals_all);
	//push loc_map --> map [key = v, value = loc]
	map<sid_t, int> value_map = batch_get_global_vloc2(o_vals_all);
	sid_t end = get_usec();
	printf("Test : insert_DoI_index3 get loc  spend Time  = %d second\n", (end - start) / 1000000);
	for (triple_t tri : tripple_res) {
		//we first get the Info veretex s
		int sLoc = value_map[tri.s];
		//1.get o_vals loc 
		vector<sid_t> o_vals = tri.o_vals;
		//for source DoI
		if (DoI_index_node_map[sLoc][sLoc].find(tri.s) == DoI_index_node_map[sLoc][sLoc].end()) {
			DoI_index_node_map[sLoc][sLoc][tri.s] = o_vals.size();
		}
		else {
			int tmp = DoI_index_node_map[sLoc][sLoc][tri.s];
			DoI_index_node_map[sLoc][sLoc][tri.s] = o_vals.size() + tmp;
		}

		for (sid_t o : o_vals) {
			int oLoc = value_map[o];
			DoI_index_node_target[oLoc][sLoc].push_back(o);
		}
	}

	end = get_usec();
	printf("Test : insert_DoI_index3 get all data  spend Time  = %d second\n", (end - start) / 1000000);

	start = get_usec();
	//考虑在这里进行优化测试 每轮需要 spend 1000 second  太多了
	//1.考虑使用同步redis测试 in function batch_insert_DoI_index(DoI_index_node_map[i]);
	//2.考虑使用redis-module实现
	//This test spend most time 
	for (int i = 0; i < num_servers; i++) {
		sid_t start1 = get_usec();
		counter_db[i]->batch_insert_DoI_index(DoI_index_node_map[i]);
		counter_db[i]->batch_insert_DoI_index(DoI_index_node_target[i]);
		sid_t end1 = get_usec();
		int mapSize = 0, targetSize = 0;
		for (int j = 0; j < num_servers; j++) {
			mapSize += DoI_index_node_map[i][j].size(); 
			targetSize += DoI_index_node_target[i][j].size();
		}
		printf("Tests exp3 spend  %d second: sid = %d, map size = %d target all size = %d\n", (end1 - start1) / 1000000, i, mapSize, targetSize);
	}
	end = get_usec();
	printf("Test : insert_DoI_index3 to redis spend Time  = %d second\n", (end - start) / 1000000);
}

void Database::insert_DoI_index4(vector<triple_t> tripple_res, int tgt_s) {
	//1.创建DoI index 格式  sid_t sc_key = key_vpid_t(tri.s, 0, COUNTER, (dir_t)0);
	//call RedisModule_Call(ctx, "HINCRBY", "lll", (long long)sc_key, (long long)tgt_s, (long long)1);
	vector<vector<vector<sid_t>>> DoI_index_node_target; //DoI[i][j] 标识存储node i的数据 x 的的第DoI FIELD j add 1
	vector<map<sid_t, int>> DoI_index_node_map;
	DoI_index_node_target.resize(num_servers);
	DoI_index_node_map.resize(num_servers);
	for (int i = 0; i < num_servers; i++) {
		DoI_index_node_target[i].resize(num_servers);
		//DoI_index_node_map[i].resize(num_servers);
	}
	//sid_t start = get_usec();
	//
	vector<sid_t> o_vals_all;
	for (triple_t triple : tripple_res) {
		o_vals_all.push_back(triple.s);
		o_vals_all.insert(o_vals_all.end(), triple.o_vals.begin(), triple.o_vals.end());
	}
	//sort_rem_dup(&o_vals_all);
	//push loc_map --> map [key = v, value = loc]
	map<sid_t, int> value_map = batch_get_global_vloc2(o_vals_all);
	//sid_t end = get_usec();
	//printf("Test : insert_DoI_index4 get loc  spend Time  = %d second\n", (end - start) / 1000000);
	for (triple_t tri : tripple_res) {
		//we first get the Info veretex s
		int sLoc = value_map[tri.s];
		//1.get o_vals loc 
		vector<sid_t> o_vals = tri.o_vals;
		//for source DoI
		if (DoI_index_node_map[sLoc].find(tri.s) == DoI_index_node_map[sLoc].end()) {
			DoI_index_node_map[sLoc][tri.s] = o_vals.size();
		}
		else {
			int tmp = DoI_index_node_map[sLoc][tri.s];
			DoI_index_node_map[sLoc][tri.s] = o_vals.size() + tmp;
		}

		for (sid_t o : o_vals) {
			int oLoc = value_map[o];
			DoI_index_node_target[oLoc][sLoc].push_back(o);
		}
	}

	//end = get_usec();
	//printf("Test : insert_DoI_index4 get all data  spend Time  = %d second\n", (end - start) / 1000000);

	//start = get_usec();
	//2.考虑使用redis-module实现
	for (int i = 0; i < num_servers; i++) {
		//async_store[i][0]->batch_insert_DoI_index(DoI_index_node_map[i],i);
		//async_store[i][0]->batch_insert_DoI_index(DoI_index_node_target[i]);
		counter_store[i]->batch_insert_DoI_index(DoI_index_node_map[i], i);
		counter_store[i]->batch_insert_DoI_index(DoI_index_node_target[i]);
	}
	//end = get_usec();
	//printf("Test : insert_DoI_index4 to redis spend Time  = %d second\n", (end - start) / 1000000);
}


//this function spend too mush time should update
void Database::insert_DoI_index_new(vector<triple_t> tripple_res, int tgt_s) {
	//add time count;
	sid_t start = get_usec();
	vector<vector<vector<pair<sid_t,sid_t>>>> DoI_index_node_out; 
	vector<vector<vector<pair<sid_t, sid_t>>>> DoI_index_node_in;
	//DoI_index_node.resize(num_servers);
	DoI_index_node_out.resize(num_servers);
	DoI_index_node_in.resize(num_servers);
	for (int i = 0; i < num_servers; i++) {
		//DoI_index_node[i].resize(num_servers);
		DoI_index_node_out[i].resize(num_servers);
		DoI_index_node_in[i].resize(num_servers);
	}
	//printf("start insert_DoI_index_new sid = %d\n", tgt_s);

	for (triple_t tri : tripple_res) {
		//we first get the Info veretex s
		int sLoc = get_global_vloc(tri.s);
		
		//1.get o_vals loc 
		vector<sid_t> o_vals = tri.o_vals;
		map<int, vector<sid_t>> v_loc_map = batch_get_global_vloc(o_vals); //get o_vals loc 
		
		for (int i = 0; i < num_servers; i++) {
			if (v_loc_map.find(i) == v_loc_map.end()) {
				continue;
			}
			vector<sid_t> o_loc_vals = v_loc_map[i];
			//s p d o  s[tgt_s]-->o[i] s[o_loc]++ and o[o_loc++]
			for (int j = 0; j < o_loc_vals.size(); j++) {
				DoI_index_node_out[sLoc][i].push_back(pair<sid_t, sid_t>(tri.s,o_loc_vals[j]));
				DoI_index_node_in[i][i].push_back(pair<sid_t, sid_t>(o_loc_vals[j], tri.s));
			}
			//DoI_index_node[i][i].insert(DoI_index_node[i][i].end(), o_loc_vals.begin(), o_loc_vals.end());
		}
	}

//#pragma omp parallel for num_threads(thread_num)
	for (int i = 0; i < num_servers; i++) {

		counter_store[i]->batch_insert_DoI_index_new(DoI_index_node_out[i],OUT);
		counter_store[i]->batch_insert_DoI_index_new(DoI_index_node_in[i],IN);
	}
	//printf("end batch_insert_DoI_index_new\n");
	sid_t end = get_usec();;
	cout << "insert DoI index new  spend time = " << (end - start) / 1000000 << " second " << endl;
}

//
void Database::insert_DoI_index_new2(vector<triple_t> tripple_res, int tgt_s) {
	sid_t start = get_usec();
	vector<vector<vector<pair<sid_t, sid_t>>>> DoI_index_node_out;
	vector<vector<vector<pair<sid_t, sid_t>>>> DoI_index_node_in;
	//DoI_index_node.resize(num_servers);
	DoI_index_node_out.resize(num_servers);
	DoI_index_node_in.resize(num_servers);
	for (int i = 0; i < num_servers; i++) {
		//DoI_index_node[i].resize(num_servers);
		DoI_index_node_out[i].resize(num_servers);
		DoI_index_node_in[i].resize(num_servers);
	}
	//printf("start insert_DoI_index_new sid = %d\n", tgt_s);
	//
	vector<sid_t> o_vals_all;
	for (triple_t triple : tripple_res) {
		o_vals_all.push_back(triple.s);
		o_vals_all.insert(o_vals_all.end(), triple.o_vals.begin(), triple.o_vals.end());
	}
	sort_rem_dup(&o_vals_all);
	map<int, vector<sid_t>> all_loc_map = batch_get_global_vloc(o_vals_all); //get o_vals loc
	//push loc_map --> map [key = v, value = loc]
	map<sid_t, int> value_map;
	for (int i = 0; i < num_servers; i++) {
		if (all_loc_map.find(i) == all_loc_map.end()) {
			continue;
		}
		vector<sid_t> o_loc_vals = all_loc_map[i];
		for (sid_t o : o_loc_vals) {
			value_map[o] = i;
		}
	}

	for (triple_t tri : tripple_res) {
		//we first get the Info veretex s
		int sLoc =value_map[tri.s];

		//1.get o_vals loc 
		vector<sid_t> o_vals = tri.o_vals;
		//map<int, vector<sid_t>> v_loc_map = batch_get_global_vloc(o_vals); //get o_vals loc 
		for (sid_t o : o_vals) {
			int oLoc = value_map[o];
			DoI_index_node_out[sLoc][oLoc].push_back(pair<sid_t, sid_t>(tri.s,o));
			DoI_index_node_in[oLoc][oLoc].push_back(pair<sid_t, sid_t>(o, tri.s));
		}
		
	}

	//#pragma omp parallel for num_threads(thread_num)
	for (int i = 0; i < num_servers; i++) {

		counter_store[i]->batch_insert_DoI_index_new(DoI_index_node_out[i], OUT);
		counter_store[i]->batch_insert_DoI_index_new(DoI_index_node_in[i], IN);
	}
	//printf("end batch_insert_DoI_index_new\n");
	sid_t end = get_usec();;
	cout << "insert DoI index new2  spend time = " << (end - start) / 1000000 << " second " << endl;
}



//update DoI info
bool Database::update_DoI_Index(sid_t v, int loc, int tgt) {
	//printf("v= %ld ,loc = %d, tgt = %d,update_DoI_Index function \n", v, loc, tgt);
	//vertex v from loc move to tgt
	//1.get v DoI in INFO
	vector<sid_t> results;
	sid_t doI_key = key_vpid_t(v, loc, DoI_INDEX, IN); 
	counter_store[loc]->read_zset(doI_key, &results);
	//printf("v = %d succ get DoI info \n",v);
	//
	map<int, vector<sid_t>> map_result = batch_get_global_vloc(results);
	//2. move v info in result from node loc to node tgt;
	for (size_t i = 0; i < num_servers; i++) {
		if (map_result.find(i) == map_result.end()) {
			continue;
		}
		//sid_t doI_key = key_vpid_t(v, i, DoI_INDEX, IN);
		counter_store[i]->updateDoI_index(map_result[i], loc, tgt, v);
	}
	//printf("v = %d succ updateDoI_index \n", v);
	//3. delete key in lco map
	vector<sid_t> keys;
	keys.push_back(key_vpid_t(v, loc, DoI_INDEX, IN));
	for (size_t i = 0; i < num_servers; i++) {
		keys.push_back(key_vpid_t(v, i, DoI_INDEX, OUT));
	}
	counter_store[loc]->del_keys(keys);
	//printf("v = %d succ del_keys \n", v);

	//defalut is true;
	return true;
}

int Database::client_pattern_evaluate_global(QueryNode* queryRoot, vector<triple_t> *results){
	printf("Donnot call this function now!\n");
	if(queryRoot == NULL)
		return 0;
	queryRoot->init_interunion_key(num_servers);
	if((queryRoot->bind_val).size() != 0){
		int flag = 1;
		map<int, vector<sid_t>> loc_binds = batch_get_global_vloc(queryRoot->bind_val);
		int map_sz = loc_binds.size();
		vector<vector<triple_t>> res; res.resize(num_servers);
		int sum = 0;
		#pragma omp parallel for num_threads(num_servers) //reduction(+:sum)
		for(int i = 0; i < num_servers; i++){
			if(loc_binds.find(i) == loc_binds.end()){
				continue;
			}
			sum ++;
			//printf("%d send %d to node %d\n", sid, loc_binds[i].size(), i);
			flag = flag & _client_pattern_evaluate(i, loc_binds[i], queryRoot, &res[i]);
		}
		for(int i = 0; i < num_servers; i++)
			results -> insert(results->end(), res[i].begin(), res[i].end());
		if(sum != map_sz){
			printf("WARN: some objects may not be evaluated!\n");
		}
		
		//vector<sid_t> dif = difference(queryRoot->bind_val, queryRoot->bind_to_prune);
		//printf("@%d bind_val %d - bind_to_prune %d size =  %d\n", sid, queryRoot->bind_val.size(), queryRoot->bind_to_prune.size(), dif.size());
		return flag;
	}else{
		return _client_pattern_evaluate(sid, vector<sid_t>(), queryRoot, results);
	}
}

int Database::client_pattern_evaluate_local(QueryNode* queryRoot, vector<triple_t> *results){
	//Here tmp for test
	//testReadAndWrite();
	//return 0;

	if(queryRoot == NULL)
		return 0;
	queryRoot->init_interunion_key(num_servers);
	return _client_pattern_evaluate(sid, queryRoot->bind_val, queryRoot, results);
}
int Database::client_pattern_evaluate(int sid, const vector<triple_t> &triples, 
		const vector<vector<sid_t>> &binds, vector<triple_t> *results){
	QueryNode* queryRoot = parse_patterns(triples, binds);
	queryRoot->init_interunion_key(num_servers);
	clock_t start = clock();
	int flag = _client_pattern_evaluate(sid, queryRoot->bind_val,  queryRoot, results);
	clock_t s1 = clock();
	//printf("%d client pattern evaluate time: %ld\n", sid, s1-start);
	//delete queryRoot;
	return flag;
}


//deprecated ： 效率没有原来的高 client_pattern_evaluate_local
void Database::client_pattern_evaluate_local2(QueryNode* queryRoot, vector<triple_t>* results) {
	if (queryRoot == NULL || queryRoot->edges.size()==0)
		return ;
	//对节点的每条边进行模式匹配
	vector<vector<triple_t>> edge_results;
	int edge_size = (queryRoot->edges).size();
	edge_results.resize(edge_size);
	cout << "Test : start client_pattern_evaluate edge_size = "<< edge_size << endl;
	for (int i = 0; i < edge_size; i++) {
		//cout << "Test : edge = " << i << endl;
		//int j = i % thread_num;
		matchPattern(queryRoot, queryRoot->edges[i], &edge_results[i]);
		//cout << "Test : end  matchPattern" << i << endl;
		cout << "Test : curren node " << queryRoot->name.c_str() <<" edge = "<< queryRoot->edges[i]->node->name.c_str() << " bind_val size = " << queryRoot->bind_val.size() << "Test : RDF  size = " << edge_results[i].size() << endl;
	}
	//cout << "Test : current node " << queryRoot->name << " finish matchPattern" << endl;
	//prune step1
	long start, end;
	start = get_usec();
	set<sid_t> src_bound;
	vector<sid_t> prune_step1 = queryRoot->bind_to_prune;
	sort_rem_dup(&prune_step1);
	//prune s_vals
	for (sid_t s : queryRoot->bind_val) {
		if (!binary_search(prune_step1.begin(), prune_step1.end(), s)) {
			src_bound.insert(s);
		}
	}
	queryRoot->bind_val.assign(src_bound.begin(), src_bound.end());

	//prune triple vals
	for (int i = 0; i < edge_size; i++) {
		//vector<sid_t> pre_bind;
		set<sid_t> obj_bind;
		for (triple_t t : edge_results[i]) {
			if (binary_search(src_bound.begin(), src_bound.end(), t.s)) {
				obj_bind.insert(t.o_vals.begin(), t.o_vals.end());
				results->push_back(t);
			}
		}
		if (queryRoot->edges[i]->node->bind_val.size() == 0) //if edges[i]->node is not bound
			queryRoot->edges[i]->node->bind_val.assign(obj_bind.begin(), obj_bind.end());
		
	}
	end = get_usec();
	cout << "Test : merge bind_val spend time = " << (float)(end - start) / 1000000 << endl;
}

//TOOD 重写为三元组模式(如何和之前的qp结合到一起)
//重新为不使用triplePattern
void Database::matchPattern(QueryNode* node, QueryEdge* edge, vector<triple_t>* results) {
	//cout << "Test : go into matchPattern " << endl;
	results->clear();
	dir_t dir = edge->d;

	vector<sid_t> src_bound = node->bind_val;
	vector<sid_t> pred_bound = edge->bind_val;
	vector<sid_t> tgt_bound = edge->node->bind_val;
	
	//修改为获取global对应的S set
	//如果一个节点node存在多条边只需要获取第一条边的 bind_val(因为边和边之间是交集操作)
	if (src_bound.size() == 0) {
		bacth_loadEntities_global(pred_bound, dir, &src_bound);
		//执行去重操作
		sort_rem_dup(&src_bound);
		node->bind_val.insert(node->bind_val.end(),src_bound.begin(), src_bound.end());
	}

	if (tgt_bound.size() > 0) {
		vector<sid_t> inter_set;
		for (sid_t o : tgt_bound) {
			string key_t;
			for (sid_t p : pred_bound) {
				if (p == 1 && dir == OUT)
					key_t = std::to_string(key_vpid_t(0, o, INDEX, dir));
				else
					key_t = std::to_string(key_vpid_t(o, p, INDEX, inverse_dir(dir)));
			}
			async_store[sid][0]->read_zset(key_t, &inter_set);
		}
		
		//cout << "s_set size = " << s_set.size() << endl;
		//获取交集大小
		inter_set = intersect(inter_set, src_bound);
		//cout << "inter_set size = " << inter_set.size();
		vector<sid_t> bind_prune = difference(src_bound, inter_set);
		//printf("@%d src_bind %d src_num %d prune bind size %d\n", sid, src_bind.size(), inter_sets.size(), bind_prune.size());
		node->insert_prune_bind(bind_prune);
		src_bound = inter_set;
	}

	//按照src_bound的host分割(改到函数外部)
	map<int, vector<sid_t>> initial_servers = get_initial_server(src_bound);
	//打印结果 没问题啊
	/*cout << "Test : print initial_servers = [";
	for (sid_t tmp: initial_servers[0]) {
		cout << tmp << ",";
	}
	cout << "]" << endl;*/
	vector<vector<triple_t>> res; res.resize(num_servers);

	#pragma omp parallel for num_threads(num_servers) //reduction(+:sum)
	for (int i = 0; i < num_servers; i++) {
		async_store[i][0]->traverse_vertex(node, initial_servers[i], edge, &res[i]);
	}

	//cout << "Test : get triplePattern succ" << endl;
	for (int i = 0; i < num_servers; i++) {
		if (res[i].size() > 0)
			results->insert(results->end(), res[i].begin(), res[i].end());
	}
	//cout << "Test : finsh matchPattern succ" << endl;
}

void Database::matchPattern2(QueryNode* node, QueryEdge* edge, vector<triple_t>* results) {
	results->clear();
	dir_t dir = edge->d;

	vector<sid_t> src_bound = node->bind_val;
	vector<sid_t> pred_bound = edge->bind_val;
	vector<sid_t> tgt_bound = edge->node->bind_val;

	//修改为获取global对应的S set
	//如果一个节点node存在多条边只需要获取第一条边的 bind_val(因为边和边之间是交集操作)
	if (src_bound.size() == 0) {
		bacth_loadEntities_global(pred_bound, dir, &src_bound);
		//执行去重操作
		sort_rem_dup(&src_bound);
		node->bind_val.insert(node->bind_val.end(), src_bound.begin(), src_bound.end());

	}
	//按照src_bound的host分割(改到函数外部)
	map<int, vector<sid_t>> initial_servers = get_initial_server(src_bound);
	vector<vector<triple_t>> res; res.resize(num_servers);
	#pragma omp parallel for num_threads(num_servers) //reduction(+:sum)
	for (int i = 0; i < num_servers; i++) {
		cout << "Test : current server no  = " << i << endl;
		for (sid_t s : initial_servers[i]) {
			vector<sid_t> pre_bound_tmp = pred_bound;
			if (pred_bound.size() == 0) { //默认pre is bound
				//pre_bound_tmp = &loadPredicates(s, dir);
				//cout << "Test :get p set " << endl;
				async_store[i][0]->loadPredicates(s, dir, &pre_bound_tmp);
			}
			for (sid_t p : pre_bound_tmp) {
				vector<sid_t> tgt_bound_tmp;
				// cout << "Test :get o set " << endl;
				async_store[i][0]->loadNeighbors(s, p, dir, &tgt_bound_tmp);
				if (tgt_bound.size() > 0) {
					tgt_bound_tmp = intersect(tgt_bound, tgt_bound_tmp);
				}
				// 考虑 s p o_vals 三元组类型（save space ）
				triple_t t(s, p, dir);
				if (tgt_bound_tmp.size() == 0) {
					node->insert_prune_bind(s);
				}
				else {
					t.o_vals = tgt_bound_tmp;
					res[i].push_back(t);
				}
			}
		}
	}

	//cout << "Test : get triplePattern succ" << endl;
	for (int i = 0; i < num_servers; i++) {
		if (res[i].size() > 0)
			results->insert(results->end(), res[i].begin(), res[i].end());
	}
}

void Database::bacth_loadEntities_global(const vector<sid_t> pset, dir_t d,vector<sid_t> * result) {
	for (int i = 0; i < num_servers; i++) {
		//loadEntitiesSet(vector<sid_t> pset, dir_t d, vector<sid_t> * result)
		vector<sid_t> ret;
		async_store[i][0]->loadEntitiesSet(pset, d,&ret);
		//s_set_map[i] = ret;
		result->insert(result->end(), ret.begin(), ret.end());
	}
}
/**********************一些测试函数功能的函数***********************/
void Database::testKeyIndex() {
	//
	srand((int)time(0));  // 产生随机种子
	sid_t a = 131073,b = 1770751;
	sid_t c = 33;
	int loop = 1000;
	dir_t d = dir_t::OUT; // 默认out
	int testNum = 0, err = 0, err1 = 0;// , err2 = 0;
	int sp_count = 0;
	for (int i = 0; i < loop; i++) {
		sid_t s = (rand() % (b - a + 1)) + a;
		sid_t p = rand() % 33 + 1;
		//sid_t sp_key = key_vpid_t(s, p, NO_INDEX, d);
		//1. get o vals;
		vector<sid_t> o_vals;
		vector<sid_t> o_vals2;
		async_store[0][0]->loadNeighbors(s, p, d, &o_vals);
		triple_store[0]->get_neighbor(s, p, d, &o_vals2);
		if (o_vals.size() != o_vals2.size()) {
			sp_count++;
		}
		//2. 随机选择一个o ;by o p d  get s_vals
		for (sid_t o : o_vals) {
			vector<sid_t > s1_vals, s2_vals;
			sid_t key_t;
			if (p == 1 && d == OUT)
				key_t = key_vpid_t(0, o, INDEX, d);
			else
				key_t = key_vpid_t(o, p, INDEX, inverse_dir(d));
			//1.使用zset模式 string
			async_store[0][0]->read_zset(key_t, &s1_vals);
			//2.使用trippleDB模式
			//async_store[0][0]->read_zset(key_t, &s2_vals);
			if (!binary_search(s1_vals.begin(), s1_vals.end(), s)) {
				err1++;
				cout << "error in zset pattern1 : " << "s = " << s << "; p = " << p  <<"; o = " << o << endl;
			}
			/*if (!binary_search(s2_vals.begin(), s2_vals.end(), s)) {
				err2++;
				cout << "error in zset pattern2 : " << "s = " << s << "p = " << p << "o = " << o << endl;
			}*/
			testNum++;
		}

		
	}
	if (sp_count > 0) {
		cout << "diff way get neighbor err num = " << sp_count << endl;
	}
	cout << "testNum = " << testNum  << " err1 = " << err1  << endl;
	//cout << "testNum = " << testNum << " err = " << err << " err1 = " << err1 << "err2 = " << err2 << endl;
	
}


void Database::testDumpAndRestore() {
	//1.从redis数据或随机获取一个key
	sid_t a = 131073, b = 1770751;
	int tgt_id = (sid + 1) % 3;
	printf("start testDumpAndRestore sid = %d\n",sid);
	printf("sid = %d; tgs_id = %d\n", sid, tgt_id);
	vector<sid_t> keys;
	//2.dump and restore
	for (int i = 0; i < 100; i++) {
		sid_t v = (rand() % (b - a + 1)) + a;
		sid_t sc_key = key_vpid_t(v, 0, NO_INDEX, OUT); //[sd_key,p_list]
		string ret = triple_store[sid]->get_string_by_key(sc_key);
		if (ret.empty()) continue;
		keys.push_back(sc_key);
		size_t len = 0;
		//2.dump
		const char* str = triple_store[sid]->dump(sc_key, &len);
		//3.restore
		if (str != NULL) {
			triple_store[tgt_id]->restore(sc_key, str, len);
		//	free(str);
		}
		else {
			printf("str is null");
		}
	}
	//4.测试
	for (sid_t key : keys) {
		vector<sid_t> ret = triple_store[sid]->get_set_member(key);
		vector<sid_t> ret2 = triple_store[tgt_id]->get_set_member(key);
		printf("key = %ld , old sid = %d , result size = %d, new sid = %d,result size = %d\n", key, sid, ret.size(), tgt_id, ret2.size());
	}
	printf("end testDumpAndRestore sid = %d, keys size = %d\n",sid, keys.size());

}

void Database::testMoveData() {
	sid_t a = 131073, b = 1770751;
	int tgt_id = (sid + 1) % 3;
	printf("start testMoveData sid = %d\n", sid);
	printf("sid = %d; tgs_id = %d\n", sid, tgt_id);
	vector<sid_t> vset;
	vector<sid_t> result;
	//2.dump and restore
	for (int i = 0; i < 100; i++) {
		sid_t v = (rand() % (b - a + 1)) + a;
		sid_t sc_key = key_vpid_t(v, 0, NO_INDEX, OUT); //[sd_key,p_list]
		vector<sid_t> ret = triple_store[sid]->get_set_member(sc_key);
		if (ret.size() == 0) continue;
		result.push_back(ret.size());
		vset.push_back(v);
		client_reassign(sid, v, tgt_id);
	}
	printf("reassign data size = %d", vset.size());
	for (int i = 0; i < vset.size();i++) {
		sid_t sc_key = key_vpid_t(vset[i], 0, NO_INDEX, OUT); //[sd_key,p_list]
		vector<sid_t> ret = triple_store[sid]->get_set_member(sc_key);
		vector<sid_t> ret2 = triple_store[tgt_id]->get_set_member(sc_key);

		if (ret.size() == 0 && ret2.size() == result[i]) {
			printf("reassign key %ld success\n", vset[i]);
		}
		else {
			printf("reassign key %ld error orign size = %d ,old size = %d ,new size = %d \n", vset[i],result[i],ret.size(),ret2.size());
		}
	}
	//判定释放成功reassign
	printf("end testMoveData sid = %d\n", sid);
}
//TODO add test for differnt 
void Database:: testReadAndWrite() {
	//读写10W条数据进行测试
	//write use HINCRBY command
	//10 轮测试
	int round = 10;
	for (int k = 0; k < num_servers; k++) {
		vector<sid_t> vs;
		srand((int)time(0));  // 产生随机种子  把0换成NULL也行
		for (int i = 0; i < 100000 *(k + 1); i++) {
			vs.push_back(rand() % 1000000);
		}
		sid_t start, end;
		start = get_usec();
		printf("test %d  for async_store\n",k);
		printf("%d start write data in async_store\n",k);
		async_store[k][0]->batch_write_test(vs);
		//read use get command
		printf("%d start read data in async_store\n",k);
		vector<int> result1;
		async_store[k][0]->batch_read_test(vs, &result1);
		printf("%d result1 size = %d\n", k,result1.size());

		sid_t end1 = get_usec();
		printf("Tests: async_store in %d spend time = %d second\n", k, (end1 - start) / 1000000);
		
		//read use hget command
		printf("test %d for count_store\n",k);
		printf("%d start write data in counter_store\n",k);
		counter_store[k]->batch_write_test(vs);
		//read use get command
		printf("%d start read data in counter_store\n",k);
		vector<int> result2;
		counter_store[k]->batch_read_test(vs, &result2);
		printf("%d result2 size = %d\n", k ,result2.size());
		end = get_usec();
		printf("Tests: async_store in %d spend time = %d second\n", k, (end - end1) / 1000000);

		printf("Tests: one loop in %d spend time = %d second\n", k, (end - start) / 1000000);
	}
	
	printf("end function testReadAndWrite\n");
}

/****************************	End Test *******************************************/
/************************* End: execute patterns on client side *******************************/


/************************** Begin: Reassignment ********************************/
int Database::reassign_evaluate(int loc_s, sid_t v){
	return get_reassign_info(loc_s, v);	
}
//基于DoI Index的评估策略 naive评估
int Database::reassign_evaluate2(int loc_s, sid_t v) {
	//1.判断节点存在性判断
	if (!async_store[loc_s][0]->isExist(v)) return -1;

	return get_max_DoI_Node(loc_s, v); 
	//return counter_store[loc_s]->get_max_Di(v, loc_s, num_servers);
}

map<int, vector<sid_t>> Database::batch_reassign_evaluate2(int loc_s, vector<sid_t>& vs) {
	//1.判断节点存在性判断
	vector<sid_t> existNode = async_store[loc_s][0]->batch_isExist(vs);
	map<int, vector<sid_t> > reassign_map;
	if (existNode.empty()) return reassign_map;

	reassign_map = batch_get_max_DoI_Node(loc_s,existNode);

	return reassign_map;
	//return counter_store[loc_s]->get_max_Di(v, loc_s, num_servers);
}

//new Reassign DoI evaluate
int Database::new_reassign_evaluate(int loc_s, sid_t v) {
	//1.判断节点存在性判断
	if (!async_store[loc_s][0]->isExist(v)) return -1;

	return get_max_DoI_Node_new(loc_s, v);
}

//////////////////////////////////////////////////////////////////////////////////

bool Database::client_reassign_vd(int loc_s, sid_t v, dir_t d, int tgt_node){
	bool flag = true;
	sid_t sd_key = key_vpid_t(v, 0, NO_INDEX, d);
	vector<sid_t> p_list;
	//printf("TEST : client_reassign_vd start sd_key %ld %ld and d = %d\n", v, sd_key,d);
	
	async_store[loc_s][0]->read_zset(to_string(sd_key), &p_list); 
	//printf("finsh read p_list : sd_ky %ld", sd_key);
	for(int i = 0; i < p_list.size(); i++){
		sid_t p = p_list[i];
		flag = flag && triple_store[loc_s]
			->reassign_vpd(v,p,d,triple_store[tgt_node]);
	}
	//printf("TEST : finish client_reassign_vpd sd_key %ld %ld and d = %d\n", v, sd_key,d);
	flag = flag && triple_store[loc_s]->reassign_vd(v,d,triple_store[tgt_node]);
	//printf("TEST : client_reassign_vd end sd_key %ld %ld and d = %d\n", v, sd_key, d);
	return flag;
}

bool Database::client_migrate_vd(int loc_s, sid_t v, dir_t d, int tgt_node) {
	bool flag = true;
	sid_t sd_key = key_vpid_t(v, 0, NO_INDEX, d);
	//1.获取triple_stor[loc_s]数据库的hostname 和 port host_names[i].c_str(), ports[i]
	//const char* hostname = triple_store[loc_s]., int port
	vector<sid_t> p_list;
	async_store[loc_s][0]->read_zset(to_string(sd_key), &p_list);
	for (int i = 0; i < p_list.size(); i++) {
		sid_t p = p_list[i];
		flag = flag && triple_store[tgt_node]
			->migrate_vpd(v, p, d, cfg->host_names[loc_s].c_str(), cfg->ports[loc_s]);
	}
	flag = flag && triple_store[tgt_node]->migrate_vd(v, d, cfg->host_names[loc_s].c_str(), cfg->ports[loc_s]);
	return flag;
}

//TODO ADD remove DoI index info
bool Database::client_remove_DoI_index(int loc_s, sid_t v) {
	
	sid_t sc_key = key_vpid_t(v, 0, COUNTER, (dir_t)0);

	vector<string> params;
	params.push_back("HDEL");
	params.push_back(to_string(sc_key));
	for (int i = 0; i < num_servers; i++) {
		string field = "node" + to_string(i);
		params.push_back(field);
	}

	return counter_store[loc_s]->remove_DoI_index(params);

}
//reassign vertex from node_i  to node_j
int Database::client_reassign(int loc_s, sid_t v, int tgt_node) {
	int f = 1; //Right
	if (request_global_reassign_lock(loc_s, v) == 0) {
		f = 0; //Error 
		return f;
	}
	bool flag = true;
	flag = flag && client_reassign_vd(loc_s, v, OUT, tgt_node);
	flag = flag && client_reassign_vd(loc_s, v, IN, tgt_node);

	//printf("start client_reassign_vcount\n");
	
	flag = flag && triple_store[loc_s]
		->reassign_vcounter_weight(v, triple_store[tgt_node], true);
	flag = flag && counter_db[loc_s]
		->reassign_vcounter_weight(v, counter_db[tgt_node], true);
	// here reassign DoI index info
	flag = flag && counter_db[loc_s]->reassign_DoI_index(v, triple_store[tgt_node], tgt_node);

	if (!flag) printf("v = %ld reassign count fail \n",v);
	//printf("start receive fuction\n");
	if (!flag) {
		f = -1; //Error
		return f;
	}

	//receive函数是给新节点数据添加索引信息和其他边缘信息
	flag = flag && triple_store[tgt_node]->receive(v, true);
	flag = flag && counter_db[tgt_node]->receive(v, false);
	if (!flag){
		f = -2; //Error
		return f;
	}
	set_v_loc(v, tgt_node);

	//reasign是删除原来节点的索引信息和其他边缘信息
	flag = flag && triple_store[loc_s]->reassign(v, true);
	flag = flag && counter_db[loc_s]->reassign(v, false);


	if (!flag) {
		f = -3;
		return f;
	}
	return f;
}

//TODO 未完待续
int Database::batch_client_reassign(int loc_s, vector<sid_t> vs, int tgt_node) {

}
int Database::client_new_reassign(int loc_s, sid_t v, int tgt_node) {
	int f = 1; //Right
	if (request_global_reassign_lock(loc_s, v) == 0) {
		f = 0; //Error 
		return f;
	}
	bool flag = true;

	//move base data to tgt node
	flag = flag && client_reassign_vd(loc_s, v, OUT, tgt_node);
	flag = flag && client_reassign_vd(loc_s, v, IN, tgt_node);

	//printf("start client_reassign_vcount\n");
	flag = flag && triple_store[loc_s]
		->reassign_vcounter_weight(v, triple_store[tgt_node], true);
	flag = flag && counter_db[loc_s]
		->reassign_vcounter_weight(v, counter_db[tgt_node], false);
	//move DoI Index info from loc to tgt
	//printf("v = %ld,start reassign_doI_index\n",v);
	flag = flag && counter_db[loc_s]->reassign_doI_index(v,num_servers, counter_db[tgt_node]);
	//delete loc doI key
	//printf("v = %ld start update_DoI_Index fuction\n",v);
	flag = flag && update_DoI_Index(v, loc_s, tgt_node); 
	if (!flag) {
		f = -1; //Error
		return f;
	}
	//printf("start receive fuction\n");
	//receive函数是给新节点数据添加索引信息和其他边缘信息
	flag = flag && triple_store[tgt_node]->receive(v, true);
	flag = flag && counter_db[tgt_node]->receive(v, false);
	if (!flag) {
		f = -2; //Error
		return f;
	}
	set_v_loc(v, tgt_node);
	//reasign是删除原来节点的索引信息和其他边缘信息
	flag = flag && triple_store[loc_s]->reassign(v, true);
	flag = flag && counter_db[loc_s]->reassign(v, false);
	//TODO 删除loc DoI 信息和其他关联信息
	//for(int i=0;i<num)
	flag = flag && counter_db[loc_s]->del_key(v);
	if (!flag) {
		f = -3;
		return f;
	}
	return f;
}

////////////////////////////////////////////////////////////////////////////////////
int Database::get_max_DoI_Node(int loc_s, sid_t v) {
	vector<int> result;
	//cout << "DoI Index v = " << v << " sid = " << loc_s << " num_servers = " << num_servers << endl;
	//async_store[loc_s][0]->get_DoI_List(v, num_servers, &result);
	counter_store[loc_s]->get_DoI_List(v, num_servers, &result);
	//printf("node %d to %d need 3 result and get %d result \n", loc_s,v, result.size());
	/*if (result.size() != num_servers) {
		printf("Error : node %d to %d need 3 result and get %d result \n", loc_s, v, result.size());
	}*/
	assert(result.size() == num_servers);

	int maxNode = loc_s; //default is Loc Node
	int maxDi = result[loc_s];
	for (int i = 0; i < num_servers; i++) {
		if (result[i] > maxDi) {
			maxDi = result[i];
			maxNode = i;
		}
	}
	return maxNode;
}
//TODO 未完待续
map<int, vector<sid_t> > Database::batch_get_max_DoI_Node(int loc_s, vector<sid_t>& vs) {
	vector<int> result;
	counter_store[loc_s]->batch_get_DoI_List(vs, num_servers, &result);
	assert(result.size() == num_servers * vs.size());
	map<int, vector<sid_t> > reassign_map;
	for (int i = 0; i < vs.size(); i++) {
		int maxNode = loc_s;
		int maxDi = result[num_servers * i + loc_s];
		for (int k = num_servers * i; k < num_servers * (i + 1); k++) {
			if (result[k] > maxDi) {
				maxDi = result[k];
				maxNode = k;
			}
		}
		maxNode %= 3;
		if (reassign_map.find(maxNode) == reassign_map.end()) {
			reassign_map.insert(pair<int, vector<sid_t>>(maxNode, vector<sid_t>()));
		}
		reassign_map[maxNode].push_back(vs[i]);
	}
	return reassign_map;
}

//add in new Reassign 
int Database::get_max_DoI_Node_new(int loc_s, sid_t v) {
	vector<int> result;
	//printf("v = %ld loc = %d,get_DoI_List_new start\n", v, loc_s);
	//TODO 这里获取的结果全为[0 0  0]
	counter_db[loc_s]->getDoI_Index_size(v, num_servers, &result);
	//counter_store[loc_s]->get_DoI_List(v, num_servers, &result);
	//printf("v = %ld get_DoI_List_new succ res = [%d,%d,%d]\n",v,result[0], result[1], result[2]);
	if (result.size() != num_servers) {
		printf("Error : node %d to %d need 3 result and get %d result \n", loc_s, v, result.size());
	}
	assert(result.size() == num_servers);
	//need some node point to v ? d ==IN
	int maxNode = sid; //default local
	int maxDi = result[sid];
	for (int i = 0; i < num_servers; i++) {
		if (result[i] > maxDi) {
			maxDi = result[i];
			maxNode = i;
		}
	}

	return maxNode;
}
int Database::get_reassign_info(int loc_s, sid_t v){
	vector<vector<int>> reassign_info_ts, reassign_info_cs;
	reassign_info_ts.resize(num_servers);
	reassign_info_cs.resize(num_servers);
	int obj_weight = 0, obj_edgenum = 0, obj_prev_reassign_weight = 0; 
	int edgesum = 0, weightsum = 0, objsum = 0; 
	bool flag = true, locality_null = true;

#pragma omp parallel for num_threads(num_servers)
	for(int i = 0; i < num_servers; i++){
		/* 0- server_objnum. 1 - server_edgenum, 
		 * 2 - obj-exists, 3 - obj_top_ll, 
		 * 4 - obj-edgenum
		 * */
		async_store[i][0]->get_reassign_info( v, &reassign_info_ts[i], true);
		if(reassign_info_ts[i].size() != 5)
			printf("error reassign info %d %ld\n", i, v);
		assert(reassign_info_ts[i].size() == 5);
		
		objsum += reassign_info_ts[i][0];
		edgesum += reassign_info_ts[i][1];
		if(reassign_info_ts[i][2] == 1){
			if(i != loc_s)
				flag = false;
			obj_edgenum = reassign_info_ts[i][4];
		}
	}

#pragma omp parallel for num_threads(num_servers)
	for(int i = 0; i < num_servers; i++){
		/*  0 - server_weight, 1- active_obj_num
		 *  2- obj_workload_ll, 
		 *  3 - obj_weight, 4 - obj-prev_reassign_weight
		 * */
		counter_store[i]->get_reassign_info( v, &reassign_info_cs[i],false);
		if(reassign_info_cs[i].size() != 5)
			printf("error reassign counter info %d %ld\n", i, v);
		assert(reassign_info_cs[i].size() == 5);
		
		weightsum += reassign_info_cs[i][0];
		if(reassign_info_cs[i][2] > 0)
			locality_null = false;
		if(i == loc_s){
			obj_weight = reassign_info_cs[i][3];
			obj_prev_reassign_weight = reassign_info_cs[i][4];
		}
	}
	//TODO print reassign_info

	//大部分reassign_evaluate ret = -1
	if(obj_edgenum == 0 || !flag || locality_null)
		return -1;
	
	if(obj_prev_reassign_weight == 0)
	{
		if(obj_weight < cfg->trigger_k)
			return -1;
	}else{
		if(cfg->trigger_double && 
				obj_weight - obj_prev_reassign_weight < obj_prev_reassign_weight)
			return -1;
		if(!(cfg->trigger_double) && 
				obj_weight - obj_prev_reassign_weight < cfg->trigger_k)
			return -1;
	}

	double avg_edge = edgesum/num_servers, avg_weight = weightsum/num_servers, avg_obj = objsum/num_servers;
	double max_score = -INFINITY;
	int server_chose = -1;
	for(int i = 0; i < num_servers; i++){
		double score = fennel(reassign_info_cs[loc_s][0], reassign_info_cs[i][0]
				, reassign_info_cs[i][1],avg_weight, 
				obj_weight, reassign_info_cs[i][2]);
		if(score > max_score){
			max_score = score;
			server_chose = i;
		}
	}
	return server_chose;
}

//打分函数 f = locality - cfg->beta * (server_weight/(num_servers * active_obj_num))
double Database::fennel(int src_weight, int server_weight, int active_obj_num, int avg_weight, 
		int o_weight, int locality){

	if((double)(src_weight - o_weight ) - (avg_weight * (2 - cfg -> gama)) < 0.0
		|| (double)(server_weight + o_weight) - (avg_weight * cfg->gama) > 0.0)
		return -INFINITY;

	if(active_obj_num == 0)
		return (double)locality ;
	else
		return (double)locality - cfg->beta * (server_weight/(num_servers * active_obj_num));
}
/***************************** End: reassignment *************************************/
//double up
int Database::request_global_reassign_lock(int loc_s, sid_t v){
	return triple_store[loc_s]->reassign_lock(v);	
}

int Database::batch_up_edgeweight(int sid, sid_t root_v, const vector<triple_t> &triples){
	//async_store[sid][0]->batch_update_edgeweight(triples, cfg->edgelog_len);
	counter_store[sid]->batch_update_edgeweight(root_v, triples, cfg->edgelog_len);	
	return 1;
}

int Database::batch_up_vertexweight(int sid, const vector<sid_t> &vs, int incr_weight){
	//async_store[sid][0]->batch_update_vertexweight(vs, incr_weight, cfg->verticelog_len);
	counter_store[sid]->batch_update_vertexweight(vs, incr_weight, cfg->verticelog_len);
	return 1;
}

int Database::batch_up_DoI_index_info(int sid, const vector<triple_t>& triples) {

	counter_store[sid]->batch_update_DoI_index(triples, cfg->edgelog_len);
}
//add new function in 2020/07

sid_t Database::loadNode(sid_t v) {
	int init_s = jumpConsistentHash(v, num_servers);
	return init_s;
}

sid_t Database::loadNode(const string& text) {
	//1. get vertext_id by str in dict
	sid_t str_hash = hash64(text.c_str(), 0);
	int32_t node = jumpConsistentHash(str_hash, num_servers);
	sid_t vid = dicts[node]->get_id_by_dict(text);
	int init_s = jumpConsistentHash(vid, num_servers);
	return init_s;
}

vector<sid_t>* Database::loadNodes(sid_t p, dir_t d) {
	vector<sid_t>* results;
	for (int i = 0; i < num_servers; i++) {
		triple_store[i]->get_vs(p, d, results);
	}
	return results;
}

vector<sid_t>* Database::loadEntities(sid_t p, dir_t d) {
	vector<sid_t>* results;
	triple_store[sid]->get_vs(p, d, results);
	return results;
}

vector<sid_t>* Database::loadPredicates(sid_t v, dir_t d) {
	vector<sid_t>* results;
	int init_s = jumpConsistentHash(v, num_servers);
	if (init_s != sid) {
		printf("this server node donot contain vertex %ld", &v);

	}
	triple_store[sid]->get_pset(v, d, results);
	return results;
}

vector<sid_t>* Database::loadNeighbors(sid_t v, sid_t p, dir_t d) {
	vector<sid_t>* results;
	int init_s = jumpConsistentHash(v, num_servers);
	if (init_s != sid) {
		printf("this server node donot contain vertex %ld", &v);
	}
	triple_store[sid]->get_neighbor(v, p, d, results);
	return results;
}
