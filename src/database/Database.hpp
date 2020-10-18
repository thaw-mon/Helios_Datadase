#ifndef DATABASE_HPP_
#define DATABASE_HPP_

extern "C"{
#include "type.h"
#include "HashFunction.h"

}
#include "triple_t.hpp"
#include <cassert>
#include <vector>
#include <string>
#include <map>
#include "TripleDB.hpp"
#include "Memory.hpp"
#include "Dictionary.hpp"
#include "HeliosConfig.hpp"
#include "AsyncRedisStore.hpp"
#include "QueryNode.hpp"

using namespace std;
class Database{

	private:
		const int NUM_CONNECT = 10;
		typedef vector<TripleDB*> TRIPLE_STORE;
		typedef vector<Dictionary*> DICTS;
		typedef vector<AsyncRedisStore*> ASYNC_REDIS;

		int sid; // current server id
		int num_servers;
		
		HeliosConfig* cfg;

		/*Following data depends on the triple data and vertex loc*/
		TRIPLE_STORE triple_store;//
		vector<ASYNC_REDIS> async_store;
		/*Following data is fixed once inserted*/
		DICTS dicts;

		ASYNC_REDIS counter_store;
		TRIPLE_STORE counter_db;

		Memory* mem;

	public:
		Database(int sid, HeliosConfig* cfg, Memory* mem)
			:sid(sid), cfg(cfg), mem(mem){
			num_servers = cfg->global_num_servers;
			vector<string> host_names = cfg->host_names;
			vector<int> ports = cfg->ports;
			assert(num_servers == (int) host_names.size());

			int dict_port = cfg->db_dict_port;
			
			triple_store.resize(num_servers);
			for(unsigned int i = 0; i < num_servers; i++){
				//printf("TDB %s %d\n", host_names[i].c_str(), ports[i]);
				triple_store[i] = new TripleDB(host_names[i].c_str(), ports[i]);
			}
			async_store.resize(num_servers);
			for(unsigned int i = 0; i < num_servers; i++){
				//printf("ADB %s %d %s\n", host_names[i].c_str(), 	ports[i], (cfg->sockfiles)[i].c_str());
				async_store[i].resize(NUM_CONNECT);
				for(int j = 0; j < NUM_CONNECT; j++)
					async_store[i][j] = new AsyncRedisStore(sid, i, j, host_names[i].c_str(), 
						ports[i], (cfg->sockfiles)[i].c_str(), cfg);
			}
	
			counter_db.resize(num_servers);
			for(unsigned int i = 0; i < num_servers; i++){
				counter_db[i] = new TripleDB(host_names[i].c_str(), cfg->db_counter_port);
			}
			counter_store.resize(num_servers);
			for(unsigned int i = 0; i < num_servers; i++){
				counter_store[i] = new AsyncRedisStore(sid, i, 0, host_names[i].c_str(), 
						cfg->db_counter_port, (cfg->db_counter_sock).c_str(), cfg);
			}

			dicts.resize(num_servers);
			for(unsigned int i = 0; i < num_servers; i++){
				//printf("Dict %s %d\n", host_names[i].c_str(), dict_port);
				dicts[i] = new Dictionary(host_names[i].c_str(), dict_port);
			}
		//	printf("database sid %d\n", sid);
			
		}
		virtual ~Database(){
			for(int i = 0; i < triple_store.size(); i++){
				if(triple_store[i] != NULL)
					delete triple_store[i];
			}
			for(int i = 0; i < async_store.size(); i++){
				for(int j = 0; j < NUM_CONNECT; j++)
					if(async_store[i][j] != NULL)
						delete async_store[i][j];
			}
			for(int i = 0; i < dicts.size(); i++){
				if(dicts[i] != NULL)
					delete dicts[i];
			}
			for(int i = 0; i < counter_store.size(); i++){
				if(counter_store[i] != NULL)
					delete counter_store[i];
			}
			for(int i = 0; i < counter_db.size(); i++){
				if(counter_db[i] != NULL)
					delete counter_db[i];
			}
		}

		int current_server(){return sid;}

		Memory* getMem(){return mem;}
		/* Dictionary Operator*/
		/*bool load_local_dict(const char* filename){
		 	return dicts[sid]->load_local_dict(filename, num_servers, sid);
		}*/

		bool insert_local_dict(const string &str, sid_t id){
			return dicts[sid]->insert_dict(str, id);
		}
		
		bool insert_local_dict(sid_t id, const string &str){
			return dicts[sid]->insert_dict(id,str);
		}
		sid_t lookup_global_dict(const string &text){
			sid_t str_hash = hash64(text.c_str(),0);
			int32_t node = jumpConsistentHash(str_hash, num_servers);
			return dicts[node]->get_id_by_dict(text);
		}

		string lookup_global_dict(sid_t id){
			int32_t node = jumpConsistentHash(id, num_servers);
			return dicts[node]->get_dict_by_id(id);	
		}

		void get_neighbor(int loc, sid_t v, dir_t d, vector<sid_t>* neighbors){
			sid_t sd_key = key_vpid_t(v, 0, NO_INDEX, d);
			vector<sid_t> p_list;
			async_store[loc][0]->read_zset(sd_key, &p_list);
			triple_store[loc]->get_neighbor(v,p_list,d,neighbors);
		}
		void get_neighbor_global(sid_t v, vector<sid_t>* neighbors){
			int32_t node = jumpConsistentHash(v, num_servers);
			get_neighbor(node, v, OUT, neighbors);
			get_neighbor(node, v, IN, neighbors);
			sort_rem_dup(neighbors);
		}

		/************ info related to graph topology ******************/
		bool add_global_triple(const triple_t &triple){
			int32_t initial_node = jumpConsistentHash(triple.s, num_servers);
			return _add_triple(initial_node, triple);	
		}

		bool add_local_triple(const triple_t &triple){
			return _add_triple(sid, triple);
		}

		bool _add_triple(int initial_node, const triple_t &triple);

		bool load_data(const vector<itriple_t> &spo, dir_t d);
		/*********************************/

		bool flush_local_command(){
			triple_store[sid]->flush_command();
			dicts[sid]->flush_command();
			return true;
		}
		bool flush_global_command(){
			for(unsigned int i = 0; i < num_servers; i++){
				triple_store[i]->flush_command();
			}
			//dicts[sid]->flush_command();
			return true;
		}
		
		map<int, vector<sid_t>> get_initial_server(const vector<sid_t> &v);

		/*
		 * Global predcate
		 * Get the predicate counter
		 * */
		int get_p_counter(sid_t p, int *scounter, int *ocounter);

		vector<int> get_p_loc(sid_t p, dir_t d);
		
		int get_global_pcounter(sid_t p, int *counters, 
				vector<int> *p_out_locs, vector<int> *p_in_locs);
		/*
		 * current is the initial node of the vertex
		 * */
		int get_global_vloc(sid_t v);
		int set_v_loc(sid_t v, int loc);
		int batch_set_local_vloc(sid_t v, int loc){
			return triple_store[sid]->batch_set_vloc(v, loc);
		}
		map<int, vector<sid_t>> batch_get_global_vloc(const vector<sid_t> &v);

		map<sid_t, int> batch_get_global_vloc2(const vector<sid_t>& v);

		int get_max_DoI_Node(int loc_s, sid_t v);

		map<int, vector<sid_t>> batch_get_max_DoI_Node(int loc_s, vector<sid_t>& vs);

		int get_max_DoI_Node_new(int loc_s, sid_t v);

		/* reassign related*/
		int get_reassign_info(int loc_s, sid_t v);
	
		int reassign_evaluate(int loc_s, sid_t v);

		int reassign_evaluate2(int loc_s, sid_t v);

		map<int, vector<sid_t>> batch_reassign_evaluate2(int loc_s, vector<sid_t>& vs);

		int new_reassign_evaluate(int loc_s, sid_t v);

		//int reassign(int loc_s, sid_t v, int tgt_node);

		bool client_remove_DoI_index(int loc_s, sid_t v);

		int client_reassign(int loc_s, sid_t v, int tgt_node);

		int batch_client_reassign(int loc_s, vector<sid_t> vs, int tgt_node);

		int client_new_reassign(int loc_s, sid_t v, int tgt_node);

		//int client_reassign2(int loc_s, sid_t v, int tgt_node);

		bool client_reassign_vd(int loc_s, sid_t v, dir_t d, int tgt_node);

		bool client_migrate_vd(int loc_s, sid_t v, dir_t d, int tgt_node);

		double fennel(int src_weight, int server_weight, 
				int active_obj_num, 
				int avg_weight, 
				int o_weight, int locality);

		//binds are stored in the corresponding sid.
		int client_pattern_evaluate(int sid, const vector<triple_t> &triples, 
				const vector<vector<sid_t>> &binds, vector<triple_t> *results);

		void client_pattern_evaluate_local2(QueryNode* queryRoot, vector<triple_t>* results);

		void matchPattern(QueryNode* node, QueryEdge* edge, vector<triple_t>* results);

		void matchPattern2(QueryNode* node, QueryEdge* edge, vector<triple_t>* results);

		void bacth_loadEntities_global(const vector<sid_t> pset, dir_t d, vector<sid_t>* result);

		void testKeyIndex();

		void testDumpAndRestore();

		void testMoveData();

		void testReadAndWrite();

		int _client_pattern_evaluate(int sid, vector<sid_t> src_bind, QueryNode* queryRoot, 
				vector<triple_t> *results);

		void insert_DoI_index(vector<triple_t> tripple_res, int tgt_s);

		void insert_DoI_index2(vector<triple_t> tripple_res, int tgt_s);

		void insert_DoI_index3(vector<triple_t> tripple_res, int tgt_s);

		void insert_DoI_index4(vector<triple_t> tripple_res, int tgt_s);

		void insert_DoI_index_new(vector<triple_t> tripple_res, int tgt_s);

		void insert_DoI_index_new2(vector<triple_t> tripple_res, int tgt_s);

		bool update_DoI_Index(sid_t v, int loc, int tgt);
		
		int client_pattern_evaluate_global(QueryNode* queryRoot, 
				vector<triple_t> *results);
		
		int client_pattern_evaluate_local(QueryNode* queryRoot, 
				vector<triple_t> *results);

		int evaluate_root_binding_local(QueryNode* queryRoot, vector<sid_t>* src_bind);

		QueryNode* parse_patterns(const vector<triple_t> &triples, const vector<vector<sid_t>> &binds);

		/*edge weight*/
		int batch_up_edgeweight(int sid, sid_t root_v, const vector<triple_t> &triples);
		int batch_up_vertexweight(int sid, const vector<sid_t> &vs, int incr_weight);
		
		int batch_up_DoI_index_info(int sid, const vector<triple_t>& triples);
		/*
		 * batch request & release lock
		 * on the initial server of each obj, a lock is maintain for concurrent read/write and reassignment
		 * return: failed vertex
		 * */
		//vector<sid_t> batch_request_global_lock(vector<sid_t> vs, lock_t lock);
		//vector<sid_t> batch_release_global_lock(vector<sid_t> vs, lock_t lock);

		//bool request_global_lock(sid_t v, lock_t lock);
		//bool release_global_lock(sid_t v, lock_t lock);
		int request_global_reassign_lock(int loc_s, sid_t v);

		//int batch_request_lock(int sid, vector<sid_t> vs, lock_t lock, vector<sid_t> *failed_sets);
		//int batch_release_lock(int sid, vector<sid_t> vs, lock_t lock, vector<sid_t> *failed_sets);


		//modify in 2020/07/31

		/*
		* function For a given entity e, return its hosting node. 
		* input : v : vertex id
		* output : server_id 
		*/
		sid_t loadNode(sid_t v);
		/*
		* function For a given entity e, return its hosting node.
		* input : v : vertex name
		* output : server_id
		*/
		sid_t loadNode(const string& text);

		/*
		* function :For a given predicate p,  return nodes hosting entities which have at least one incoming (dir = IN) or outgoing (dir = OUT) edge labeled as p. 
		* 
		*/
		vector<sid_t>* loadNodes(sid_t p, dir_t d);

		/*
		* function : Return entities hosted on the current node that have at least incoming (dir = IN) or outgoing (dir = OUT) edge labeled as p
		* 
		*/
		vector<sid_t>* loadEntities(sid_t p, dir_t d);

		/*
		* function : For a given entity e on the current node, return its incoming (dir = IN) or outgoing (dir = OUT) predicates. 
		*/
		vector<sid_t>* loadPredicates(sid_t v, dir_t d);

		/*
		* function : For a given entity e and predicate p on the current node, return its incoming (dir = IN) or outgoing (dir = OUT) neighbors
		*/
		vector<sid_t>* loadNeighbors(sid_t v, sid_t p, dir_t d);


};
#endif 

