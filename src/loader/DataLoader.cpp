#include "DataLoader.hpp"
#include <fstream>
#include <algorithm>
#include "unit.hpp"

using namespace std;

/* First load in mem */
////////////////////////////////////////////////////////////////////
void dedup_itriples(vector<itriple_t> &triples){
	if (triples.size() <= 1)
		return;

	uint64_t n = 1;
	for (uint64_t i = 1; i < triples.size(); i++) {
		/*if (triples[i].s == triples[i - 1].s
		        && triples[i].p == triples[i - 1].p
	        	&& triples[i].o == triples[i - 1].o)*/
		if(memcmp(&triples[i], &triples[i-1], sizeof(itriple_t)) == 0)
			continue;

		triples[n++] = triples[i];
	}
	triples.resize(n);
} 

uint64_t inline floor(uint64_t original, uint64_t n) {
	assert(n != 0);
	return original - original % n;
}

uint64_t inline ceil(uint64_t original, uint64_t n) {
	assert(n != 0);
	if (original % n == 0)
		return original;
	return original - original % n + n;
}
	// selectively load own partitioned data from all files
int DataLoader::load_data_from_allfiles(vector<string> &fnames){
	uint64_t t1 = get_usec();

	sort(fnames.begin(), fnames.end());

	int num_files = fnames.size();
	int num_load_thread = cfg->num_loader_threads;
	int num_servers = cfg->global_num_servers;
	#pragma omp parallel for num_threads(num_load_thread)
	for (int i = 0; i < num_files; i++) {
		int localtid = omp_get_thread_num();
		//printf("thread %d load %s\n", localtid, fnames[i].c_str());
		uint64_t kvs_sz = floor(mem_sz / num_load_thread - sizeof(uint64_t), sizeof(sid_t));
		uint64_t *pn = (uint64_t *)(mem + (kvs_sz + sizeof(uint64_t)) * localtid);
		sid_t *kvs = (sid_t *)(pn + 1);

		// the 1st uint64_t of kvs records #triples
		uint64_t n = *pn;

		ifstream file(fnames[i].c_str());
		sid_t s, p, o;
		while (file >> s >> p >> o) {
			int s_sid = jumpConsistentHash(s, num_servers);
			int o_sid = jumpConsistentHash(o, num_servers);
			if ((s_sid == server_id) || (o_sid == server_id)) {
				assert((n * 3 + 3) * sizeof(uint64_t) <= kvs_sz);

				// buffer the triple and update the counter
				kvs[n * 3 + 0] = s;
				kvs[n * 3 + 1] = p;
				kvs[n * 3 + 2] = o;
				n++;
			}
		}
		file.close();
		*pn = n;
		//printf("thread %d load %s end!\n", localtid, fnames[i].c_str());
	}

	// timing
	uint64_t t2 = get_usec();
	cout << (t2 - t1) / 1000 << " ms for loading RDF data files (w/o networking)" << endl;
	return num_load_thread;
}

void DataLoader::aggregate_data(){
	uint64_t t1 = get_usec();
	int num_partitions = cfg->num_loader_threads;
	int nthread_parallel_load = cfg->num_loader_threads;
	int num_servers = cfg->global_num_servers;

	// calculate #triples on the kvstore from all servers
	uint64_t total = 0;
	uint64_t kvs_sz = floor(mem_sz / num_partitions - sizeof(uint64_t), sizeof(sid_t));
	for (int id = 0; id < num_partitions; id++) {
		uint64_t *pn = (uint64_t *)(mem + (kvs_sz + sizeof(uint64_t)) * id);
		total += *pn; // the 1st uint64_t of kvs records #triples
	}

	// pre-expand to avoid frequent reallocation (maybe imbalance)
	for (int i = 0; i < triple_spo.size(); i++) {
		triple_spo[i].reserve(total / nthread_parallel_load);
		triple_ops[i].reserve(total / nthread_parallel_load);
	}

	// each thread will scan all triples (from all servers) and pickup certain triples.
	// It ensures that the triples belong to the same vertex will be stored in the same
	// triple_spo/ops. This will simplify the deduplication and insertion to gstore.
	volatile int progress = 0;
	#pragma omp parallel for num_threads(nthread_parallel_load)
	for (int tid = 0; tid < nthread_parallel_load; tid++) {
		int cnt = 0; // per thread count for print progress
		for (int id = 0; id < num_partitions; id++) {
			uint64_t *pn = (uint64_t *)(mem + (kvs_sz + sizeof(uint64_t)) * id);
			sid_t *kvs = (sid_t *)(pn + 1);

			// the 1st uint64_t of kvs records #triples
			uint64_t n = *pn;
			for (uint64_t i = 0; i < n; i++) {
				sid_t s = kvs[i * 3 + 0];
				sid_t p = kvs[i * 3 + 1];
				sid_t o = kvs[i * 3 + 2];

				// out-edges
				if (jumpConsistentHash(s, num_servers) == server_id)
					if ((s % nthread_parallel_load) == tid)
						triple_spo[tid].push_back(itriple_t(s, p, o));

				// in-edges
				if (jumpConsistentHash(o, num_servers) == server_id)
					if ((o % nthread_parallel_load) == tid)
						triple_ops[tid].push_back(itriple_t(o, p, s));

				// print the progress (step = 5%) of aggregation
				if (++cnt >= total * 0.05) {
					int now = __sync_add_and_fetch(&progress, 1);
					if (now % nthread_parallel_load == 0)
						cout << "already aggregrate " << 
						(now / nthread_parallel_load) * 5 << "%" << endl;
					cnt = 0;
				}
			}
		}
		uint64_t sort_time = get_usec();
		sort(triple_spo[tid].begin(), triple_spo[tid].end(), edge_sort_by_spo());
		dedup_itriples(triple_spo[tid]);
		//printf("tid %d spo size %d dedup time %ld \n", tid, triple_spo[tid].size(), get_usec()-sort_time);
		
		sort(triple_ops[tid].begin(), triple_ops[tid].end(), edge_sort_by_spo());
		dedup_itriples(triple_ops[tid]);
		//printf("tid %d ops size %d\n", tid, triple_ops[tid].size());
	}
	uint64_t t2 = get_usec();
	cout << (t2 - t1) / 1000 << " ms for aggregrating triples" << endl;
}

void DataLoader::load_mem_data(){
	mem_sz = GiB2B(cfg->memdb_size_gb);
	mem = (char*)malloc(mem_sz);
	memset(mem, 0, mem_sz);
	
	vector<string> data_files = cfg->data_files;
	int num_servers = cfg->global_num_servers;
	int nthread_parallel_load = cfg->num_loader_threads;

	triple_spo.resize(nthread_parallel_load);
	triple_ops.resize(nthread_parallel_load);
	
	load_data_from_allfiles(data_files);
	aggregate_data();

	uint64_t t1 = get_usec();
	
	#pragma omp parallel for num_threads(nthread_parallel_load)
	for (int t = 0; t < nthread_parallel_load; t++) {
		//uint64_t pos = t * tid_key_sz;
		db->load_data(triple_spo[t], OUT);	
		db->load_data(triple_ops[t], IN);
		
		vector<itriple_t>().swap(triple_spo[t]);
		vector<itriple_t>().swap(triple_ops[t]);
	}
	uint64_t t2 = get_usec();
	cout << (t2 - t1) / 1000 << " ms for inserting normal data into gstore" << endl;
}
//////////////////////////////////////////////////////////////////////////////////////////////

void DataLoader::load_local_dict(){
	int num_threads = cfg->num_loader_threads;
	int num_servers = cfg->global_num_servers;
	//cout << "load dict start : num_servers = " << num_servers << endl;
	//cout << "current server_id = " << server_id << endl;
	for(int i = 0; i < (cfg->dict_files).size(); i++){
		if(i % num_threads != thread_id) continue;
		
		//current thread takes over ith dict_file
		printf("%d %d %s\n", i, thread_id, ((cfg->dict_files)[i]).c_str());
		string str;
		sid_t id;
		string line;
		
		ifstream file((cfg->dict_files)[i]);
		while(getline(file, line)){
	        	stringstream strs(line);
	    		getline(strs, str, '\t');
			string id_str;
			getline(strs, id_str);
			id = stringToNum<sid_t>(id_str);
		//while(file >> str >> id){
			if(server_id == jumpConsistentHash(id, num_servers)){
				db->insert_local_dict(id, str);
			}

			sid_t str_hash = hash64(str.c_str(),0);
			if(server_id == jumpConsistentHash(str_hash, num_servers)){
				db->insert_local_dict(str, id);
			}
		}
		//db->load_local_dict(((cfg->dict_files)[i]).c_str());
		db->flush_local_command();	
	}

	cout << "success insert dict data" << endl;
}


/*
 * Every thread on the local server takes over 1 file.
 * */
void DataLoader::load_local_data(){
	int num_threads = cfg->num_loader_threads; //д╛хо20
	int num_servers = cfg->global_num_servers;
	//cout << "load data start : num_servers = " << num_servers << endl;
	//cout << "current server_id = " << server_id << endl;
	for(int i = 0; i < (cfg->data_files).size(); i++){
		if(i % num_threads != thread_id) continue;
		
		//current thread takes over ith data_file
		//db->load_local_data(((cfg->data_files)[i]).c_str());// This one is little bit faster
		printf("load data : server_id = %d i= %d thread_id = %d path = %s\n", server_id, i, thread_id, ((cfg->data_files)[i]).c_str());
		//current thread takes over ith data_file
		ifstream file((cfg->data_files)[i]);
		//string s_str, p_str, o_str, dot_str;
		sid_t s, p, o;
		while(file >> s >> p >> o ){
			//printf("start insert triple s =%ld ,p = %ld, o = %ld\n",s,p,o);
			if(server_id == jumpConsistentHash(s, num_servers)){
				triple_t t_out(s, p, o, OUT);
				db->add_local_triple(t_out);
			}

			if(p != 1 && server_id == jumpConsistentHash(o, num_servers)){
				triple_t t_in(o, p, s, IN);
				db->add_local_triple(t_in);
			}
			//printf("finish insert triple s =%ld ,p = %ld, o = %ld\n",s,p,o);
		}
		db->flush_local_command();
	}
}

/////////////////////////////////////////////////////
void DataLoader::load_vloc_metis(){
	sid_t reduRange = 131072;
	int num_servers = cfg->global_num_servers;

	ifstream file(cfg->metis_part);
	sid_t line = 0;
	int v_loc;
	while(file >> v_loc){
		sid_t vid = line + reduRange;
		line ++;

		if(server_id == jumpConsistentHash(vid, num_servers) 
			&& v_loc != server_id){
			/*update loc function*/
			db->batch_set_local_vloc(vid, v_loc);	
		}

		if(v_loc == server_id){
			/*insert the vid to the list of loc server*/
			shared_mem->set_local_metis(vid);	
		}
	}
	db->flush_local_command();
}

void DataLoader::load_local_data_metis(){

	int num_threads = cfg->num_loader_threads;
	int num_servers = cfg->global_num_servers;

	for(int i = 0; i < (cfg->data_files).size(); i++){
		if(i % num_threads != thread_id) continue;
		
		printf("%d %d %s\n", i, thread_id, ((cfg->data_files)[i]).c_str());
		ifstream file((cfg->data_files)[i]);
		sid_t s, p, o;
		while(file >> s >> p >> o ){
			
			if(shared_mem->is_local_metis(s)){
				triple_t t_out(s, p, o, OUT);
				db->add_local_triple(t_out);
			}

			if(p != 1 && shared_mem->is_local_metis(o)){
				triple_t t_in(o, p, s, IN);
				db->add_local_triple(t_in);
			}
		}
		db->flush_local_command();
	}
}

/*
 * Every thread over all server take over one file
 * */
void DataLoader::load_all_data(){
	int num_threads = cfg->num_loader_threads;
	int num_servers = cfg->global_num_servers;
	
	for(int i = 0; i < (cfg->data_files).size(); i++){
		if(i % num_servers != server_id) continue;

		if((i-num_servers+1) % num_threads != thread_id) continue;
		
		//current thread takes over ith data_file
		printf("@%d %d %d %s\n", server_id, i, thread_id, ((cfg->data_files)[i]).c_str());
		//current thread takes over ith data_file
		ifstream file((cfg->data_files)[i]);
		//string s_str, p_str, o_str, dot_str;
		sid_t s, p, o;
		while(file >> s >> p >> o ){
			
			triple_t t_out(s, p, o, OUT);
			db->add_global_triple(t_out);

			if(p != 1){
				triple_t t_in(o, p, s, IN);
				db->add_global_triple(t_in);
			}
		}
		db->flush_global_command();
	}
}

