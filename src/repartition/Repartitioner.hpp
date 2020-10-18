#ifndef REPARTITIONER_HPP_
#define REPARTITIONER_HPP_

#include <vector>
#include "Database.hpp"
#include "ReassignQueue.hpp"
#include "HeliosConfig.hpp"

using namespace std;
class Repartitioner{

	private:
		//int num_dbs;
		vector<Database*> db;
		//Database* db;
		int sid;
		HeliosConfig *cfg;
		Memory* mem;
		int num_servers;
	public:
		Repartitioner(int sid, HeliosConfig* cfg, Memory* mem)
			: sid(sid), cfg(cfg), mem(mem){
			num_servers = cfg->global_num_servers;
			db.resize(num_servers);
			for(int i = 0; i < num_servers; i++)
				db[i] = new Database(sid, cfg, mem);
		}

		virtual ~Repartitioner(){
			for(int i = 0; i < num_servers; i++)
				delete db[i];
		}

		void run();

		void up_edge_weight();

		ReassignQueue* queue(){
			return mem->reassign_queue;
			//return db->getMem()->reassign_queue;
		}
	
};
#endif
