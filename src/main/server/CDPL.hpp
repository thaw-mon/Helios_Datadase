//////////////////////////////////////////////////////////////////////////////////
// Our database pool stores a list of handles to the database, where each worker thread 
// has its specific handle

namespace cdpl {

	class database_pool {
		/////////////////////////////
		private:
			vector <Database *> db_list;
			HeliosConfig* config;	
			int database_pool_size;			
		/////////////////////////////
		public:
			database_pool(HeliosConfig* c) /*: config(c), db_list(c->_workerThread_nbr_)*/ {
				config = c;
				int database_pool_size = 0;
				for (int L = 0; L < config->_workerThread_nbr_per_level_.size(); L++)
					database_pool_size += config->_workerThread_nbr_per_level_[L];
				db_list.resize(database_pool_size);
			}
			
			/////////////////////////////
		    ~database_pool(){
               for (int i = 0; i < database_pool_size; i++) 
				   if (db_list[i] != NULL) delete db_list[i];
			};

			/////////////////////////////		
		    void set(int id, Database *db) {
				// Store PUSH sockets in the socket pool
				db_list[id] = db; 
			}	
			
			/////////////////////////////		  	
			Database *get(int id) { 
				return(db_list[id]);
			}				
	};
}
