//////////////////////////////////////////////////////////////////////////////////
// Our socket pool is a map (a list <of worker threads> of lists <of PUSH/PULL sockets> 
//////////////////////////////////////////////////////////////////


//////////////////////////////////////////////////////////////////
namespace cspl {

    class socket_pool {

		/////////////////////////////
		private:
			spinlock map_lock;
			typedef vector<zmq::socket_t *> socket_list;
			typedef pair<zmq::socket_t*, string> pair_k;
			vector <vector <pair_k>> map;
			HeliosConfig* config;
			int socket_pool_size;
		
		//////////////////////////////
		public:
			socket_pool(HeliosConfig* c) { /*: config(c), map(c->_workerThread_nbr_, vector<pair_k>(_socketsPerWorkerThread_)) {}*/
				config = c;
				socket_pool_size = 0;
				for (int L = 0; L < config->_workerThread_nbr_per_level_.size() - 1; L++)
					socket_pool_size += config->_workerThread_nbr_per_level_[L];
				map.resize(socket_pool_size, vector<pair_k>(_socketsPerWorkerThread_));
			//	std::cout << "socket_pool_size" << socket_pool_size << std::endl;
			}
			
			
			/////////////////////////////
		    ~socket_pool(){
               for (int t_nbr = 0; t_nbr < socket_pool_size; t_nbr++)
				  for (int s_nbr = 0; s_nbr < _socketsPerWorkerThread_; s_nbr++)
						if (map[t_nbr][s_nbr].first != NULL) delete map[t_nbr][s_nbr].first;
			};

			/////////////////////////////		
		    void store_mySockets(int id, socket_list sList, string portNo) {
				
				// Store PUSH sockets for a worker thread (with identifier ID) in the socket pool
				for (int sNo = 0; sNo < (_socketsPerWorkerThread_ - 1); sNo++) {
					map[id][sNo].first = sList[sNo]; 
					(map[id][sNo].second).clear();
				  }	
				  
				// Store PULL socket in the socket pool
				map[id][_socketsPerWorkerThread_ - 1].first = sList[_socketsPerWorkerThread_ - 1]; 
                map[id][_socketsPerWorkerThread_ - 1].second = portNo; 
				}  			  
			
			/////////////////////////////		  	
			zmq::socket_t *takeSocket(int id, int col = (_socketsPerWorkerThread_ - 1), string endpoint = "") { 
				map_lock.lock();
				int success;
				if ((id < 0) || (id >= socket_pool_size) ||
					(col < 0) || (col >= _socketsPerWorkerThread_))
					{
						std::cout << "id=" << id << std::endl;
						std::cout << "socket_pool_size=" << socket_pool_size << std::endl;
						std::cout << "col=" << col << std::endl;
						std::cout << "_socketsPerWorkerThread_" << _socketsPerWorkerThread_ << std::endl;
						errorReport("Invalid worker Thread or socket number!");	
					}
			//	else
			//	{
			//		std::cout << "socket_pool_size normal id=" << id << std::endl; 
			//	}		
		    
				if (col == (_socketsPerWorkerThread_ - 1)) { 
					map_lock.unlock();
					return map[id][col].first; }
			
				if ((map[id][col].second).empty()) {
					connect_(*(map[id][col].first), endpoint);
					
					/////////////////////////////	
					// The ZMQ_LINGER option shall set the linger period for the specified socket. 
					// The linger period determines how long pending messages which have yet to be sent to a 
					// peer shall linger in memory after a socket is disconnected or closed.
					// int linger = -1;
					// (map[id][col].first)->setsockopt (ZMQ_LINGER, &linger, sizeof (linger));
					
					map[id][col].second = endpoint;					
				} else {
					disconnect_(*(map[id][col].first), map[id][col].second);
					// boost::this_thread::sleep( boost::posix_time::milliseconds(30) );
					connect_(*(map[id][col].first), endpoint);
					
					/////////////////////////////	
					// The ZMQ_LINGER option shall set the linger period for the specified socket. 
					// The linger period determines how long pending messages which have yet to be sent to a 
					// peer shall linger in memory after a socket is disconnected or closed.
					// int linger = -1;
					// (map[id][col].first)->setsockopt (ZMQ_LINGER, &linger, sizeof (linger));
					
					map[id][col].second = endpoint;
				}
				map_lock.unlock();
				return map[id][col].first;
			}	
		
			/////////////////////////////
			string takePortNo(int id) { 
				if ((id < 0) || id > (socket_pool_size - 1)) 
					errorReport("Invalid socketNo!");		
				return map[id][_socketsPerWorkerThread_ - 1].second;
			}
	};
}
