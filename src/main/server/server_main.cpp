///////////////////////////////////////////////////////////////////////////////////
#include "mpi.h"
#include "DataLoader.hpp"
#include "Repartitioner.hpp"
#include "HeliosConfig.hpp"
#include "server2.hpp"
//#include "../client/client2.hpp"

#define LOAD_DICT_ONLY 0
#define LOAD_DATA_ONLY 1
#define LOAD_DICT_DATA 2

// ///////////////////////////////////////////////////////////////////////////////////
// using namespace std;

void* load_thread2(void* arg) {
	DataLoader* loader = (DataLoader*)arg;
	loader->load_data(LOAD_DICT_DATA);
}

void* repartitioner_thread2(void* arg) {
	Repartitioner* repartitioner = (Repartitioner*)arg;
	repartitioner->run();
}

void* up_edge_weight_thread2(void* arg) {
	Repartitioner* repartitioner = (Repartitioner*)arg;
	repartitioner->up_edge_weight();
}
///////////////////////////////////////////////////////////////////////////////////
//TODO 输入格式为 ./server numberOfClients numberOfQueries 1 //load data
// ./ server numberOfClients numberOfQueries  //run query

int main(int argc, char* argv[]) {

	//1.遍历参数
	for (int i = 0; i < argc; i++) {
		cout << "argv[" << i << "] = " << argv[i] << ",";
	}
	cout << endl;

	/////////////////////////
	int numberOfClients;
	int numberOfQueries;
	string current_nodeName;
	vector<boost::thread> clientThread_list;

	////////////////////////// Validate the number of command line parameters.
	if (argc < 3) errorReport("Usage: ./server <numberOfClients> <numberOfQueries>");
	numberOfClients = atoi(argv[1]);
	numberOfQueries = atoi(argv[2]);

	if (numberOfClients < 1) errorReport("At least one client is required!");
	if (numberOfQueries < 1) errorReport("At least one message needs to be sent!");

	////////////////////////////////
	MPI_Init(NULL, NULL);

	int32_t global_num_servers;
	MPI_Comm_size(MPI_COMM_WORLD, &global_num_servers);

	int32_t world_rank;
	MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);


	////////////////////////////////
	HeliosConfig config("../config/helios.cfg", global_num_servers);
	current_nodeName = config.host_names[(int)world_rank];
	assert(global_num_servers == config.global_num_servers);
	//shared by all threads
	Memory* mem = new Memory(global_num_servers);


	////////////////////////////////
	_workerThread_index_per_level_.resize(config._workerThread_nbr_per_level_.size());
	_workerThread_index_per_level_[0] = 0;
	for (int i = 1; i < _workerThread_index_per_level_.size(); i++)
		_workerThread_index_per_level_[i] = _workerThread_index_per_level_[i - 1] +
		config._workerThread_nbr_per_level_[i - 1];


	if (argc == 4) {

		cout << "start load data" << endl;
		if (config.initial_metis) {
			//use metis as the initial partitioning method
			DataLoader* loader = new DataLoader(world_rank, -1, &config, mem);
			loader->load_vloc_metis();
			printf("%d complete load metis partitioning \n", world_rank);
			delete loader;
		}

		double begin = MPI_Wtime();
		int num_loader_threads = config.num_loader_threads;
		pthread_t* loader_threads = new pthread_t[num_loader_threads];
		vector<DataLoader*> loaders;
		for (int tid = 0; tid < num_loader_threads; tid++) {
			DataLoader* loader = new DataLoader(world_rank, tid, &config, mem);
			pthread_create(&(loader_threads[tid]), NULL, load_thread2, (void*)loader);
			loaders.push_back(loader);
		}

		for (size_t t = 0; t < num_loader_threads; t++) {
			int rc = pthread_join(loader_threads[t], NULL);
			if (rc) {
				printf("ERROR: return code from pthread_join() is %d\n", rc);
				exit(-1);
			}
		}

		for (int i = 0; i < loaders.size(); i++) {
			delete loaders[i];
		}
		delete[] loader_threads;
		printf("%d data loading time: %9.7f second\n", world_rank, MPI_Wtime() - begin);
	}
	else {
		////////////////////////////////
		sPool = new cspl::socket_pool(&config);
		dbPool = new cdpl::database_pool(&config);

		//add workload info
		//vector<QueryBank> vec_workload = get_workload_config_info(&config);

		////////////////////////////////
		// Create and run the following 3 server threads:
		boost::thread query_p = boost::thread(boost::bind(&query_proxy2, world_rank, &config, mem));
		boost::this_thread::sleep(boost::posix_time::milliseconds(10));

		boost::thread queryPlan_p = boost::thread(boost::bind(&queryPlan_proxy2, world_rank, &config, mem));
		boost::this_thread::sleep(boost::posix_time::milliseconds(10));

		boost::thread subQueryPlan_p = boost::thread(boost::bind(&subQueryPlan_proxy2, world_rank, &config, mem));

		//////////////////////////////////
		vector<Repartitioner*> reparts;
		pthread_t* repart_threads;
		int num_repart_threads = config.num_metadata_threads + 1;
		if (config.enable_reassign) {
			cout << "start Repartitioner" << endl;
			repart_threads = new pthread_t[num_repart_threads];
			Repartitioner* repart = new Repartitioner(world_rank, &config, mem);//, global_num_servers);
			reparts.push_back(repart);
			pthread_create(&(repart_threads[0]), NULL, repartitioner_thread2, (void*)repart);
			for (int i = 1; i < num_repart_threads; i++) {
				Repartitioner* repart2 = new Repartitioner(world_rank, &config, mem);//, 1);
				reparts.push_back(repart2);
				pthread_create(&(repart_threads[i]), NULL, up_edge_weight_thread2, (void*)repart2);
			}
			/*	boost::thread metaDataHandler_p = boost::thread(boost::bind(&metaDataHandler_proxy,
											world_rank, &config, mem));
				boost::this_thread::sleep( boost::posix_time::milliseconds(10) );
				metaDataHandler_p.join();*/
		}
		//////////////////////////////////
		query_p.join();
		queryPlan_p.join();
		subQueryPlan_p.join();

		if (config.enable_reassign) {
			for (size_t t = 0; t < num_repart_threads; t++) {
				int rc = pthread_join(repart_threads[t], NULL);
				if (rc) {
					printf("ERROR: return code from pthread_join() is %d\n", rc);
					exit(-1);
				}
			}
			for (size_t t = 0; t < num_repart_threads; t++) {
				delete reparts[t];
			}
			delete[] repart_threads;
		}

	}

	//////////////////////////////
	delete mem;
	MPI_Finalize();
	////////////////////////////////

}






