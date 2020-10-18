// 用户端和服务器端分离操作。
// 这里是用户端模块，负责发送查询数据
//1.两种方式发送数据
//1-1 用户端读取配置文件并发送Query数据，灵活方便
//1-2 用户端只负责发送QueryNumber, 服务器端读取配置文件生成基础查询数据 用户端是个工具人
#include "mpi.h"
#include "DataLoader.hpp"
#include "Repartitioner.hpp"
#include "HeliosConfig.hpp"
#include "../server/server2.hpp"
#include "client2.hpp"

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
	if (argc < 3) errorReport("Usage: ./helios_client <numberOfClients> <numberOfQueries>");
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
	//Memory* mem = new Memory(global_num_servers);

	////////////////////////////////
	//1.用户端读取生成查询
	//add workload info
	vector<QueryBank> vec_workload = get_workload_config_info(&config); //通过配置文件生成查询

	//开启查询任务
	for (int client_nbr = 1; client_nbr <= numberOfClients; client_nbr++)
		clientThread_list.push_back(boost::thread(boost::bind(&client_task2, current_nodeName,
			numberOfQueries, client_nbr, &config, vec_workload)));

	////////////////////////////////
	// Wait for terminating the clients and servers.
	for (int client_nbr = 1; client_nbr <= numberOfClients; client_nbr++)
		clientThread_list[client_nbr - 1].join();

	
	cout << "client task thread end " << endl;
	////////////////////////////////
	MPI_Finalize();

	return 0;
}
