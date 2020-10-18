#ifndef WEIGHT_UPDATE_TASK_HPP
#define WEIGHT_UPDATE_TASK_HPP

#include <vector>
#include "triple_t.hpp"

class WeightUpdateTask{
	
	private:
		int type; //0 - edgweight, 1 - vertex weight add 2, DoI Index info
		sid_t root_v = 0;
		vector<triple_t> edges_to_weight;
		vector<sid_t> vertex_to_weight;
		int incr_weight;

	public:
		WeightUpdateTask(sid_t root_v, vector<triple_t> &edges_to_weight)
		:root_v(root_v),edges_to_weight(edges_to_weight){
			type = 0;
		}

		WeightUpdateTask(vector<triple_t>& edges_to_weight)
			:edges_to_weight(edges_to_weight) {
			type = 2;
		}
		WeightUpdateTask(vector<sid_t> &vertex_to_weight, int incr_weight)
			:vertex_to_weight(vertex_to_weight), 
			incr_weight(incr_weight)
		{
			type = 1;
		}
		//new way to update vertext info
		WeightUpdateTask(vector<sid_t>& vertex_to_weight, int incr_weight, vector<triple_t>& DoI_edges)
			:vertex_to_weight(vertex_to_weight), edges_to_weight(DoI_edges), incr_weight(incr_weight)
		{
			type = 1;
		}
		virtual ~WeightUpdateTask(){
		}

		vector<triple_t> getEdge(){
			return edges_to_weight;
		}

		sid_t getRoot_v(){return root_v;}

		vector<sid_t> getVertex(){
			return vertex_to_weight;
		}

		int getIncrWeight(){
			return incr_weight;
		}

		bool isUpdateEdge(){
			if (type == 0)
				return true;
			else return false;
		}
		bool isUpdateVertex() {
			if (type == 1)
				return true;
			else return false;
		}

};
#endif
