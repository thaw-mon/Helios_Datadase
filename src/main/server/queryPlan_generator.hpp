//////////////////////////////////////////////////////////////////
using namespace std;
#include "QueryEdge.hpp"
#include "QueryNode.hpp"
#include "triple_t.hpp"
#include "Database.hpp"
#include "RDF.hpp"
/////////////////////////////
// This struct contains the result of each query plan, which should be sent from a <queryPlan task> to 
// the aorresponding <query task>.
struct queryPlan_result {

	// The list of RDF instances of the joint (non-leaf) nodes (of the query plan) after their prune 
	// (in step1 & step2).
	vector <vector<triple_t>> result_map;

	/////////////////////////////
	vector <vector<sid_t>> binding_map;
	// The list of values of the joint (non-leaf) nodes (of the query plan) after their prune (in step1 & step2).

	// The list of objects/subjects (of the triple patterns of the query plan) where 
	// each bound subject/object is a list of values, and each non-bound subject/object is a variable.
	vector<RDFtermList> subListOrObjList_Set;

	// The list of pruned_step1 flags of the joint (non-leaf) nodes (either root or its children) 
	// of the query plan that determine whether they were pruned at step1 or not.
	vector<bool> pruned_step1List;

	// The list of pruned_step2 flags of the joint (non-leaf) nodes (either root or its children) 
	// of the query plan that determine whether they were pruned at step1 or not.r not.
	vector<bool> pruned_step2List;

	//TODO add new value edge_count info
	sid_t edge_count;
	sid_t edge_count_spend_time;
	/////////////////////////////
	// The <ip> and <port> whereby the result of query plan should be sent (to the appropriate <query task>)
	string sender_ip;
	string sender_port;
	uint64_t time_sent;
	uint64_t eval_time;

	/////////////////////////////
	queryPlan_result() {
		result_map.clear();
		binding_map.clear();
		subListOrObjList_Set.clear();
		pruned_step1List.clear();
		pruned_step2List.clear();
		time_sent = get_usec();
		eval_time = 0;
		edge_count = 0;
		edge_count_spend_time = 0;
	}
	//
	//
	void  addEdgeCountSpendTime(sid_t time) {
		edge_count_spend_time += time;
	}
	void setEdgeCountSpendTime(sid_t time) {
		edge_count_spend_time = time;
	}
	sid_t getEdgeCountSpendTime() {
		return edge_count_spend_time;
	}

	void addEdgeCount(sid_t count) {
		edge_count += count;
	}

	void setEdgeCount(sid_t count) {
		edge_count = count;
	}

	sid_t getEdgeCount() {
		return edge_count;
	}
	/////////////////////////////
	void setIP(string ip) {
		sender_ip = ip;
	}

	/////////////////////////////
	void setPort(string port) {
		sender_port = port;
	}

	/////////////////////////////
	void setTime_sent(uint64_t t) {
		time_sent = t;
	}

	/////////////////////////////
	uint64_t getTime_sent() {
		return(time_sent);
	}

	/////////////////////////////
	template <typename Archive> void serialize(Archive& ar, const unsigned int version) {
		ar& result_map;
		ar& pruned_step1List;
		ar& pruned_step2List;
		ar& binding_map;
		ar& subListOrObjList_Set;
		ar& sender_ip;
		ar& sender_port;
		ar& time_sent;
		ar& eval_time;
		ar& edge_count;
		//ar& edge_count_spend_time;
	}
};

/////////////////////////////
// This struct contains each calculated query plan, which should be sent from a <query task> to 
// the corresponding <queryPlan tasks>.
struct queryPlan {

	/////////////////////////////
	// The root node of the query plan tree
	QueryNode* root;

	/////////////////////////////
	// The list of nodes of the query plan tree
	vector<QueryNode*> nodeList;
	
	/////////////////////////////
	// The results of query plan
	queryPlan_result qpr;

	/////////////////////////////
	// the <ip> and <port> whereby the query plan should be sent (to the appropriate <queryPlan task>)
	string sender_ip;
	string sender_port;

	/////////////////////////////
	queryPlan() {
	}

	/////////////////////////////
	void setIP(string ip) {
		sender_ip = ip;
	}

	/////////////////////////////
	void setPort(string port) {
		sender_port = port;
	}

	///////////////////////////// 
	// Recursively output the information of the query plan tree in a depth-first travesral.
	void output(QueryNode* n = NULL) {

		/////////////////
		if (n == NULL) {
			n = root;
			for (int i = 0; i < nodeList.size(); i++)
				nodeList[i]->was_seen = 0;
		}

		/////////////////
		if (!n->was_seen) {

			/////////////////
			for (int i = 0; i < nodeList.size(); i++)
				if (n == nodeList[i]) {
					cout << std::endl << "Visiting node#" << i << std::endl;
					break;
				}

			/////////////////
			for (int i = 0; i < n->edges.size(); i++) {
				cout << std::endl
					<< "Edge#" << i
					<< ", dir: " << (n->edges[i]->d ? OUT : IN)
					<< ", towardsJoint: " << (n->edges[i]->towardsJoint ? "Yes" : "No")
					<< ", exec_flag: " << (n->edges[i]->exec_flag);

				for (int j = 0; j < nodeList.size(); j++)
					if (n->edges[i]->node == nodeList[j]) {
						cout << ", to the node#" << j << std::endl;
						break;
					}
			}

			/////////////////
			n->was_seen = 1;

			/////////////////
			for (int i = 0; i < n->edges.size(); i++)
				output(n->edges[i]->node);
		}
	}

	/////////////////////////////////////////////////////////////////////////
	// Assume <x> is a joint node and <x.result> contains the RDF instances evaluated for <x>.
	// <x.result> and <x.binding> are pruned in two steps:

	// ***Step1**: for each value "s" in <x.binding>, "s" must be the source of some RDF instances (belonging to 
	// <x.result>) with regard to all predicates incident to <x>; otherwise, all RDF instances with "s" as the 
	// source are removed from <x.result> and "s" is removed from <x.binging>.	
	// ***Step2**...
	/////////////////////////////////////////////////////////////////////////
	void prune_step1(QueryNode* sNode, int sNode_resultIndex, string nodeName) {

		/////////////////
		uint64_t time = get_usec();

		/////////////////
		// Deduplicate and sort <sNode->bind_val> using <set1>
		set<sid_t> set1;
		set1.clear();
		for (int i = 0; i < sNode->bind_val.size(); i++)
			set1.insert(sNode->bind_val[i]);
		if (sNode->bind_val.size() > 0)
			sNode->bind_val.assign(set1.begin(), set1.end());

		/////////////////
		// Deduplicate and sort <sNode->bind_to_prune> using <set2>
		set<sid_t> set2;
		set2.clear();
		for (int i = 0; i < sNode->bind_to_prune.size(); i++)
			set2.insert(sNode->bind_to_prune[i]);
		if (sNode->bind_to_prune.size() > 0)
			sNode->bind_to_prune.assign(set2.begin(), set2.end());

		/////////////////
		// Joint points whose <bind_to_prune> is empty do not need any pruning at step1

		if (sNode->bind_to_prune.size() == 0) {

			////////////////
			qpr.binding_map[sNode_resultIndex] = sNode->bind_val;

			////////////////
			determine_bind_val_for_joint_children(sNode, sNode_resultIndex);

			////////////////
			return;
		}

		if (sNode->edges.size() == 1) {

			////////////////
			vector<sid_t> diff;
			set_difference(set1.begin(), set1.end(),
				set2.begin(), set2.end(),
				back_inserter(diff));

			////////////////
			qpr.binding_map[sNode_resultIndex] = sNode->bind_val;
			qpr.pruned_step1List[sNode_resultIndex] = true;

			////////////////
			qpr.binding_map[sNode_resultIndex] = diff;

			////////////////
			determine_bind_val_for_joint_children(sNode, sNode_resultIndex);

			////////////////
			return;
		}

		/////////////////
		// <new_result> contains those triple instances (evaluated for <sNode>) that are not
		// pruned.
		vector <triple_t> new_result;
		new_result.clear();

		/////////////////
		// <new_binding> contains those values (of <sNode>) that are not pruned with resepct to <new_result>
		vector <sid_t> new_binding;
		new_binding.clear();

		/////////////////
		// Note that <sNode->bind_to_prune> is already sorted, so we can apply binary search on it.
		bool found;
		for (int i = 0; i < qpr.result_map[sNode_resultIndex].size(); i++) {

			/////////////////
			found = binary_search(sNode->bind_to_prune.begin(), sNode->bind_to_prune.end(),
				qpr.result_map[sNode_resultIndex][i].s);
			if (!found) {
				new_result.push_back(qpr.result_map[sNode_resultIndex][i]);
				new_binding.push_back(qpr.result_map[sNode_resultIndex][i].s);
			}
		}

		/////////////////
		if ((qpr.result_map[sNode_resultIndex].size()) != (new_result.size())) {

			/////////////////
			qpr.pruned_step1List[sNode_resultIndex] = true;

			/////////////////
			qpr.result_map[sNode_resultIndex] = new_result;

			/////////////////
			// Deduplicate <new_binding> using <set1>
			set1.clear();
			for (int i = 0; i < new_binding.size(); i++)
				set1.insert(new_binding[i]);
			new_binding.assign(set1.begin(), set1.end());
			qpr.binding_map[sNode_resultIndex] = new_binding;
		}

		/////////////////
		determine_bind_val_for_joint_children(sNode, sNode_resultIndex);

		/////////////////
		return;
	}

	/////////////////////////////////////////////////////////////////////////
	// <determine_bind_val_for_joint_children> determines the <bind_val> of <sNode>'s joint children
	// based the results of <sNode>.
	/////////////////////////////////////////////////////////////////////////
	void determine_bind_val_for_joint_children(QueryNode* sNode, int sNode_resultIndex) {

		///////////////////
		QueryNode* tNode;

		/////////////////
		for (int e = 0; e < sNode->edges.size(); e++) {

			/////////////////
			tNode = sNode->edges[e]->node;

			/////////////////
			// If <tNode> is a joint node 																	
			if (tNode->edges.size() != 0) {
				//printf("joint node %s BEGING\n", tNode->name.c_str()); //这里没问题
				sid_t start = get_usec();
				/////////////////
				tNode->bind_val.clear();

				// Deduplicate and sort <tNode->bind_val> using <set1>
				set<sid_t> set1;
				set1.clear();
				vector<triple_t>& result = sNode->edges[e]->result;
				for (triple_t tri_data : result) {
					set1.insert(tri_data.o_vals.begin(), tri_data.o_vals.end());
				}
				/*for (int i = 0; i < tNode->bind_val.size(); i++)
					set1.insert(tNode->bind_val[i]);*/
				tNode->bind_val.assign(set1.begin(), set1.end());
				sid_t end = get_usec();
				sNode->edges[e]->result.clear();
				printf("joint node %s bind_val = %d all spend time = %d ms\n", tNode->name.c_str(), tNode->bind_val.size(),(end - start) / 1000);
			}
		}
	}


	//////////////////////////////////////////////////////////////////////////
	// **step2**: if <x> has some children which in turn are joint nodes, their pruning (in step1 & strep2) 
	// necessiates pruning of <x>. In other words, after pruning (in step1 & step2) of the results and 
	// bindings of the joint children of <x>, <x.result> and <x.binding> need to be pruned in step2. 
	// For more clarification, suppose <y> is a joint child of <x>. Also, "p" is a predicate whose source is <x>
	// and its target is <y>. Also assume we store two triple instances such as <"s", "p", "t1"> and 
	// <"s", "p", "t2"> as one triple instance <"s", "p", {"t1", "t2"}> because of saving space. 
	// As such, for a triple instance <"s", "p", {"t1", "t2"}> belonging to <x.result> where "s" is in <x.binding>
	// there are two cases: 
	// (1) both "t1" and "t2" have already been removed from <y.binding> due to the pruning (in step1 or 2) 
	// of <y>. As a result, "s" is removed from <s.binding>, and every RDF instance (belonging to <x.result>) 
	// whose source is "s" is removed from <x.result>.
	// (2) just "t1" is removed from <y.binding>. Therefore, we just need to update <"s", "p", {"t1", "t2"}> into
	// <"s", "p", {"t2"}>
	//////////////////////////////////////////////////////////////////////////
	void prune_step2(QueryNode* sNode, int sNode_resultIndex, string nodeName) {

		///////////////////
		QueryNode* tNode;
		int tNode_resultIndex;

		/////////////////
		// It contains the pruned values of <sNode> due to pruning of corresponding RDF instances
		std::vector <sid_t> pruned_binding;
		pruned_binding.clear();

		/////////////////
		std::vector <bool> is_pruned;
		for (int i = 0; i < qpr.result_map[sNode_resultIndex].size(); i++)
			is_pruned.push_back(false);

		/////////////////
		for (int e = 0; e < sNode->edges.size(); e++) {

			/////////////////
			tNode = sNode->edges[e]->node;

			/////////////////
			// If <tNode> is a joint node																	
			if (tNode->edges.size() != 0) {

				/////////////////
				// Determine the index of <tNode>'s results in <binding_map>
				for (int i = 0; i < nodeList.size(); i++)
					if (nodeList[i] == tNode) {
						tNode_resultIndex = i;
						break;
					}

				// if <tNode> is already pruned either in step1 or step2
				if (qpr.pruned_step1List[tNode_resultIndex] || qpr.pruned_step2List[tNode_resultIndex]) {

					/////////////////
					// Sort the predicate between <sNode> and <tNode> based on their pIDs
					sort(sNode->edges[e]->bind_val.begin(), sNode->edges[e]->bind_val.end());

					/////////////////
					bool found1, found2, flag;

					/////////////////
					// Prune the sNode's result based on already pruned result of <tNode>
					for (int i = 0; i < qpr.result_map[sNode_resultIndex].size(); i++) {

						/////////////////
						found1 = binary_search(sNode->edges[e]->bind_val.begin(), sNode->edges[e]->bind_val.end(),
							qpr.result_map[sNode_resultIndex][i].p);
						if (found1) {

							/////////////////
							flag = false;

							/////////////////
							std::vector <sid_t> new_oval;
							new_oval.clear();

							/////////////////
							for (int j = 0; j < qpr.result_map[sNode_resultIndex][i].o_vals.size(); j++) {

								/////////////////
								found2 = binary_search(qpr.binding_map[tNode_resultIndex].begin(),
									qpr.binding_map[tNode_resultIndex].end(),
									qpr.result_map[sNode_resultIndex][i].o_vals[j]);
								if (found2) {
									flag = true;
									new_oval.push_back(qpr.result_map[sNode_resultIndex][i].o_vals[j]);
									break;
								}
							}

							/////////////////
							qpr.result_map[sNode_resultIndex][i].o_vals = new_oval;

							if (!flag) {
								is_pruned[i] = true;
								pruned_binding.push_back(qpr.result_map[sNode_resultIndex][i].s);
							}
						}
					}
				}
			}
		}

		/////////////////
		if (pruned_binding.size() == 0) return;

		/////////////////
		// Sort pruned_binding based on their IDs
		sort(pruned_binding.begin(), pruned_binding.end());

		/////////////////
		bool found;
		for (int i = 0; i < qpr.result_map[sNode_resultIndex].size(); i++) {
			found = binary_search(pruned_binding.begin(), pruned_binding.end(),
				qpr.result_map[sNode_resultIndex][i].s);
			if (found) is_pruned[i] = true;
		}

		/////////////////
		// It contains the new values of <sNode> due to pruning of corresponding RDF instances.
		std::vector <sid_t> new_binding;
		new_binding.clear();

		/////////////////
		// <new_result> contains those triple instances (evaluated for <sNode>) that are not pruned.
		std::vector <triple_t> new_result;
		new_result.clear();

		/////////////////
		for (int i = 0; i < is_pruned.size(); i++)
			if (!is_pruned[i]) {
				new_result.push_back(qpr.result_map[sNode_resultIndex][i]);
				new_binding.push_back(qpr.result_map[sNode_resultIndex][i].s);
			}

		/////////////////
		if ((qpr.result_map[sNode_resultIndex].size()) != (new_result.size()))
			qpr.pruned_step2List[sNode_resultIndex] = true;

		/////////////////
		qpr.result_map[sNode_resultIndex] = new_result;

		/////////////////
		// It is used for deduplicating the content of new_binding
		std::set <sid_t> set1;
		set1.clear();

		/////////////////
		for (int i = 0; i < new_binding.size(); i++)
			set1.insert(new_binding[i]);
		new_binding.assign(set1.begin(), set1.end());

		/////////////////
		qpr.binding_map[sNode_resultIndex] = new_binding;
		return;
	}


	/////////////////////////////
	//递归剪枝
	void prune_step_new(QueryNode* sNode, int sNode_resultIndex, string nodeName) {
		///////////////////
		QueryNode* tNode;
		int tNode_resultIndex;

		/////////////////
		// It contains the pruned values of <sNode> due to pruning of corresponding RDF instances
		std::vector <sid_t> pruned_binding;
		pruned_binding.clear();

		/////////////////
		std::vector <bool> is_pruned;
		for (int i = 0; i < qpr.result_map[sNode_resultIndex].size(); i++)
			is_pruned.push_back(false);

		/////////////////
		for (int e = 0; e < sNode->edges.size(); e++) {

			/////////////////
			tNode = sNode->edges[e]->node;

			/////////////////
			// If <tNode> is a joint node																	
			if (tNode->edges.size() != 0) {

				/////////////////
				// Determine the index of <tNode>'s results in <binding_map>
				for (int i = 0; i < nodeList.size(); i++)
					if (nodeList[i] == tNode) {
						tNode_resultIndex = i;
						break;
					}

				// if <tNode> is already pruned either in step1 or step2
				if (tNode->bind_to_prune.size() > 0) {
					sort(sNode->edges[e]->bind_val.begin(), sNode->edges[e]->bind_val.end());
					bool found1, found2,flag;
					for (int i = 0; i < qpr.result_map[sNode_resultIndex].size(); i++) {
						found1 = binary_search(sNode->edges[e]->bind_val.begin(), sNode->edges[e]->bind_val.end(),
							qpr.result_map[sNode_resultIndex][i].p);
						if (found1) {
							std::vector <sid_t> new_oval;
							new_oval.clear();
							for (int j = 0; j < qpr.result_map[sNode_resultIndex][i].o_vals.size(); j++) {
								sid_t current_o_val = qpr.result_map[sNode_resultIndex][i].o_vals[j];
								/////////////////
								found2 = binary_search(tNode->bind_val.begin(),tNode->bind_val.end(),current_o_val);

								if (found2) {
									flag = true;
									new_oval.push_back(current_o_val);
								}
							}
							qpr.result_map[sNode_resultIndex][i].o_vals = new_oval;

							if (!flag) {
								is_pruned[i] = true;
								pruned_binding.push_back(qpr.result_map[sNode_resultIndex][i].s);
							}
						}
					}
				}
				
			}
		}

		/////////////////
		if (pruned_binding.size() == 0) return;

		/////////////////
		// Sort pruned_binding based on their IDs
		sort(pruned_binding.begin(), pruned_binding.end());

		/////////////////
		bool found;
		for (int i = 0; i < qpr.result_map[sNode_resultIndex].size(); i++) {
			found = binary_search(pruned_binding.begin(), pruned_binding.end(),
				qpr.result_map[sNode_resultIndex][i].s);
			if (found) is_pruned[i] = true;
		}

		/////////////////
		// It contains the new values of <sNode> due to pruning of corresponding RDF instances.
		std::vector <sid_t> new_binding;
		new_binding.clear();

		/////////////////
		// <new_result> contains those triple instances (evaluated for <sNode>) that are not pruned.
		std::vector <triple_t> new_result;
		new_result.clear();

		/////////////////
		for (int i = 0; i < is_pruned.size(); i++)
			if (!is_pruned[i]) {
				new_result.push_back(qpr.result_map[sNode_resultIndex][i]);
				new_binding.push_back(qpr.result_map[sNode_resultIndex][i].s);
			}

		/////////////////
		if ((qpr.result_map[sNode_resultIndex].size()) != (new_result.size()))
			qpr.pruned_step2List[sNode_resultIndex] = true;

		/////////////////
		qpr.result_map[sNode_resultIndex] = new_result;

		/////////////////
		// It is used for deduplicating the content of new_binding
		std::set <sid_t> set1;
		set1.clear();

		/////////////////
		for (int i = 0; i < new_binding.size(); i++)
			set1.insert(new_binding[i]);
		new_binding.assign(set1.begin(), set1.end());

		/////////////////
		qpr.binding_map[sNode_resultIndex] = new_binding;
		return;
	}
	/////////////////////////////
	template <typename Archive> void serialize(Archive& ar, const unsigned int version) {
		ar& root;
		ar& nodeList;
		ar& qpr;
		ar& sender_ip;
		ar& sender_port;
	}
};

//////////////////////////////////////////////////////////////////
class queryPlan_generator {

private:

	/////////////////////////////
	queryPlan qp;

	/////////////////////////////
	RDFterm explorationPoint;
	int explorationPoint_index; // The index of exploration point in the list of query graph vertices.
	Database* db;
	Query q;

	/////////////////////////////
	struct patInfo { // This contains the information of a query graph edge (or triple pattern).  
		int pat_nbr, seqNo, dir, towardsJoint, sBound, oBound; // dir = 0: IN, 1:OUT
	};
	std::vector <patInfo> stmtList; // This consruct contains the gradually created query plan

	/////////////////////////////	
	struct subOrObjInfo { // This contains the information of a query graph vertex (or subject/object).
		RDFterm subOrObj;
		std::vector <patInfo> patInfoList; // This contains the list of query graph edges incident to 
										   // a query graph vertex.
		int min_index; // The index of an edge (incident to the subject/object) with minimum sBound or oBound.
		// Note that the list of all edges is stored in the query body.

		int min; // Minimum sBound or oBound of the edges incident to the subject/object; 
		float weight; // This value shows how many triple instances will be pruned if the subject or object
					  // is selected as the exploration point.

		subOrObjInfo() : weight(0) {
			patInfoList.clear();
		}
	};
	std::vector <subOrObjInfo> subOrObjInfoList; // This contains the list of query grapg vertices.

	/////////////////////////////
	// Servers where the query plan is sent 
	std::vector<int> s_list;

public:
	/////////////////////////////////////////////////
	queryPlan_generator() : explorationPoint_index(0) {
		stmtList.clear();
	}

	/////////////////////////////////////////////////
	queryPlan_generator(Query Q, Database* DB) : explorationPoint_index(0), db(DB), q(Q) {

		/////////////////////
		detExplorationPoint();
		detStmtList();
		reArrange();
		detServerList();
		create_tree_representation();
	}

	/////////////////////////////
	void detExplorationPoint() {
		int res, sBound, oBound;
		RDFtermList subList, predList, objList;
		triplePattern p;
		patInfo eInfo;
		subOrObjInfo vInfo;

		/////////////////////////////////////////////////	
		subOrObjInfoList.clear();
		for (int pat_nbr = 0; pat_nbr < q.size(); pat_nbr++) {
			/////////////////////////////////////////////////
			p = q.getPattern(pat_nbr);
			subList = p.getSub();
			predList = p.getPred();
			objList = p.getObj();

			///////////////////////////////////////////////// Catch and report any error condition
			///////////////////////////////////////////////// in the given query patterns.

			/////////////////////////////
			// Report an error when the predicate is not bound.
			if (!predList.isBound()) {
				cerr << "The following triple pattern is invalid: " << std::endl << p.output(db)
					<< std::endl << "Variables cannot be used as the predicate!"
					<< std::endl;
				exit(1);
			}

			/////////////////////////////
			int predList_contains_type = 0;
			for (int pred_nbr = 0; pred_nbr < predList.size(); pred_nbr++)
				if (predList.get(pred_nbr).getID() == 1) predList_contains_type = 1;

			/////////////////////////////
			// Report an error when there is a list of predicates including the <type> predicate.
			if (predList_contains_type && predList.size() > 1) {
				cerr << "The following triple pattern is invalid: " << std::endl << p.output(db)
					<< std::endl << "A list of predicates cannot include the <type> predicate!"
					<< std::endl;
				exit(1);
			}

			/////////////////////////////
			sBound = oBound = 0;
			int sCnt, oCnt;

			/////////////////////////////
			if (predList_contains_type && !objList.isBound()) {
				db->get_p_counter(predList.get(0).getID(), &sCnt, &oCnt);
				sBound = sCnt;
				oBound = oCnt;
			}

			/////////////////////////////
			if (predList_contains_type && objList.isBound())
				for (int obj_nbr = 0; obj_nbr < objList.size(); obj_nbr++) {
					db->get_p_counter(objList.get(obj_nbr).getID(), &sCnt, &oCnt);
					sBound += sCnt;
				}

			if (!predList_contains_type)
				for (int pred_nbr = 0; pred_nbr < predList.size(); pred_nbr++) {
					db->get_p_counter(predList.get(pred_nbr).getID(), &sCnt, &oCnt);
					sBound += sCnt;
					oBound += oCnt;
				}

			/////////////////////////////
			if (!predList_contains_type && subList.isBound()) {
				int s = subList.size();
				if (oBound >= sBound)
					sBound = s;
				else {
					sBound = s;
					oBound = 1;
				}
			}

			if (!predList_contains_type && objList.isBound()) {
				int s = objList.size();
				if (sBound >= oBound)
					oBound = s;
				else {
					oBound = s;
					sBound = 1;
				}
			}

			if (oBound == 0) oBound = 1;
			if (sBound == 0) sBound = 1;

			// Insert each individual (non-bound) subject/object (of the query graph), as well as 
			// the sBound/oBound and direction of their incident triple patterns.

			eInfo.pat_nbr = pat_nbr;
			eInfo.sBound = sBound;
			eInfo.oBound = oBound;

			if (!subList.isBound()) {
				RDFterm sub = subList.get(0);
				eInfo.dir = 1;
				if ((res = find_subOrObj(sub.getName())) == -1) {
					vInfo.subOrObj = sub;
					vInfo.patInfoList.clear();
					vInfo.min = sBound;
					vInfo.min_index = pat_nbr;

					vInfo.patInfoList.push_back(eInfo);
					subOrObjInfoList.push_back(vInfo);
				}
				else {
					subOrObjInfoList[res].patInfoList.push_back(eInfo);
					if (sBound < subOrObjInfoList[res].min) {
						subOrObjInfoList[res].min = sBound;
						subOrObjInfoList[res].min_index = pat_nbr;
					}
				}
			}

			///////////////////////////////////////////////
			if (!objList.isBound()) {
				RDFterm obj = objList.get(0);
				eInfo.dir = 0;
				if ((res = find_subOrObj(obj.getName())) == -1) {
					vInfo.subOrObj = obj;
					vInfo.patInfoList.clear();
					vInfo.min = oBound;
					vInfo.min_index = pat_nbr;

					vInfo.patInfoList.push_back(eInfo);
					subOrObjInfoList.push_back(vInfo);
				}
				else {
					subOrObjInfoList[res].patInfoList.push_back(eInfo);
					if (oBound < subOrObjInfoList[res].min) {
						subOrObjInfoList[res].min = oBound;
						subOrObjInfoList[res].min_index = pat_nbr;
					}
				}
			}
		}

		// Find the weight of each (non-bound) subject/object
		float /* max_t, avg_t, */ min_t, w = 0;
		for (int v_nbr = 0; v_nbr < subOrObjInfoList.size(); v_nbr++) {
			vInfo = subOrObjInfoList[v_nbr];
			subOrObjInfoList[v_nbr].weight = 0;
			for (int e_nbr = 0; e_nbr < vInfo.patInfoList.size(); e_nbr++) {
				sBound = vInfo.patInfoList[e_nbr].sBound;
				oBound = vInfo.patInfoList[e_nbr].oBound;

				// max_t = sBound * oBound; // Maximum number of triple instances of the triple pattern			
				min_t = std::max(sBound, oBound); // Minimum number of triple instances of the triple pattern
				// avg_t = (max_t + min_t) / 2;

				if (vInfo.patInfoList[e_nbr].dir == 1) {
					// subOrObjInfoList[v_nbr].weight += (sBound - vInfo.min) * (avg_t / sBound);
					if (sBound == 0) { cout << "3- Division by zero"; exit(1); }
					//subOrObjInfoList[v_nbr].weight += (sBound - vInfo.min) * (oBound + min_t / sBound) / 2;
					subOrObjInfoList[v_nbr].weight += (sBound - vInfo.min) * (oBound / sBound);
				}
				else {
					// subOrObjInfoList[v_nbr].weight += (oBound - vInfo.min) * (avg_t / oBound);	
					if (oBound == 0) { cout << "4- Division by zero"; exit(1); }
					//subOrObjInfoList[v_nbr].weight += (oBound - vInfo.min) * (sBound + min_t / oBound) / 2;
					subOrObjInfoList[v_nbr].weight += (oBound - vInfo.min) * (sBound / oBound);

				}
			}
			if (subOrObjInfoList[v_nbr].weight > w) {
				w = subOrObjInfoList[v_nbr].weight;
				explorationPoint_index = v_nbr;
			}
		}

		explorationPoint = subOrObjInfoList[explorationPoint_index].subOrObj;
	}

	/////////////////////////////
	void create_tree_representation() {

		/////////////////////////////////////////
		triplePattern p;
		RDFtermList subList, objList, predList;
		int sFound, oFound;

		for (int s_nbr = 0; s_nbr < stmtList.size(); s_nbr++) {
			/////////////////////////////////////////
			p = q.getPattern(stmtList[s_nbr].pat_nbr);
			subList = p.getSub();
			objList = p.getObj();
			sFound = oFound = 0;

			/////////////////////////////////////////
			for (int i = 0; i < qp.qpr.subListOrObjList_Set.size(); i++) {
				if (qp.qpr.subListOrObjList_Set[i] == subList) sFound = 1;
				if (qp.qpr.subListOrObjList_Set[i] == objList) oFound = 1;
			}

			if (stmtList[s_nbr].dir == OUT) {
				if (!sFound) qp.qpr.subListOrObjList_Set.push_back(subList);
				if (!oFound) qp.qpr.subListOrObjList_Set.push_back(objList);
			}
			else {
				if (!oFound) qp.qpr.subListOrObjList_Set.push_back(objList);
				if (!sFound) qp.qpr.subListOrObjList_Set.push_back(subList);
			}
		}

		////////////////////
		qp.nodeList.clear();
		qp.qpr.result_map.clear();

		vector<sid_t> subOrObjID_list;
		for (int i = 0; i < qp.qpr.subListOrObjList_Set.size(); i++) {
			subOrObjID_list.clear();
			subOrObjID_list.resize(0);

			/////////////////////
			if (qp.qpr.subListOrObjList_Set[i].isBound())
				for (int j = 0; j < qp.qpr.subListOrObjList_Set[i].size(); j++)
					subOrObjID_list.push_back(qp.qpr.subListOrObjList_Set[i].get(j).getID());

			/////////////////////
			QueryNode* node = new QueryNode(1, subOrObjID_list);

			/////////////////////
			if (!qp.qpr.subListOrObjList_Set[i].isBound())
				node->name = qp.qpr.subListOrObjList_Set[i].get(0).getName();
			else node->name = "";

			/////////////////////
			qp.nodeList.push_back(node);
		}

		////////////////////
		qp.qpr.result_map.resize(qp.nodeList.size());
		qp.qpr.binding_map.resize(qp.nodeList.size());
		qp.qpr.pruned_step1List.resize(qp.nodeList.size());
		qp.qpr.pruned_step2List.resize(qp.nodeList.size());
		qp.root = qp.nodeList[0];

		vector<sid_t> pID_list, sID_list, oID_list;
		for (int s_nbr = 0; s_nbr < stmtList.size(); s_nbr++) {
			/////////////////////////////////////////
			p = q.getPattern(stmtList[s_nbr].pat_nbr);
			subList = p.getSub();
			objList = p.getObj();
			predList = p.getPred();

			/////////////////////////////////////////
			pID_list.clear();

			/////////////////////////////////////////
			for (int pred_nbr = 0; pred_nbr < predList.size(); pred_nbr++)
				pID_list.push_back(predList.get(pred_nbr).getID());

			QueryEdge* edge = new QueryEdge(1, (stmtList[s_nbr].dir ? OUT : IN), stmtList[s_nbr].towardsJoint,
				pID_list);
			////////////////////////////////
			if (stmtList[s_nbr].dir == OUT) {
				if (objList.isBound()) edge->exec_flag = 1;
				else if (stmtList[s_nbr].seqNo == 0) edge->exec_flag = 3;
				else edge->exec_flag = 2;
			}
			else {
				if (subList.isBound()) edge->exec_flag = 1;
				else if (stmtList[s_nbr].seqNo == 0) edge->exec_flag = 3;
				else edge->exec_flag = 2;
			}

			/////////////////////////////////////////
			if (stmtList[s_nbr].dir == OUT) {
				for (int i = 0; i < qp.qpr.subListOrObjList_Set.size(); i++)
					if (qp.qpr.subListOrObjList_Set[i] == subList) {
						qp.nodeList[i]->edges.push_back(edge);

						for (int j = 0; j < qp.qpr.subListOrObjList_Set.size(); j++)
							if (qp.qpr.subListOrObjList_Set[j] == objList) {
								edge->node = qp.nodeList[j];
								break;
							}
						break;
					}
			}
			else {
				for (int i = 0; i < qp.qpr.subListOrObjList_Set.size(); i++)
					if (qp.qpr.subListOrObjList_Set[i] == objList) {
						qp.nodeList[i]->edges.push_back(edge);
						for (int j = 0; j < qp.qpr.subListOrObjList_Set.size(); j++)
							if (qp.qpr.subListOrObjList_Set[j] == subList) {
								edge->node = qp.nodeList[j];
								break;
							}
						break;
					}
			}
		}
	}

	/////////////////////////////////////////////////
	int find_subOrObj(string name) {
		for (int i = 0; i < subOrObjInfoList.size(); i++)
			if (subOrObjInfoList[i].subOrObj.getName() == name) return i;
		return -1;
	}

	/////////////////////////////////////////////////
	//s_list 存在问题只有一个0
	void detServerList() {

		/////////////////////////////////////////////////
		RDFtermList predList, objList;
		std::vector<int> s_list1, s_list2;
		s_list2.clear(), s_list.clear();

		/////////////////////////////////////////////////	
		triplePattern p = q.getPattern(stmtList[0].pat_nbr);
		cout << "pre = " << p.getPred().get(0).getName() << ";id = " << p.getPred().get(0).getID() << endl;

		if (p.getPred().get(0).getID() == 1) { // type predicate
			objList = p.getObj();
			for (int obj_nbr = 0; obj_nbr < objList.size(); obj_nbr++) {
				s_list1 = db->get_p_loc(objList.get(obj_nbr).getID(), (stmtList[0].dir ? OUT : IN));
				for (int i = 0; i < s_list1.size(); i++) {
					s_list2.push_back(s_list1[i]);
				}
			}
		}

		else {
			predList = q.getPattern(stmtList[0].pat_nbr).getPred();
			for (int pred_nbr = 0; pred_nbr < predList.size(); pred_nbr++) {
				s_list1 = db->get_p_loc(predList.get(pred_nbr).getID(), (stmtList[0].dir ? OUT : IN));
				for (int i = 0; i < s_list1.size(); i++) {
					s_list2.push_back(s_list1[i]);
				}
			}
		}
		set<int> s;
		unsigned size = s_list2.size();
		for (unsigned i = 0; i < size; ++i) s.insert(s_list2[i]);
		s_list.assign(s.begin(), s.end());
		cout << "s_list = [";
		for (int i = 0; i < s_list.size(); i++) {
			cout << s_list[i] << ",";
		}
		cout << "]" << endl;
	}

	/////////////////////////////////////////////////
	void detStmtList() {
		patInfo s;

		stmtList.clear();

		std::vector <int> patNumberList, savedPatNumberList;
		patNumberList.clear();

		std::vector <RDFterm> explorationPointSet, _explorationPointSet;
		explorationPointSet.clear();

		for (int pat_nbr = 0; pat_nbr < q.size(); pat_nbr++)
			patNumberList.push_back(pat_nbr);

		explorationPointSet.push_back(explorationPoint);
		int seqNo = 0;

		std::vector <RDFterm> srcPointSet, tgtPointSet, jointPointSet;
		srcPointSet.clear();
		tgtPointSet.clear();
		jointPointSet.clear();

		triplePattern p;
		RDFterm sub, obj;

		int p_nbr = subOrObjInfoList[explorationPoint_index].min_index;

		while (!patNumberList.empty()) {
			_explorationPointSet.clear();
			savedPatNumberList.clear();
			seqNo++;

			for (int i = 0; i < patNumberList.size(); i++) {

				p = q.getPattern(patNumberList[i]);
				s.pat_nbr = patNumberList[i];

				if (!p.getSub().isBound() && !p.getObj().isBound()) {

					sub = p.getSub().get(0);
					obj = p.getObj().get(0);

					std::vector <RDFterm>::iterator it = std::find(explorationPointSet.begin(),
						explorationPointSet.end(), sub);
					if (it != explorationPointSet.end()) {
						s.dir = 1;
						if (patNumberList[i] == p_nbr) s.seqNo = 0;
						else s.seqNo = seqNo;
						patNumberList[i] = -1;
						_explorationPointSet.push_back(obj);
						stmtList.push_back(s);
						srcPointSet.push_back(sub);
						tgtPointSet.push_back(obj);
					}
					else {
						std::vector <RDFterm>::iterator it = std::find(explorationPointSet.begin(),
							explorationPointSet.end(), obj);
						if (it != explorationPointSet.end()) {
							s.dir = 0;
							if (patNumberList[i] == p_nbr) s.seqNo = 0;
							else s.seqNo = seqNo;
							patNumberList[i] = -1;
							_explorationPointSet.push_back(sub);

							stmtList.push_back(s);
							srcPointSet.push_back(obj);
							tgtPointSet.push_back(sub);
						}
					}
				}

				else if (!p.getSub().isBound() && p.getObj().isBound()) {

					sub = p.getSub().get(0);

					std::vector <RDFterm>::iterator it = std::find(explorationPointSet.begin(),
						explorationPointSet.end(), sub);
					if (it != explorationPointSet.end()) {
						s.dir = 1;
						if (patNumberList[i] == p_nbr) s.seqNo = 0;
						else s.seqNo = seqNo;
						patNumberList[i] = -1;
						stmtList.push_back(s);
						srcPointSet.push_back(sub);
					}
				}

				else if (p.getSub().isBound() && !p.getObj().isBound()) {

					obj = p.getObj().get(0);

					std::vector <RDFterm>::iterator it = std::find(explorationPointSet.begin(),
						explorationPointSet.end(), obj);
					if (it != explorationPointSet.end()) {
						s.dir = 0;
						if (patNumberList[i] == p_nbr) s.seqNo = 0;
						else s.seqNo = seqNo;
						patNumberList[i] = -1;
						stmtList.push_back(s);
						srcPointSet.push_back(obj);
					}
				}
			}

			for (int i = 0; i < patNumberList.size(); i++)
				if (patNumberList[i] != -1) savedPatNumberList.push_back(patNumberList[i]);
			patNumberList = savedPatNumberList;
			explorationPointSet = _explorationPointSet;
		}

		for (int i = 0; i < srcPointSet.size(); i++)
			for (int j = 0; j < tgtPointSet.size(); j++)
				if (srcPointSet[i].getName() == tgtPointSet[j].getName()) {
					jointPointSet.push_back(srcPointSet[i]); break;
				}

		for (int i = 0; i < stmtList.size(); i++) {
			stmtList[i].towardsJoint = 0;
			for (int j = 0; j < jointPointSet.size(); j++) {
				if ((stmtList[i].dir == 1 &&
					q.getPattern(stmtList[i].pat_nbr).getObj().get(0).getName() == jointPointSet[j].getName()) ||
					(stmtList[i].dir == 0 &&
						q.getPattern(stmtList[i].pat_nbr).getSub().get(0).getName() == jointPointSet[j].getName())) {
					stmtList[i].towardsJoint = 1;
					break;
				}
			}
			//add print stmtList info
			//cout << "edge : seqNo = " << stmtList[i].seqNo << ";pat_nbr = " << stmtList[i].pat_nbr << ";dir = " << stmtList[i].dir
			//	<< ";towardsJoint = " << stmtList[i].towardsJoint << endl;

		}



	}

	/////////////////////////////////////////////////
	vector<int> getServers() {
		return (s_list);
	}

	// sort the patterns of query plan based on their sequence number;
	/////////////////////////////////////////////////
	void reArrange() {
		patInfo p;
		int size = stmtList.size();
		for (int i = 1; i < size; i++)
			for (int j = 0; j < size - i; j++)
				if (stmtList[j].seqNo > stmtList[j + 1].seqNo) {
					p = stmtList[j];
					stmtList[j] = stmtList[j + 1];
					stmtList[j + 1] = p;
				}
	}

	/////////////////////////////////////////////////
	void output() {

		///////////////////////
		std::cout << "--------------------------------------------" << std::endl;
		std::cout << " Metadata information: " << std::endl;
		std::cout << "--------------------------------------------" << std::endl;
		for (int i = 0; i < subOrObjInfoList.size(); i++) {
			std::cout << " Node: " << subOrObjInfoList[i].subOrObj.output(db)
				<< ", Weight: " << subOrObjInfoList[i].weight << std::endl;
		}

		std::cout << "---------------" << std::endl;
		std::cout << " Exploration point is: " << explorationPoint.output(db) << std::endl << std::endl;


		std::cout << "---------------" << std::endl;
		std::cout << "List of servers: " << std::endl;
		std::cout << "---------------" << std::endl;
		for (int i = 0; i < s_list.size(); i++)
			std::cout << "S#" << s_list[i] << " ";
		std::cout << std::endl;

		std::cout << "--------------------------------------------" << std::endl;
		std::cout << " Tree nodes: " << std::endl;
		std::cout << "--------------------------------------------" << std::endl;
		for (int i = 0; i < qp.qpr.subListOrObjList_Set.size(); i++)
			cout << std::endl << "Node# " << i << ":" << std::endl
			<< qp.qpr.subListOrObjList_Set[i].output(db) << std::endl << "---------------" << std::endl;

		std::cout << "---------------" << std::endl;
		std::cout << "Query plan: " << std::endl;
		std::cout << "---------------" << std::endl;
		qp.output();
	}

	/////////////////////////////////////////////////
	queryPlan get_queryPlan() {
		return qp;
	}
};

//////////////////////////////////////////////////////////////////
string wrap_queryPlan(queryPlan qp) {
	stringstream ss;
	boost::archive::binary_oarchive oa(ss);
	oa << qp;

	string str = ss.str();
	return str;
}

//////////////////////////////////////////////////////////////////
queryPlan unwrap_queryPlan(string str) {
	stringstream ss;
	ss << str;

	boost::archive::binary_iarchive ia(ss);
	queryPlan qp;
	ia >> qp;
	return qp;
}

//////////////////////////////////////////////////////////////////
string wrap_queryPlan_result(queryPlan_result qpr) {
	stringstream ss;
	boost::archive::binary_oarchive oa(ss);
	oa << qpr;

	string str = ss.str();
	return str;
}

//////////////////////////////////////////////////////////////////
queryPlan_result unwrap_queryPlan_result(string str) {
	stringstream ss;
	ss << str;

	boost::archive::binary_iarchive ia(ss);
	queryPlan_result qpr;
	ia >> qpr;
	return qpr;
}

