//////////////////////////////////////////////////////////////////
#ifndef HELIOS_PART_RDF_H
#define HELIOS_PART_RDF_H

#ifndef _selectSize_
#define _selectSize_  5  
#endif

#ifndef _bodySize_
#define _bodySize_  10 
#endif

#ifndef _LUBM_query_nbr_
#define _LUBM_query_nbr_  14 
#endif

#ifndef _WATDiv_query_nbr_
#define _WATDiv_query_nbr_  20 
#endif

#include "type.h"
#include "Database.hpp"

//////////////////////////////////////////////////////////////////
using namespace std;

//////////////////////////////////////////////////////////////////
enum class Type {
	URI,
	Literal,
	Variable
};

//////////////////////////////////////////////////////////////////
class RDFterm { // This class represents an RDFTerm whether a vertex (i.e., s subject/object) or 
				// an edge (i.e., s predicate) in an RDF graph).

private:
	/////////////////////////////
	Type type;
	string name;
	sid_t id;

public:
	/////////////////////////////
	RDFterm(Type t, string n) : id(-1), type(t), name(n) {}

	/////////////////////////////
	RDFterm() : id(-1) {}

	/////////////////////////////
	Type getType() {
		return type;
	}

	/////////////////////////////
	string getName() {
		return name;
	}

	/////////////////////////////
	sid_t getID() {
		return id;
	}

	/////////////////////////////
	void setType(Type t) {
		type = t;
	}

	/////////////////////////////
	void setName(string n) {
		name = n;
	}

	/////////////////////////////
	void setID(sid_t ID) {
		id = ID;
	}

	/////////////////////////////
	void compress(Database* db) {
		if (type != Type::Variable)
			id = db->lookup_global_dict(name);
	}

	/////////////////////////////
	std::string output(Database* db) {
		string n;

		if (type == Type::Variable)
			return ("?" + name);

		if (id != -1) n = db->lookup_global_dict(id);
		else n = name;

		return n;
	}

	/////////////////////////////
	template <typename Archive> void serialize(Archive& ar, const unsigned int version) {
		ar& type;
		ar& name;
	}

	bool operator==(const RDFterm& t) const {
		return ((t.name == name) && (t.type == type));
	}
};

//////////////////////////////////////////////////////////////////
class RDFtermList {

private:
	/////////////////////////////
	std::vector <RDFterm> RDFterms;

public:
	/////////////////////////////
	RDFtermList() {
		RDFterms.clear();
	}

	/////////////////////////////
	void add(RDFterm t) {
		if (RDFterms.size() >= 1)
			if (t.getType() == Type::Variable) {
				cerr << "Invalid triple pattern!" << std::endl;
				exit(1);
			}
		RDFterms.push_back(t);
	}

	/////////////////////////////
	RDFterm get(int index) {
		if (index < 0 || index >= RDFterms.size()) {
			cerr << "Out of range!" << std::endl;
			exit(1);
		}
		return RDFterms[index];
	}

	/////////////////////////////
	std::vector <sid_t> getID_list() {
		std::vector <sid_t> list;
		list.empty();

		for (int i = 0; i < RDFterms.size(); i++)
			list.push_back(RDFterms[i].getID());
		return list;
	}

	/////////////////////////////
	int size() {
		return (RDFterms.size());
	}

	/////////////////////////////
	int isBound() {
		if (RDFterms.size() == 0) {
			cerr << "Invalid triple pattern!" << std::endl;
			exit(1);
		}
		return (RDFterms[0].getType() != Type::Variable);
	}

	/////////////////////////////
	void compress(Database* db) {
		for (int i = 0; i < RDFterms.size(); i++)
			RDFterms[i].compress(db);
	}

	/////////////////////////////
	std::string output(Database* db) {
		if (isBound()) {
			std::string str = "{";
			for (int i = 0; i < RDFterms.size() - 1; i++)
				str += (RDFterms[i].output(db) + ", ");
			str += RDFterms[RDFterms.size() - 1].getName();
			str += "}";
			return (str);
		}
		return (RDFterms[0].output(db));
	}

	/////////////////////////////
	template <typename Archive> void serialize(Archive& ar, const unsigned int version) {
		ar& RDFterms;
	}

	/////////////////////////////
	bool operator==(const RDFtermList& l) const {
		return (l.RDFterms == RDFterms);
	}
};

//////////////////////////////////////////////////////////////////
class triplePattern { // This class represents an RDF triple pattern.

private:
	/////////////////////////////
	RDFtermList subList, // a Set of subjects
		predList, // a set of predicates
		objList; // a set of objects

public:
	//////////////////////////////
	triplePattern() {}

	/////////////////////////////
	triplePattern(RDFtermList sList, RDFtermList pList, RDFtermList oList) {
		subList = sList;
		predList = pList;
		objList = oList;
	}

	/////////////////////////////
	RDFtermList getPred() {
		return(predList);
	}

	/////////////////////////////
	RDFtermList getObj() {
		return(objList);
	}

	/////////////////////////////
	RDFtermList getSub() {
		return(subList);
	}


	/////////////////////////////
	void compress(Database* db) {
		subList.compress(db);
		predList.compress(db);
		objList.compress(db);
	}

	/////////////////////////////
	string output(Database* db) {
		return (subList.output(db) + ' ' + predList.output(db) + ' ' + objList.output(db));
	}

	/////////////////////////////
	template <typename Archive> void serialize(Archive& ar, const unsigned int version) {
		ar& subList;
		ar& predList;
		ar& objList;
	}

	bool operator==(const triplePattern& p) const {
		return ((p.subList == subList) && (p.objList == objList) && (p.predList == predList));
	}
};

//////////////////////////////////////////////////////////////////
class Query { // This class represents a SPARQL query.

	/////////////////////////////
private:
	typedef vector <RDFterm> head_part;
	typedef vector <triplePattern> body_part;
	body_part body;
	string sender_ip;
	string sender_port;

	/////////////////////////////
	// <query_result_map> stores the triple instances after evaluating the query.
	// Each row of <query_result_map> corresponds to a variable in the head of query.

/////////////////////////////
public:
	/////////////////////////////
	int query_pattern;   //added for query hard  of workload
	int queryNo;
	uint64_t time_sent;
	uint64_t time_elapsed_for_queryPlans;
	uint64_t time_elapsed_for_evaluations;
	uint64_t time_elapsed_for_prune_step1;
	uint64_t time_elapsed_for_prune_step2;
	uint64_t time_elapsed_for_evaluation;
	vector <vector<sid_t>> binding_map;
	vector <vector<triple_t>> result_map;
	head_part head;
	//add edge_counter;
	sid_t edge_cut_counter;
	sid_t edge_cut_count_spend_time;

	Query() {
		head.clear();
		body.clear();
		binding_map.clear();
		result_map.clear();
		time_sent = get_usec();
		edge_cut_counter = 0;
		edge_cut_count_spend_time = 0;
	};

	void printQueryInfo() {
		cout << "Query :" << endl;
		cout << "queryNo = " << queryNo << "\tquery_pattern = " << query_pattern;
		cout << "head = ";
		for (RDFterm term : head) {
			cout << term.getName() << ",";
		}
		cout << endl;
		cout << "body = ";
		for (triplePattern triple : body) {
			cout << "[" << triple.getSub().get(0).getName() << "," << triple.getPred().get(0).getName() << "," << triple.getObj().get(0).getName() << "],";
		}
		cout << endl;
	}

	/////////////////////////////
	template <typename Archive> void serialize(Archive& ar, const unsigned int version) {
		ar& head;
		ar& body;
		ar& sender_ip;
		ar& sender_port;
		ar& query_pattern;
		ar& queryNo;
		ar& time_sent;
		ar& binding_map;
		ar& result_map;
		ar& time_elapsed_for_queryPlans;
		ar& time_elapsed_for_evaluations;
		ar& time_elapsed_for_prune_step1;
		ar& time_elapsed_for_prune_step2;
		ar& time_elapsed_for_evaluation;
		ar& edge_cut_counter;
		ar& edge_cut_count_spend_time;
	}

	/////////////////////////////
	void addToHead(RDFterm t) {
		if (t.getType() != Type::Variable) {
			cerr << "A variable expected!" << std::endl;
			exit(1);
		}
		head.push_back(t);
	}

	/////////////////////////////
	void addToBody(triplePattern p) {
		body.push_back(p);
	}

	//////////////////////////////
	int re_set_body(triplePattern p_old, triplePattern p_new)
	{
		auto it = (find(body.begin(), body.end(), p_old));//tripePattern已�~O�~G~M载�~F=
		if (it == body.end())
		{
			std::cout << "error:p didn't found" << std::endl;
			return -1;
		}
		std::replace(body.begin(), body.end(), p_old, p_new);
		return 1;
	}

	/////////////////////////////
	triplePattern getPattern(int pat_nbr) {
		if (pat_nbr < 0 || pat_nbr >= body.size()) {
			cerr << "Out of range!" << std::endl;
			exit(1);
		}
		return body[pat_nbr];
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
	string getIP() {
		return (sender_ip);
	}

	/////////////////////////////
	string getPort() {
		return (sender_port);
	}

	/////////////////////////////
	void setQueryNo(int qNo) {
		queryNo = qNo;
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
	int size() {
		return (body.size());
	}

	/////////////////////////////
	void compress(Database* db) {
		for (int pat_nbr = 0; pat_nbr < body.size(); pat_nbr++) {
			body[pat_nbr].compress(db);
		}
	}

	/////////////////////////////
	string output(Database* db) {
		string str("Select ");

		for (int i = 0; i < head.size(); i++)
			if (i != head.size() - 1)
				str += (head[i].output(db) + ", ");
			else str += (head[i].output(db) + '\n');

		str += "where {";
		str += '\n';

		for (int i = 0; i < body.size(); i++)
			str += (body[i].output(db) + '\n' + '\n');

		str += " }\n";
		return str;
	}


};


//////////////////////////////////////////////////////////////////
string wrap_query(Query q) {
	stringstream ss;
	boost::archive::binary_oarchive oa(ss);
	oa << q;

	string str = ss.str();
	return str;
}

//////////////////////////////////////////////////////////////////
Query unwrap_query(string str) {
	stringstream ss;
	ss << str;

	boost::archive::binary_iarchive ia(ss);
	Query q;
	ia >> q;
	return q;
};


//定义用户端发送数据：qeuryNo，IP,port 和worklaodno
class ClientData {
private:
	int queryNo;
	int workloadNo;
	string send_ip;
	string send_port;
	Query q;
	//添加get 和set 
public:
	ClientData() {}
	ClientData(int _queryNo, int _workloadNo, string ip, string port) {
		queryNo = _queryNo;
		workloadNo = _workloadNo;
		send_ip = ip;
		send_port = port;
	}

	ClientData(int _queryNo, int _workloadNo, string ip, string port,Query query) {
		queryNo = _queryNo;
		workloadNo = _workloadNo;
		send_ip = ip;
		send_port = port;
		q = query;
	}
	int getQueryNo() {
		return queryNo;
	}
	void setQueryNo(int _queryNo) {
		queryNo = _queryNo;
	}
	int getWorkloadNo() {
		return workloadNo;
	}
	void setWorkloadNo(int _workloadNo) {
		workloadNo = _workloadNo;
	}
	string getIP() {
		return send_ip;
	}
	void setIP(string _ip) {
		send_ip = _ip;
	}
	string getPort() {
		return send_port;
	}
	void setPort(string _port) {
		send_port = _port;
	}

	Query getQuery() {
		return q;
	}
	void setQuery(Query query) {
		q = query;
	}
	//添加序列化
	template <typename Archive> void serialize(Archive& ar, const unsigned int version) {
		ar& queryNo;
		ar& workloadNo;
		ar& send_ip;
		ar& send_port;
		ar& q;
	}
};

string wrap_clientData(ClientData clientData) {
	stringstream ss;
	boost::archive::binary_oarchive oa(ss);
	oa << clientData;

	string str = ss.str();
	return str;
}

//////////////////////////////////////////////////////////////////
ClientData unwrap_clientData(string str) {
	stringstream ss;
	ss << str;

	boost::archive::binary_iarchive ia(ss);
	ClientData clientData;
	ia >> clientData;
	return clientData;
};

class WORKLOAD_serialize
{
public:
	WORKLOAD_serialize() {}
	WORKLOAD_serialize(std::vector<std::string> vec_se) :
		vec_serialize(vec_se) {}
	template <typename Archive> void serialize(Archive& ar, const unsigned int version) {
		ar& vec_serialize;
	}
public:
	std::vector<std::string> get_workload()
	{
		return vec_serialize;
	}
private:
	std::vector<std::string> vec_serialize;
};

//add in 2020/08/11 make baseQueryBank
class QueryBank {
private:
	/////////////////////////////
	vector<Query> qList;
	string dataSource;
public:
	QueryBank(){
		dataSource = "";
	}
	QueryBank(string type) {
		dataSource = type;
		bool flag = false;
		if (type.compare("lubm")==0) {
			initLUBM();
			flag = true;
		}
		else if (type.compare("watdiv")==0) {
			initWATDiv();
			flag = true;
		}
		else if (type.compare("yago2")==0) {
			initYAGO2();
				flag = true;
		}
		if (!flag) {
			cout << "error : init QueryBank fail" << endl;
		}
	}

	void initLUBM() {
		for (int i = 0; i < _LUBM_query_nbr_; i++) {
			qList.push_back(Query());
		}
		//qList.resize(_LUBM_query_nbr_);
		RDFterm t1(Type::Variable, "X"),
			t2(Type::Variable, "Y"),
			t3(Type::Variable, "Z"),
			t4(Type::Variable, "Y1"),
			t5(Type::Variable, "Y2"),
			t6(Type::Variable, "Y3"),

			t7(Type::URI, "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"),


			t8(Type::URI, "<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#GraduateStudent>"),
			t9(Type::URI, "<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#takesCourse>"),
			t10(Type::URI, "<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#University>"),
			t11(Type::URI, "<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Department>"),
			t12(Type::URI, "<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#memberOf>"),
			t13(Type::URI, "<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#subOrganizationOf>"),
			t14(Type::URI, "<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#undergraduateDegreeFrom>"),
			t15(Type::URI, "<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Publication>"),
			t16(Type::URI, "<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor>"),

			t17(Type::URI, "<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#FullProfessor>"),
			t17_1(Type::URI, "<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#AssociateProfessor>"),
			t17_2(Type::URI, "<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#AssistantProfessor>"),

			t18(Type::URI, "<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#worksFor>"),
			t19(Type::URI, "<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name>"),
			t20(Type::URI, "<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#telephone>"),

			t23(Type::URI, "<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Course>"),
			t24(Type::URI, "<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#takesCourse>"),
			t25(Type::URI, "<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#emailAddress>"),

			t26_1(Type::URI, "<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Lecturer>"),

			t27(Type::URI, "<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#advisor>"),
			t28(Type::URI, "<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#teacherOf>"),
			t29(Type::URI, "<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#ResearchGroup>"),

			t30(Type::URI, "<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#headOf>"),

			t31_1(Type::URI, "<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#mastersDegreeFrom>"),
			t31_2(Type::URI, "<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#doctoralDegreeFrom>"),

			t32(Type::URI, "<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#UndergraduateStudent>"),

			t33(Type::URI, "<http://www.Department0.University0.edu/GraduateCourse0>"),
			t34(Type::URI, "<http://www.Department0.University0.edu/AssistantProfessor0>"),
			t35(Type::URI, "<http://www.Department0.University0.edu>"),
			t36(Type::URI, "<http://www.Department0.University0.edu/AssociateProfessor0>"),
			t37(Type::URI, "<http://www.University0.edu>");

		RDFtermList l1;
		l1.add(t1);

		RDFtermList l2;
		l2.add(t8);

		RDFtermList l3;
		l3.add(t33);

		RDFtermList l4;
		l4.add(t2);

		RDFtermList l5;
		l5.add(t10);

		RDFtermList l6;
		l6.add(t3);

		RDFtermList l7;
		l7.add(t11);

		RDFtermList l8;
		l8.add(t15);

		RDFtermList l9;
		l9.add(t34);

		RDFtermList l10;
		l10.add(t17);
		l10.add(t17_1);
		l10.add(t17_2);

		RDFtermList l11;
		l11.add(t35);

		RDFtermList l12;
		l12.add(t4);

		RDFtermList l13;
		l13.add(t5);

		RDFtermList l14;
		l14.add(t6);

		RDFtermList l15;
		l15.add(t17);
		l15.add(t17_1);
		l15.add(t17_2);
		l15.add(t8);
		l15.add(t32);

		RDFtermList l16;
		l16.add(t8);
		l16.add(t32);

		RDFtermList l17;
		l17.add(t37);

		RDFtermList l18;
		l18.add(t23);

		RDFtermList l19;
		l19.add(t26_1);
		l19.add(t17);
		l19.add(t17_1);
		l19.add(t17_2);

		RDFtermList l20;
		l20.add(t29);

		RDFtermList l21;
		l21.add(t30);

		RDFtermList l22;
		l22.add(t32);

		RDFtermList l23;
		l23.add(t36);

		RDFtermList l24;
		l24.add(t14);
		l24.add(t31_1);
		l24.add(t31_2);

		RDFtermList l25;
		l25.add(t7);

		RDFtermList l26;
		l26.add(t9);

		RDFtermList l27;
		l27.add(t12);

		RDFtermList l28;
		l28.add(t13);

		RDFtermList l29;
		l29.add(t14);

		RDFtermList l30;
		l30.add(t18);

		RDFtermList l31;
		l31.add(t19);

		RDFtermList l32;
		l32.add(t25);

		RDFtermList l33;
		l33.add(t24);

		RDFtermList l34;
		l34.add(t28);

		RDFtermList l35;
		l35.add(t16);

		RDFtermList l36;
		l36.add(t20);

		RDFtermList l37;
		l37.add(t27);

		triplePattern p1(l1, l25, l2), //?X rdf:type ub:GraduateStudent 

			p2(l1, l26, l3), //?X ub:takesCourse http://www.Department0.University0.edu/GraduateCoursv0
			//Q2
			p3(l4, l25, l5),
			p4(l6, l25, l7),
			p5(l1, l27, l6),
			p6(l6, l28, l4),
			p7(l1, l29, l4),
			//Q3
			p8(l1, l25, l8), //?X rdf:type ub:Publication
			p9(l1, l35, l9), //? X ub : publicationAuthor http ://www.Department0.University0.edu/AssistantProfessor0

			//Q4
			p10(l1, l25, l10), //?X rdf:type ub:Professor 
			p11(l1, l30, l11),  //?X ub:worksFor <http://www.Department0.University0.edu> 
			p12(l1, l31, l12), //?X ub:name ?Y1 
			p13(l1, l32, l13), // ?X ub:emailAddress ?Y2 
			p14(l1, l36, l14), //?X ub:telephone ?Y3

			//Q5
			p15(l1, l25, l15),  // ?X rdf:type ub:Person 
			p16(l1, l27, l11),  // ?X ub:memberOf http://www.Department0.University0.edu

			//Q6
			p17(l1, l25, l16), //?X rdf:type ub:graduateStudent  UndergraduateStudent

			//Q7
			p18(l4, l25, l18),
			p19(l1, l33, l4),
			p20(l23, l34, l4),

			//Q8
			p21(l1, l27, l4), //?X ub:memberOf ?Y
			p22(l4, l28, l17),
			p23(l1, l32, l6), //?X ub:emailAddress ?Z

			//Q9
			p24(l4, l25, l19),
			p25(l6, l25, l18),
			p26(l1, l37, l4),
			p27(l4, l34, l6),
			p28(l1, l33, l6),

			//Q10
			p29(l1, l33, l3),

			//Q11
			p30(l1, l25, l20),
			p31(l1, l28, l17),

			p32(l1, l21, l6),
			p35(l6, l25, l7),
			p38(l1, l30, l4),
			p33(l4, l25, l7),
			p34(l4, l28, l17),

			//Q13
			p36(l1, l24, l11),

			//Q14
			p37(l1, l25, l22);

		//-------------------------------
		qList[0].addToHead(t1);
		qList[0].addToBody(p1);
		qList[0].addToBody(p2);

		//-------------------------------
		qList[1].addToHead(t1);
		qList[1].addToHead(t2);
		qList[1].addToHead(t3);
		qList[1].addToBody(p1);
		qList[1].addToBody(p3);
		qList[1].addToBody(p4);
		qList[1].addToBody(p5);
		qList[1].addToBody(p6);
		qList[1].addToBody(p7);

		//-------------------------------
		qList[2].addToHead(t1);
		qList[2].addToBody(p8);
		qList[2].addToBody(p9);

		//-------------------------------
		qList[3].addToHead(t1);
		qList[3].addToHead(t4);
		qList[3].addToHead(t5);
		qList[3].addToHead(t6);
		qList[3].addToBody(p10);
		qList[3].addToBody(p11);
		qList[3].addToBody(p12);
		qList[3].addToBody(p13);
		qList[3].addToBody(p14);

		//-------------------------------
		qList[4].addToHead(t1);
		qList[4].addToBody(p15);
		qList[4].addToBody(p16);

		//-------------------------------//
		qList[5].addToHead(t1);
		qList[5].addToBody(p17);

		//-------------------------------//
		qList[6].addToHead(t1);
		qList[6].addToHead(t2);
		qList[6].addToBody(p17);
		qList[6].addToBody(p18);
		qList[6].addToBody(p19);
		qList[6].addToBody(p20);

		//-------------------------------//
		qList[7].addToHead(t1);
		qList[7].addToHead(t2);
		qList[7].addToHead(t3);
		qList[7].addToBody(p17);
		qList[7].addToBody(p33);
		qList[7].addToBody(p21);
		qList[7].addToBody(p22);
		qList[7].addToBody(p23);

		//-------------------------------//
		qList[8].addToHead(t1);
		qList[8].addToHead(t2);
		qList[8].addToHead(t3);
		qList[8].addToBody(p17);
		qList[8].addToBody(p24);
		qList[8].addToBody(p25);
		qList[8].addToBody(p26);
		qList[8].addToBody(p27);
		qList[8].addToBody(p28);

		//-------------------------------//
		qList[9].addToHead(t1);
		qList[9].addToBody(p17);
		qList[9].addToBody(p29);

		//-------------------------------//
		qList[10].addToHead(t1);
		qList[10].addToBody(p30);
		qList[10].addToBody(p31);

		//-------------------------------//
		qList[11].addToHead(t1);
		qList[11].addToHead(t2);
		qList[11].addToBody(p32);
		qList[11].addToBody(p33);
		qList[11].addToBody(p34);
		qList[11].addToBody(p35);
		qList[11].addToBody(p38);

		//-------------------------------//
		qList[12].addToHead(t1);
		qList[12].addToBody(p15);
		qList[12].addToBody(p36);

		//-------------------------------//
		qList[13].addToHead(t1);
		qList[13].addToBody(p37);
	}

	void initWATDiv() {
		//qList.reserve(_WATDiv_query_nbr_);
		for (int i = 0; i < _WATDiv_query_nbr_; i++) {
			qList.push_back(Query());
		}
		RDFterm t1(Type::Variable, "v0"),
			t2(Type::Variable, "v1"),
			t3(Type::Variable, "v2"),
			t4(Type::Variable, "v3"),
			t5(Type::Variable, "v4"),
			t6(Type::Variable, "v5"),
			t7(Type::Variable, "v6"),
			t8(Type::Variable, "v7"),
			t9(Type::Variable, "v8"),
			t10(Type::Variable, "v9"),
			t11(Type::URI, "<http://db.uwaterloo.ca/~galuc/wsdbm/subscribes>"),
			t12(Type::URI, "<http://db.uwaterloo.ca/~galuc/wsdbm/Website6244>"),
			t13(Type::URI, "<http://schema.org/caption>"),
			t14(Type::URI, "<http://db.uwaterloo.ca/~galuc/wsdbm/likes>"),
			t15(Type::URI, "<http://db.uwaterloo.ca/~galuc/wsdbm/City142>"),
			t16(Type::URI, "<http://www.geonames.org/ontology#parentCountry>"),
			t17(Type::URI, "<http://db.uwaterloo.ca/~galuc/wsdbm/Product0>"),
			t18(Type::URI, "<http://schema.org/nationality>"),
			t19(Type::URI, "<http://db.uwaterloo.ca/~galuc/wsdbm/Website31204>"),
			t20(Type::URI, "<http://ogp.me/ns#tag>"),
			t21(Type::URI, "<http://db.uwaterloo.ca/~galuc/wsdbm/Topic124>"),
			t23(Type::URI, "<http://schema.org/jobTitle>"),
			t24(Type::URI, "<http://db.uwaterloo.ca/~galuc/wsdbm/City21>"),
			t25(Type::URI, "<http://purl.org/goodrelations/includes>"),
			t26(Type::URI, "<http://db.uwaterloo.ca/~galuc/wsdbm/Retailer346>"),
			t27(Type::URI, "<http://purl.org/goodrelations/offers>"),
			t28(Type::URI, "<http://purl.org/goodrelations/price>"),
			t29(Type::URI, "<http://purl.org/goodrelations/serialNumber>"),
			t30(Type::URI, "<http://purl.org/goodrelations/validFrom>"),
			t31(Type::URI, "<http://purl.org/goodrelations/validThrough>"),
			t32(Type::URI, "<http://schema.org/eligibleQuantity>"),
			t33(Type::URI, "<http://schema.org/eligibleRegion>"),
			t34(Type::URI, "<http://schema.org/priceValidUntil>"),
			t35(Type::URI, "<http://purl.org/dc/terms/Location>"),
			t36(Type::URI, "<http://db.uwaterloo.ca/~galuc/wsdbm/Country21>"),
			t37(Type::URI, "<http://db.uwaterloo.ca/~galuc/wsdbm/gender>"),
			t38(Type::URI, "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"),
			t39(Type::URI, "<http://db.uwaterloo.ca/~galuc/wsdbm/ProductCategory1>"),
			t40(Type::URI, "<http://db.uwaterloo.ca/~galuc/wsdbm/hasGenre>"),
			t41(Type::URI, "<http://schema.org/publisher>"),
			t42(Type::URI, "<http://xmlns.com/foaf/age>"),
			t43(Type::URI, "<http://db.uwaterloo.ca/~galuc/wsdbm/AgeGroup2>"),
			t44(Type::URI, "<http://xmlns.com/foaf/familyName>"),
			t45(Type::URI, "<http://purl.org/ontology/mo/artist>"),
			t46(Type::URI, "<http://db.uwaterloo.ca/~galuc/wsdbm/Country1>"),
			t48(Type::URI, "<http://db.uwaterloo.ca/~galuc/wsdbm/ProductCategory14>"),
			t49(Type::URI, "<http://schema.org/description>"),
			t50(Type::URI, "<http://schema.org/keywords>"),
			t51(Type::URI, "<http://schema.org/language>"),
			t53(Type::URI, "<http://purl.org/ontology/mo/conductor>"),
			t54(Type::URI, "<http://db.uwaterloo.ca/~galuc/wsdbm/SubGenre44>"),
			t55(Type::URI, "<http://schema.org/text>"),
			t56(Type::URI, "<http://db.uwaterloo.ca/~galuc/wsdbm/User534664>"),
			t58(Type::URI, "<http://db.uwaterloo.ca/~galuc/wsdbm/ProductCategory2>"),
			t59(Type::URI, "<http://ogp.me/ns#title>"),
			t60(Type::URI, "<http://schema.org/url>"),
			t61(Type::URI, "<http://db.uwaterloo.ca/~galuc/wsdbm/hits>"),
			t62(Type::URI, "<http://db.uwaterloo.ca/~galuc/wsdbm/SubGenre91>"),
			t63(Type::URI, "<http://schema.org/contentRating>"),
			t65(Type::URI, "<http://db.uwaterloo.ca/~galuc/wsdbm/SubGenre42>"),
			t67(Type::URI, "<http://db.uwaterloo.ca/~galuc/wsdbm/purchaseDate>"),
			t70(Type::URI, "<http://xmlns.com/foaf/homepage>"),
			t71(Type::URI, "<http://db.uwaterloo.ca/~galuc/wsdbm/Topic245>"),
			t72(Type::URI, "<http://schema.org/contentSize>"),
			t73(Type::URI, "<http://db.uwaterloo.ca/~galuc/wsdbm/Language0>"),
			t74(Type::URI, "<http://db.uwaterloo.ca/~galuc/wsdbm/Retailer10487>"),
			t76(Type::URI, "<http://db.uwaterloo.ca/~galuc/wsdbm/Country5>"),
			t77(Type::URI, "<http://db.uwaterloo.ca/~galuc/wsdbm/makesPurchase>"),
			t78(Type::URI, "<http://db.uwaterloo.ca/~galuc/wsdbm/purchaseFor>"),
			t79(Type::URI, "<http://purl.org/stuff/rev#totalVotes>"),
			t80(Type::URI, "<http://db.uwaterloo.ca/~galuc/wsdbm/friendOf>"),
			t81(Type::URI, "<http://purl.org/stuff/rev#hasReview>"),
			t82(Type::URI, "<http://xmlns.com/foaf/givenName>"),
			t83(Type::URI, "<http://db.uwaterloo.ca/~galuc/wsdbm/Role2>"),
			t84(Type::URI, "<http://schema.org/trailer>"),
			t85(Type::URI, "<http://purl.org/stuff/rev#title>"),
			t86(Type::URI, "<http://purl.org/stuff/rev#reviewer>"),
			t87(Type::URI, "<http://schema.org/actor>"),
			t88(Type::URI, "<http://schema.org/legalName>");

		RDFtermList l1;
		l1.add(t1);

		RDFtermList l2;
		l2.add(t2);

		RDFtermList l3;
		l3.add(t3);

		RDFtermList l4;
		l4.add(t4);

		RDFtermList l5;
		l5.add(t5);

		RDFtermList l6;
		l6.add(t6);

		RDFtermList l7;
		l7.add(t7);

		RDFtermList l8;
		l8.add(t8);

		RDFtermList l9;
		l9.add(t9);

		RDFtermList l10;
		l10.add(t10);

		RDFtermList l11;
		l11.add(t11);

		RDFtermList l12;
		l12.add(t12);

		RDFtermList l13;
		l13.add(t13);

		RDFtermList l14;
		l14.add(t14);

		RDFtermList l15;
		l15.add(t15);

		RDFtermList l16;
		l16.add(t16);

		RDFtermList l17;
		l17.add(t17);

		RDFtermList l18;
		l18.add(t18);

		RDFtermList l19;
		l19.add(t19);

		RDFtermList l20;
		l20.add(t20);

		RDFtermList l21;
		l21.add(t21);

		RDFtermList l23;
		l23.add(t23);

		RDFtermList l24;
		l24.add(t24);

		RDFtermList l25;
		l25.add(t25);

		RDFtermList l26;
		l26.add(t26);

		RDFtermList l27;
		l27.add(t27);

		RDFtermList l28;
		l28.add(t28);

		RDFtermList l29;
		l29.add(t29);

		RDFtermList l30;
		l30.add(t30);

		RDFtermList l31;
		l31.add(t31);

		RDFtermList l32;
		l32.add(t32);

		RDFtermList l33;
		l33.add(t33);

		RDFtermList l34;
		l34.add(t34);

		RDFtermList l35;
		l35.add(t35);

		RDFtermList l36;
		l36.add(t36);

		RDFtermList l37;
		l37.add(t37);

		RDFtermList l38;
		l38.add(t38);

		RDFtermList l39;
		l39.add(t39);

		RDFtermList l40;
		l40.add(t40);

		RDFtermList l41;
		l41.add(t41);

		RDFtermList l42;
		l42.add(t42);

		RDFtermList l43;
		l43.add(t43);

		RDFtermList l44;
		l44.add(t44);

		RDFtermList l45;
		l45.add(t45);

		RDFtermList l46;
		l46.add(t46);

		RDFtermList l48;
		l48.add(t48);

		RDFtermList l49;
		l49.add(t49);

		RDFtermList l50;
		l50.add(t50);

		RDFtermList l51;
		l51.add(t51);

		RDFtermList l53;
		l53.add(t53);

		RDFtermList l54;
		l54.add(t54);

		RDFtermList l55;
		l55.add(t55);

		RDFtermList l56;
		l56.add(t56);

		RDFtermList l58;
		l58.add(t58);

		RDFtermList l59;
		l59.add(t59);

		RDFtermList l60;
		l60.add(t60);

		RDFtermList l61;
		l61.add(t61);

		RDFtermList l62;
		l62.add(t62);

		RDFtermList l63;
		l63.add(t63);

		RDFtermList l65;
		l65.add(t65);

		RDFtermList l67;
		l67.add(t67);

		RDFtermList l70;
		l70.add(t70);

		RDFtermList l71;
		l71.add(t71);

		RDFtermList l72;
		l72.add(t72);

		RDFtermList l73;
		l73.add(t73);

		RDFtermList l74;
		l74.add(t74);

		RDFtermList l76;
		l76.add(t76);

		RDFtermList l77;
		l77.add(t77);

		RDFtermList l78;
		l78.add(t78);

		RDFtermList l79;
		l79.add(t79);

		RDFtermList l80;
		l80.add(t80);

		RDFtermList l81;
		l81.add(t81);

		RDFtermList l82;
		l82.add(t82);

		RDFtermList l83;
		l83.add(t83);

		RDFtermList l84;
		l84.add(t84);

		RDFtermList l85;
		l85.add(t85);

		RDFtermList l86;
		l86.add(t86);

		RDFtermList l87;
		l87.add(t87);

		RDFtermList l88;
		l88.add(t88);

		triplePattern
			//////////////// Q0
			p1(l1, l11, l12),
			p2(l3, l13, l4),
			p3(l1, l14, l3),

			//////////////// Q1
			p4(l15, l16, l2),
			p5(l3, l14, l17),
			p6(l3, l18, l2),

			//////////////// Q2
			p7(l1, l14, l2),
			p8(l1, l11, l19),

			//////////////// Q3
			p9(l1, l20, l21),
			p10(l1, l13, l3),

			//////////////// Q4
			p11(l1, l23, l2),
			p12(l24, l16, l4),
			p13(l1, l18, l4),

			//////////////// Q5
			p14(l1, l25, l2),
			p15(l26, l27, l1),
			p16(l1, l28, l4),
			p17(l1, l29, l5),
			p18(l1, l30, l6),
			p19(l1, l31, l7),
			p20(l1, l32, l8),
			p21(l1, l33, l9),
			p22(l1, l34, l10),

			//////////////// Q6
			p23(l1, l35, l2),
			p24(l1, l18, l36),
			p25(l1, l37, l4),
			p26(l1, l38, l83),

			//////////////// Q7
			p27(l1, l38, l39),
			p28(l1, l13, l3),
			p29(l1, l40, l4),
			p30(l1, l41, l5),

			//////////////// Q8
			p31(l1, l42, l43),
			p32(l1, l44, l3),
			p33(l4, l45, l1),
			p34(l1, l18, l46),

			//////////////// Q9
			p35(l1, l38, l48),
			p36(l1, l49, l3),
			p37(l1, l50, l4),
			p38(l1, l51, l73),

			//////////////// Q10
			p39(l1, l53, l2),
			p40(l1, l38, l3),
			p41(l1, l40, l54),

			//////////////// Q11
			p42(l1, l38, l2),
			p360(l1, l55, l3),
			p370(l56, l14, l1),

			//////////////// Q12
			p380(l1, l20, l71),
			p390(l1, l38, l3),
			p400(l4, l84, l5),
			p410(l4, l50, l6),
			p420(l4, l40, l1),
			p43(l4, l38, l58),

			//////////////// Q13
			p44(l1, l70, l2),
			p45(l1, l59, l3),
			p46(l1, l38, l4),
			p47(l1, l13, l5),
			p48(l1, l49, l6),
			p49(l2, l60, l7),
			p50(l2, l61, l8),
			p51(l1, l40, l62),

			//////////////// Q14
			p52(l1, l63, l2),
			p53(l1, l72, l3),
			p54(l1, l40, l65),
			p55(l5, l77, l6),
			p56(l6, l67, l7),
			p57(l6, l78, l1),

			//////////////// Q15
			p58(l1, l70, l2),
			p59(l3, l25, l1),
			p60(l1, l20, l71),
			p61(l1, l49, l5),
			p62(l1, l72, l9),
			p63(l2, l60, l6),
			p64(l2, l61, l7),
			p65(l2, l51, l73),
			p66(l8, l14, l1),

			//////////////// Q16
			p67(l1, l25, l2),
			p68(l74, l27, l1),
			p69(l1, l28, l4),
			p70(l1, l31, l5),
			p71(l2, l59, l6),
			p72(l2, l38, l7),

			//////////////// Q17
			p73(l1, l13, l2),
			p74(l1, l55, l3),
			p75(l1, l63, l4),
			p76(l1, l81, l5),
			p77(l5, l85, l6),
			p78(l5, l86, l7),
			p79(l8, l87, l7),
			p80(l8, l51, l9),

			//////////////// Q18
			p81(l1, l88, l2),
			p82(l1, l27, l3),
			p83(l3, l33, l76),
			p84(l3, l25, l4),
			p85(l5, l23, l6),
			p86(l5, l70, l7),
			p87(l5, l77, l8),
			p88(l8, l78, l4),
			p89(l4, l81, l9),
			p90(l9, l79, l10),

			//////////////// Q19
			p91(l1, l14, l2),
			p92(l1, l80, l3),
			p93(l1, l35, l4),
			p94(l1, l42, l5),
			p95(l1, l37, l6),
			p96(l1, l82, l7);

		//------------------------------- Q0
		qList[0].addToHead(t1);
		qList[0].addToHead(t3);
		qList[0].addToHead(t4);
		qList[0].addToBody(p1);
		qList[0].addToBody(p2);
		qList[0].addToBody(p3);

		//------------------------------- Q1
		qList[1].addToHead(t2);
		qList[1].addToHead(t3);
		qList[1].addToBody(p4);
		qList[1].addToBody(p5);
		qList[1].addToBody(p6);

		//------------------------------- Q2
		qList[2].addToHead(t1);
		qList[2].addToHead(t2);
		qList[2].addToBody(p7);
		qList[2].addToBody(p8);

		//------------------------------- Q3
		qList[3].addToHead(t1);
		qList[3].addToHead(t3);
		qList[3].addToBody(p9);
		qList[3].addToBody(p10);

		//------------------------------- Q4
		qList[4].addToHead(t1);
		qList[4].addToHead(t2);
		qList[4].addToHead(t4);
		qList[4].addToBody(p11);
		qList[4].addToBody(p12);
		qList[4].addToBody(p13);

		//------------------------------- Q5
		qList[5].addToHead(t1);
		qList[5].addToHead(t2);
		qList[5].addToHead(t4);
		qList[5].addToHead(t5);
		qList[5].addToHead(t6);
		qList[5].addToHead(t7);
		qList[5].addToHead(t8);
		qList[5].addToHead(t9);
		qList[5].addToHead(t10);

		qList[5].addToBody(p14);
		qList[5].addToBody(p15);
		qList[5].addToBody(p16);
		qList[5].addToBody(p17);
		qList[5].addToBody(p18);
		qList[5].addToBody(p19);
		qList[5].addToBody(p20);
		qList[5].addToBody(p21);
		qList[5].addToBody(p22);

		//-------------------------------// Q6
		qList[6].addToHead(t1);
		qList[6].addToHead(t2);
		qList[6].addToHead(t4);
		qList[6].addToBody(p23);
		qList[6].addToBody(p24);
		qList[6].addToBody(p25);
		qList[6].addToBody(p26);

		//-------------------------------// Q7
		qList[7].addToHead(t1);
		qList[7].addToHead(t3);
		qList[7].addToHead(t4);
		qList[7].addToHead(t5);
		qList[7].addToBody(p27);
		qList[7].addToBody(p28);
		qList[7].addToBody(p29);
		qList[7].addToBody(p30);

		//-------------------------------// Q8
		qList[8].addToHead(t1);
		qList[8].addToHead(t3);
		qList[8].addToHead(t4);
		qList[8].addToBody(p31);
		qList[8].addToBody(p32);
		qList[8].addToBody(p33);
		qList[8].addToBody(p34);

		//-------------------------------// Q9
		qList[9].addToHead(t1);
		qList[9].addToHead(t3);
		qList[9].addToHead(t4);
		qList[9].addToBody(p35);
		qList[9].addToBody(p36);
		qList[9].addToBody(p37);
		qList[9].addToBody(p38);

		//-------------------------------// Q10
		qList[10].addToHead(t1);
		qList[10].addToHead(t2);
		qList[10].addToHead(t3);
		qList[10].addToBody(p39);
		qList[10].addToBody(p40);
		qList[10].addToBody(p41);

		//-------------------------------// Q11
		qList[11].addToHead(t1);
		qList[11].addToHead(t2);
		qList[11].addToHead(t3);
		qList[11].addToBody(p42);
		qList[11].addToBody(p360);
		qList[11].addToBody(p370);

		//-------------------------------// Q12
		qList[12].addToHead(t1);
		qList[12].addToHead(t3);
		qList[12].addToHead(t4);
		qList[12].addToHead(t5);
		qList[12].addToHead(t6);
		qList[12].addToBody(p380);
		qList[12].addToBody(p390);
		qList[12].addToBody(p400);
		qList[12].addToBody(p410);
		qList[12].addToBody(p420);
		qList[12].addToBody(p43);

		//-------------------------------// Q13
		qList[13].addToHead(t1);
		qList[13].addToHead(t2);
		qList[13].addToHead(t3);
		qList[13].addToHead(t5);
		qList[13].addToHead(t6);
		qList[13].addToHead(t7);
		qList[13].addToHead(t8);

		qList[13].addToBody(p44);
		qList[13].addToBody(p45);
		qList[13].addToBody(p46);
		qList[13].addToBody(p47);
		qList[13].addToBody(p48);
		qList[13].addToBody(p49);
		qList[13].addToBody(p50);
		qList[13].addToBody(p51);

		//-------------------------------// Q14
		qList[14].addToHead(t1);
		qList[14].addToHead(t2);
		qList[14].addToHead(t3);
		qList[14].addToHead(t5);
		qList[14].addToHead(t6);
		qList[14].addToHead(t7);

		qList[14].addToBody(p52);
		qList[14].addToBody(p53);
		qList[14].addToBody(p54);
		qList[14].addToBody(p55);
		qList[14].addToBody(p56);
		qList[14].addToBody(p57);

		//-------------------------------// Q15
		qList[15].addToHead(t1);
		qList[15].addToHead(t2);
		qList[15].addToHead(t3);
		qList[15].addToHead(t5);
		qList[15].addToHead(t6);
		qList[15].addToHead(t7);
		qList[15].addToHead(t8);
		qList[15].addToHead(t9);

		qList[15].addToBody(p58);
		qList[15].addToBody(p59);
		qList[15].addToBody(p60);
		qList[15].addToBody(p61);
		qList[15].addToBody(p62);
		qList[15].addToBody(p63);
		qList[15].addToBody(p64);
		qList[15].addToBody(p65);
		qList[15].addToBody(p66);

		//-------------------------------// Q16
		qList[16].addToHead(t1);
		qList[16].addToHead(t2);
		qList[16].addToHead(t4);
		qList[16].addToHead(t5);
		qList[16].addToHead(t6);
		qList[16].addToHead(t7);

		qList[16].addToBody(p67);
		qList[16].addToBody(p68);
		qList[16].addToBody(p69);
		qList[16].addToBody(p70);
		qList[16].addToBody(p71);
		qList[16].addToBody(p72);

		//-------------------------------// Q17
		qList[17].addToHead(t1);
		qList[17].addToHead(t5);
		qList[17].addToHead(t7);
		qList[17].addToHead(t8);

		qList[17].addToBody(p73);
		qList[17].addToBody(p74);
		qList[17].addToBody(p75);
		qList[17].addToBody(p76);
		qList[17].addToBody(p77);
		qList[17].addToBody(p78);
		qList[17].addToBody(p79);
		qList[17].addToBody(p80);

		//-------------------------------// Q18
		qList[18].addToHead(t1);
		qList[18].addToHead(t4);
		qList[18].addToHead(t5);
		qList[18].addToHead(t9);

		qList[18].addToBody(p81);
		qList[18].addToBody(p82);
		qList[18].addToBody(p83);
		qList[18].addToBody(p84);
		qList[18].addToBody(p85);
		qList[18].addToBody(p86);
		qList[18].addToBody(p87);
		qList[18].addToBody(p88);
		qList[18].addToBody(p89);
		qList[18].addToBody(p90);


		//-------------------------------// Q19
		qList[19].addToHead(t1);

		qList[19].addToBody(p91);
		qList[19].addToBody(p92);
		qList[19].addToBody(p93);
		qList[19].addToBody(p94);
		qList[19].addToBody(p95);
		qList[19].addToBody(p96);

	}

	void initYAGO2() {
		//qList.resize(_LUBM_query_nbr_);
		for (int i = 0; i < _LUBM_query_nbr_; i++) {
			qList.push_back(Query());
		}
		RDFterm t1(Type::Variable, "x"),
			t2(Type::Variable, "y"),
			t3(Type::Variable, "u"),
			t4(Type::Variable, "p"),
			t5(Type::Variable, "p1"),
			t6(Type::Variable, "p2"),
			t7(Type::Variable, "p3"),
			t8(Type::Variable, "e"),
			t9(Type::Variable, "e1"),

			t10(Type::URI, "<livesIn>"),
			t11(Type::URI, "<Athens>"),
			t12(Type::URI, "<graduatedFrom>"),
			t13(Type::URI, "\"Albert Einstein\""),
			t14(Type::URI, "<hasInternalWikipediaLinkTo>"),
			t15(Type::URI, "<hasExternalWikipediaLinkTo>");




		RDFtermList l1;
		l1.add(t1);//x

		RDFtermList l2;
		l2.add(t2);//y

		RDFtermList l3;
		l3.add(t3);//u

		RDFtermList l4;
		l4.add(t4);//p

		RDFtermList l5;
		l5.add(t5);//p1

		RDFtermList l6;
		l6.add(t6);//p2

		RDFtermList l7;
		l7.add(t7);//p3

		RDFtermList l8;
		l8.add(t8);//e

		RDFtermList l9;
		l9.add(t9);//e1

		RDFtermList l10;
		l10.add(t10);//livesIn

		RDFtermList l11;
		l11.add(t11);//Athens

		RDFtermList l12;
		l12.add(t12);//graduatedFrom

		RDFtermList l13;
		l13.add(t13);//Albert Einstein

		RDFtermList l14;
		l14.add(t14);//hasInternalWikipediaLinkTo

		RDFtermList l15;
		l15.add(t15);//hasExternalWikipediaLinkTo

		triplePattern p1(l1, l10, l11), 	//q1	

			p2(l2, l12, l3),		//q2
			p3(l13, l12, l3),		//q2

			p4(l4, l14, l5),		//q3
			p5(l5, l14, l6),		//q3
			p6(l6, l14, l7),		//q3

			p7(l4, l14, l5),		//q4
			p8(l5, l15, l9),		//q4
			p9(l4, l15, l8);		//q4


//-------------------------------query1
		qList[0].addToHead(t1);
		qList[0].addToBody(p1);

		//-------------------------------query2
		qList[1].addToHead(t2);
		qList[1].addToHead(t3);

		qList[1].addToBody(p2);
		qList[1].addToBody(p3);

		//-------------------------------query3
		qList[2].addToHead(t4);
		qList[2].addToHead(t5);
		qList[2].addToHead(t6);
		qList[2].addToHead(t7);

		qList[2].addToBody(p4);
		qList[2].addToBody(p5);
		qList[2].addToBody(p6);

		//-------------------------------	query4

		qList[3].addToHead(t4);
		qList[3].addToHead(t5);
		qList[3].addToHead(t9);
		qList[3].addToHead(t8);

		qList[3].addToBody(p7);
		qList[3].addToBody(p8);
		qList[3].addToBody(p9);

	}

	
	int reset_query_body(int q_nbr, triplePattern p_old, triplePattern p_new)
	{
		qList[q_nbr].re_set_body(p_old, p_new);
		return 0;
	}

	int delelet_qList(const int& index)//从0开始
	{
		if (index >= get_qlist_size())
		{
			std::cout << "index error" << "index =" << index << "  get_qlist_size=" << get_qlist_size() << std::endl;
			return -1;
		}
		qList.erase(qList.begin() + index);
		return 1;
	}

	int add_qlist_element(Query q)
	{
		qList.push_back(q);
		return 0;
	}

	int get_qlist_size()
	{
		return qList.size();
	}

	void qlist_random_shuffle()
	{
		random_shuffle(qList.begin(), qList.end());
	}

	//used in watdiv
	void add_head_list(Query q)
	{
		qList.push_back(q);
	}

	/////////////////////////////
	template <typename Archive> void serialize(Archive& ar, const unsigned int version) {
		ar& qList;
		ar& dataSource;
	}

	/////////////////////////////
	Query getQuery(int q_nbr) {
		if (q_nbr < 0 || q_nbr >= qList.size()) {
			cerr << "A valid query number is expected!" << std::endl;
			exit(1);
		}
		return (qList[q_nbr]);
	}

	//ADD in 2020/08/10 更新查询
	bool updateQuery(int q_nbr, Query q) {
		if (q_nbr < 0 || q_nbr >= qList.size()) {
			cerr << "A valid query number is expected!" << std::endl;
			return false;
		}
		qList[q_nbr] = q;
		return true;
	}
	/////////////////////////////
	void output(Database* db) {
		for (int q_nbr = 0; q_nbr < qList.size(); q_nbr++)
			std::cout << std::endl << "-----------------" << std::endl
			<< "Query# " << q_nbr << std::endl << "-----------------" << std::endl
			<< qList[q_nbr].output(db);
		//return NULL;
	}
};

#endif
