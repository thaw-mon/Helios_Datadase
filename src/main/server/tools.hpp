///////////////////////////////////////////////////////////////////////////////////
#define _socketsPerWorkerThread_ 11 // Assuming there are ten PUSH sockets plus one PULL socket per worker thread.
#define _portRangeStartingPoint_ 30000 // The range of reserved ports starts from 30000.
#define _ctplThreadPoolWaitingQueueLength_ 1000  
///////////////////////////////////////////////////////////////////////////////////
const string query_proxy_port =  "54321", 
queryPlan_proxy_port =  "65432", 
subQueryPlan_proxy_port = "33333";


///////////////////////////////////////////////////////////////////////////////////
#include "zmq.hpp"
#include <boost/serialization/string.hpp>
#include <boost/serialization/vector.hpp>
#include <boost/archive/binary_oarchive.hpp>
#include <boost/archive/binary_iarchive.hpp>
#include <boost/serialization/utility.hpp>
#include <boost/lockfree/queue.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/thread.hpp>
#include <set>
#include <vector>
#include <sstream>
#include <iostream>
#include <string>
#include <thread>
#include <time.h>
#include <fstream>
#include <unistd.h>
#include <fstream>
#include <unistd.h>
#include "mpi.h"
//#include "RDF.hpp"
#include "type.h"
#include <functional>
#include <atomic>
#include <memory>
#include <exception>
#include <future>
#include <mutex>

///////////////////////////////////////////////////////////////////////////////////
#define RESET   "\033[0m"
#define BLACK   "\033[30m"      /* Black */
#define RED     "\033[31m"      /* Red */
#define GREEN   "\033[32m"      /* Green */
#define YELLOW  "\033[33m"      /* Yellow */
#define BLUE    "\033[34m"      /* Blue */
#define MAGENTA "\033[35m"      /* Magenta */
#define CYAN    "\033[36m"      /* Cyan */
#define WHITE   "\033[37m"      /* White */
#define BOLDBLACK   "\033[1m\033[30m"      /* Bold Black */
#define BOLDRED     "\033[1m\033[31m"      /* Bold Red */
#define BOLDGREEN   "\033[1m\033[32m"      /* Bold Green */
#define BOLDYELLOW  "\033[1m\033[33m"      /* Bold Yellow */
#define BOLDBLUE    "\033[1m\033[34m"      /* Bold Blue */
#define BOLDMAGENTA "\033[1m\033[35m"      /* Bold Magenta */
#define BOLDCYAN    "\033[1m\033[36m"      /* Bold Cyan */
#define BOLDWHITE   "\033[1m\033[37m"      /* Bold White */

///////////////////////////////////////////////////////////////////////////////////
using namespace std;

///////////////////////////////////////////////////////////////////////////////////
void errorReport(string firstMSG, string secondMSG = " ", int fatal = 1) {
	cerr << endl;
    cerr << BOLDRED << firstMSG << " " << secondMSG << RESET;
	cerr << endl;
	if (fatal) exit (1);
}

///////////////////////////////////////////////////////////////////////////////////
string ZMQ_errorReport(int errorNo) {
	switch (errorNo) {
		case EINVAL: return("Details: The endpoint is invalid."); break;
		case ETERM: return("Details: The context associated with the specified socket was terminated."); break;
		case ENOTSOCK: return("Details: The socket was invalid."); break;
		case ENOENT: return("Details: The endpoint is not connected."); break;
		default : return "Details: Unknown error";
	}
}

///////////////////////////////////////////////////////////////////////////////////
void send_(zmq::socket_t &s, zmq::message_t *m) {
	while (1) 
		try {
			s.send (*m);
			break;
		} catch (std::exception const& e) {
			if (zmq_errno() == EINTR || zmq_errno() == EAGAIN) { 
				errorReport("EINTR error", " ", 0); 
				continue; 
			} else errorReport("An error occured while sending. Details: ", e.what()); 
		} 
}

///////////////////////////////////////////////////////////////////////////////////
void receive_(zmq::socket_t &s, zmq::message_t *m) {
	while (1) 
		try {
			s.recv (m);
			break;
		} catch (std::exception const& e) {
			if (zmq_errno() == EINTR || zmq_errno() == EAGAIN) { 
				errorReport("EINTR error", " ", 0); 
				continue; 
			} else errorReport("An error occured while receiving. Details: ", e.what()); 
		} 
} 

///////////////////////////////////////////////////////////////////////////////////
void connect_(zmq::socket_t &s, string endpoint) {
	const char* addr_ = endpoint.c_str();
	while (1) 
		try {
			s.connect(addr_);
			break;
		} catch (std::exception const& e) {
			continue; }
} 

///////////////////////////////////////////////////////////////////////////////////
void connect__(zmq::socket_t &s, string endpoint) {
	int success = zmq_connect(s, endpoint.c_str());
	if (success == -1)
		errorReport("A failure while connecting to " + endpoint, ZMQ_errorReport(zmq_errno()));
} 

///////////////////////////////////////////////////////////////////////////////////
void disconnect_(zmq::socket_t &s, string endpoint) {
	const char* addr_ = endpoint.c_str();
	while (1) 
		try {
			s.disconnect(addr_);
			break;
		} catch (std::exception const& e) { 
			continue; }
} 

///////////////////////////////////////////////////////////////////////////////////
void disconnect__(zmq::socket_t &s, string endpoint) {
	int success = zmq_disconnect(s, endpoint.c_str());
	if (success == -1)
		errorReport("A failure while dis-connecting from " + endpoint, ZMQ_errorReport(zmq_errno()));
} 

///////////////////////////////////////////////////////////////////////////////////
void bind_(zmq::socket_t &s, string endpoint) {
	const char* addr_ = endpoint.c_str();
	while (1) 
	try {
			s.bind(addr_);
			break;
		} catch (std::exception const& e) { 
			continue; }
} 

///////////////////////////////////////////////////////////////////////////////////
void bind__(zmq::socket_t &s, string endpoint) {
  int success = zmq_bind(s, endpoint.c_str());
  if (success == -1) errorReport("A failure while binding to ", endpoint);
} 

///////////////////////////////////////////////////////////////////////////////////
class spinlock {
    private:
        std::atomic <bool> lock_;

    public:
        spinlock() {
            lock_.store(false);
        }

        void lock() {
            bool unlocked = false;
            while (!lock_.compare_exchange_weak(unlocked, true));

        }
		
		void unlock() {
            lock_.store(false);
        }
    };