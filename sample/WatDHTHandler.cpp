#include "WatDHTHandler.h"
#include <string>
#include <vector>
#include <map>

#include "WatDHTServer.h"

using namespace ::apache::thrift;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;
using namespace ::apache::thrift::server;
using namespace ::apache::thrift::concurrency;

using boost::shared_ptr;

namespace WatDHT {

WatDHTHandler::WatDHTHandler(WatDHTServer* dht_server) : server(dht_server) {
  // Your initialization goes here
}

WatDHTHandler::~WatDHTHandler() {}

void WatDHTHandler::get(std::string& _return, const std::string& key) {
	while (server->get_state()==MIGRATE_KV);

	if (server->isOwner(key)) {
		std::map<std::string,std::string>::iterator it = server->hash_table.find(key);
		if (it == server->hash_table.end()) {
			WatDHTException e;
			e.__set_error_code(WatDHTErrorType::KEY_NOT_FOUND);
			e.__set_error_message("Key not found");
			throw e;
		}
		else { _return = it->second; }
	}
	else {
		NodeID _dest;
		server->find_closest(key, _dest);
		server->get(_return, key, _dest.ip, _dest.port);
	}
  printf("get\n");
}    
    
void WatDHTHandler::put(const std::string& key,
                        const std::string& val, 
                        const int32_t duration) {
	while (server->get_state()==MIGRATE_KV);

	if (server->isOwner(key)) {
		pthread_rwlock_wrlock(&(server->hash_mutex));
		if (duration==0) { server->hash_table.erase(key); }
		else {
			server->hash_table.insert(std::pair<std::string,std::string>(key,val));
		}
		pthread_rwlock_unlock(&(server->hash_mutex));

		if (duration>0) {
			//add to toRemove queue
		}
	}
	else {
		NodeID _dest;
		server->find_closest(key, _dest);
		server->put(key, val, duration, _dest.ip, _dest.port);
	}
  printf("put\n");
}

void WatDHTHandler::join(std::vector<NodeID> & _return, const NodeID& nid)
{
	if (server->isOwner(nid.id)) { // this is the predecessor of nid
		// populate _return (ensure all neighbours are alive before attaching them)
		_return.insert(_return.begin(), server->get_NodeID());
		_return.insert(_return.end(), server->predecessors.begin(), server->predecessors.end());
		_return.insert(_return.end(), server->successors.begin(), server->successors.end());
		_return.insert(_return.end(), server->rtable.begin(), server->rtable.end());
	} else {
		NodeID _dest;
		server->find_closest(nid.id, _dest);
		server->forward_join(_return, nid, _dest.ip, _dest.port);
	}
	printf("join\n");
}    
    

void WatDHTHandler::ping(std::string& _return) {
  // Your implementation goes here
  printf("ping\n");
  _return = server->get_id().to_string(); 
} 

void WatDHTHandler::maintain(std::vector<NodeID> & _return, 
                             const std::string& id, 
                             const NodeID& nid) {
  // Your implementation goes here
  printf("maintain\n");
}

void WatDHTHandler::migrate_kv(std::map<std::string, std::string> & _return, 
                               const std::string& nid) {
	if (server->get_state()==MIGRATE_KV) {
		WatDHTException e;
		e.__set_error_code(WatDHTErrorType::OL_MIGRATION_IN_PROGRESS);
		e.__set_error_message("In the middle of a migration.");
		throw e;		return;
	}

	if (server->successors.front().id==nid) { // nid is my successor
		//**MISSING** return all values in hash_Table >= nid

	} else {
	    NodeID it = server->predecessors.front();
		server->migrate_kv(_return, nid, it.ip, it.port);
	}
	printf("join\n");
  printf("migrate_kv\n");
}

void WatDHTHandler::gossip_neighbors(std::vector<NodeID> & _return, 
                                     const NodeID& nid, 
                                     const std::vector<NodeID> & neighbors) {
  // Your implementation goes here
  printf("gossip_neighbors\n");
}

void WatDHTHandler::closest_node_cr(NodeID& _return, const std::string& id) {
  // Your implementation goes here
  printf("closest_node_cr\n");
}

void WatDHTHandler::closest_node_ccr(NodeID& _return, const std::string& id) {
  // Your implementation goes here
  printf("closest_node_ccr\n");
}    
} // namespace WatDHT

