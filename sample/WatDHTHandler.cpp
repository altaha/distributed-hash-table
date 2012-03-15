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

void WatDHTHandler::get(std::string& _return, const std::string& key)
{
	this->server->wat_state.wait_ge(NODE_READY);

	if (server->isOwner(key)) {
		pthread_rwlock_rdlock(&(server->hash_mutex));
		std::map<std::string,long>::iterator it1 = server->stale_table.find(key);
		pthread_rwlock_unlock(&(server->hash_mutex));
		if(it1!= server->stale_table.end()){
			//lazy delete expired keys
			if( time(NULL) > it1->second ){
				pthread_rwlock_wrlock(&(server->hash_mutex));
				server->hash_table.erase(key);
				server->stale_table.erase(key);
				pthread_rwlock_unlock(&(server->hash_mutex));
			}
		}
		pthread_rwlock_rdlock(&(server->hash_mutex));
		std::map<std::string,std::string>::iterator it2 = server->hash_table.find(key);
		pthread_rwlock_unlock(&(server->hash_mutex));
		if (it2 == server->hash_table.end()) {
			WatDHTException e;
			e.__set_error_code(WatDHTErrorType::KEY_NOT_FOUND);
			e.__set_error_message("Key not found");
			throw e;
		}
		else {
			_return = it2->second;
		}
	}
	else {
		NodeID _dest;
		server->find_closest(_dest, key, true);
		server->get(_return, key, _dest.ip, _dest.port);
	}
  printf("get\n");
}    
    
void WatDHTHandler::put(const std::string& key,
                        const std::string& val, 
                        const int32_t duration) {
	this->server->wat_state.wait_ge(NODE_READY);

	if (server->isOwner(key)) {
		pthread_rwlock_wrlock(&(server->hash_mutex));
		if (duration==0) {
			server->hash_table.erase(key);
			server->stale_table.erase(key);
		}
		else {
			server->hash_table.insert(std::pair<std::string,std::string>(key,val));
			if (duration>0) {
				//add to Remove hash map
				server->stale_table.insert(std::pair<std::string,long>(key, time(NULL)+duration ));
			}
		}
		pthread_rwlock_unlock(&(server->hash_mutex));
	}
	else {
		NodeID _dest;
		server->find_closest(_dest, key, true);
		server->put(key, val, duration, _dest.ip, _dest.port);
	}
  printf("put\n");
}

void WatDHTHandler::join(std::vector<NodeID> & _return, const NodeID& nid)
{
	//if (server->isOwner(nid.id)) { // this is the predecessor of nid
	if ( server->isOwner(nid.id) ) { // this is the predecessor of nid
		_return.insert(_return.begin(), server->get_NodeID());
		pthread_rwlock_rdlock(&(server->rt_mutex));
		_return.insert(_return.end(), server->predecessors.begin(), server->predecessors.end());
		_return.insert(_return.end(), server->successors.begin(), server->successors.end());
		pthread_rwlock_unlock(&(server->rt_mutex));
		server->update_connections(nid, false);
	} else {
		NodeID _dest;
		server->find_closest(_dest, nid.id, true);
		server->join(_return, nid, _dest.ip, _dest.port);
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
	if (server->isOwner(id)) { // this is the predecessor of nid
		// populate _return (ensure all neighbours are alive before attaching them)
		_return.insert(_return.begin(), server->get_NodeID());
		pthread_rwlock_rdlock(&(server->rt_mutex));
		_return.insert(_return.end(), server->predecessors.begin(), server->predecessors.end());
		_return.insert(_return.end(), server->successors.begin(), server->successors.end());
		pthread_rwlock_unlock(&(server->rt_mutex));
	} else {
		NodeID _dest;
		server->find_closest(_dest, id, true);
		server->maintain(_return, id, nid, _dest.ip, _dest.port);
	}
	server->update_connections(nid, false);
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
		std::map<std::string, std::string>::iterator itlow = server->hash_table.lower_bound(nid); // get pointer to key that is >= nid
		_return.insert(itlow,server->hash_table.end()); 			// copy key/value pairs for returning
		pthread_rwlock_wrlock(&(server->hash_mutex));
		server->hash_table.erase(itlow,server->hash_table.end());	// delete pairs from local structure
		pthread_rwlock_unlock(&(server->hash_mutex));
	}
	else {
		WatDHTException e;
		e.__set_error_code(WatDHTErrorType::INCORRECT_MIGRATION_SOURCE);
		e.__set_error_message("I'm not your predecessor.");
		//**TODO**Check node before sending it back
	/*	NodeID sucessor = server->successors.front();
		if (!server->ping(sucessor.ip, sucessor.port)) {
			// call delete on this node
			sucessor = server->successors.front();
		}*/
		e.__set_node(server->successors.front());
		throw e;		return;
	}
  printf("migrate_kv\n");
}

void WatDHTHandler::gossip_neighbors(std::vector<NodeID> & _return, 
                                     const NodeID& nid, 
                                     const std::vector<NodeID> & neighbors) {
	// populate _return (ensure all neighbours are alive before attaching them)
	_return.insert(_return.begin(), server->get_NodeID());
	pthread_rwlock_rdlock(&(server->rt_mutex));
	_return.insert(_return.end(), server->predecessors.begin(), server->predecessors.end());
	_return.insert(_return.end(), server->successors.begin(), server->successors.end());
	pthread_rwlock_unlock(&(server->rt_mutex));

	std::vector<NodeID> neighbors_copy;
	neighbors_copy.insert(neighbors_copy.begin(), neighbors.begin(), neighbors.end());
	neighbors_copy.push_back(nid);
	server->update_connections(neighbors_copy, true);
	printf("gossip_neighbors\n");
}

void WatDHTHandler::closest_node_cr(NodeID& _return, const std::string& id) {
	server->find_closest(_return, id, true);
  printf("closest_node_cr\n");
}

void WatDHTHandler::closest_node_ccr(NodeID& _return, const std::string& id) {
	server->find_closest(_return, id, false);
	printf("closest_node_ccr\n");
}    
} // namespace WatDHT

