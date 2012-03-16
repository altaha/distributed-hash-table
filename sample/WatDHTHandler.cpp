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

	printf("get_handler start\n");

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
		if (!server->find_closest(_dest, key, true)) { // I don't have any nodes to forward to
			return;
		}
		if (!server->get(_return, key, _dest.ip, _dest.port)) {
			server->erase_node(_dest);	//comm prob with _dest, erase it and find another
			get(_return, key);
		}
	}

	printf("get_handler done\n");
}    
    
void WatDHTHandler::put(const std::string& key,
                        const std::string& val, 
                        const int32_t duration) {
	printf("put_handler start\n");

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
		if (!server->find_closest(_dest, key, true)) { // I don't have any nodes to forward to
			return;
		}
		if (!server->put(key, val, duration, _dest.ip, _dest.port)){
			server->erase_node(_dest); //comm prob with _dest, erase it and find another
			put(key, val, duration);
		}
	}
	printf("put_handler done\n");
}

void WatDHTHandler::join(std::vector<NodeID> & _return, const NodeID& nid)
{
#ifdef VERBOSE_DEBUG
	printf("join_handler start\n");
#endif
	if ( server->isOwner(nid.id) ) { // this is the predecessor of nid
		_return.insert(_return.begin(), server->get_NodeID());
		pthread_rwlock_rdlock(&(server->rt_mutex));
		_return.insert(_return.end(), server->predecessors.begin(), server->predecessors.end());
		_return.insert(_return.end(), server->successors.begin(), server->successors.end());
		pthread_rwlock_unlock(&(server->rt_mutex));
		server->update_connections(nid, false);
	} else {
		NodeID _dest;
		if (!server->find_closest(_dest, nid.id, true)) { // I don't have any nodes to forward to
			return;
		}
		if (!server->join(_return, nid, _dest.ip, _dest.port)) {
			server->erase_node(_dest); //comm prob with _dest, erase it and find another
			join(_return, nid);
		}
	}
#ifdef VERBOSE_DEBUG
	printf("join_handler end\n");
#endif
}    
    

void WatDHTHandler::ping(std::string& _return) {
  // Your implementation goes here
  _return = server->get_id().to_string();
#ifdef VERBOSE_DEBUG
  printf("pinged\n");
#endif
} 

void WatDHTHandler::maintain(std::vector<NodeID> & _return, 
                             const std::string& id, 
                             const NodeID& nid) {
#ifdef VERBOSE_DEBUG
	printf("maintain_handler start\n");
#endif
	if (server->isOwner(id)) { // this is the predecessor of nid
		// populate _return (ensure all neighbours are alive before attaching them)
		_return.insert(_return.begin(), server->get_NodeID());
		pthread_rwlock_rdlock(&(server->rt_mutex));
		_return.insert(_return.end(), server->predecessors.begin(), server->predecessors.end());
		_return.insert(_return.end(), server->successors.begin(), server->successors.end());
		pthread_rwlock_unlock(&(server->rt_mutex));
	} else {
		NodeID _dest;
		if (!server->find_closest(_dest, id, true)) { // I don't have any nodes to forward to
			return;
		}
		if (!server->maintain(_return, id, nid, _dest.ip, _dest.port)) {
			server->erase_node(_dest); //comm prob with _dest, erase it and find another
			maintain(_return, id, nid);
		}
	}
	server->update_connections(nid, false);
#ifdef VERBOSE_DEBUG
    printf("maintain_handler end\n");
#endif
}

void WatDHTHandler::migrate_kv(std::map<std::string, std::string> & _return, 
                               const std::string& nid) {
#ifdef VERBOSE_DEBUG
	printf("migrate_kv_handler start\n");
#endif

	if (server->get_state()==MIGRATE_KV) {
		WatDHTException e;
		e.__set_error_code(WatDHTErrorType::OL_MIGRATION_IN_PROGRESS);
		e.__set_error_message("In the middle of a migration.");
		throw e;		return;
	}

	if (server->isOwner(nid)) { // nid is my successor
		pthread_rwlock_rdlock(&server->hash_mutex);
		std::map<std::string, std::string>::iterator itlow = server->hash_table.lower_bound(nid), it1; // get pointer to key that is >= nid
		std::map<std::string, long>::iterator it2;
 		_return.insert(itlow,server->hash_table.end()); 			// copy key/value pairs for returning
		pthread_rwlock_unlock(&server->hash_mutex);
		it1 = _return.begin();
		while(it1!= _return.end()){
			//lazy delete expired keys
			if( (it2 = server->stale_table.find(it1->first)) != server->stale_table.end()) {//key exists in stale table
				if (time(NULL) > it2->second ){ // key has expired
					_return.erase(it1->first);	// do not return expired keys
				}
				pthread_rwlock_wrlock(&(server->hash_mutex));
				server->stale_table.erase(it1->first);	// remove key from stale table
				pthread_rwlock_unlock(&(server->hash_mutex));
			}
			it1++;
		}
		pthread_rwlock_wrlock(&(server->hash_mutex));
		server->hash_table.erase(itlow,server->hash_table.end());	// delete pairs from local structure
		pthread_rwlock_unlock(&(server->hash_mutex));
	}
	else {
		WatDHTException e;
		e.__set_error_code(WatDHTErrorType::INCORRECT_MIGRATION_SOURCE);
		e.__set_error_message("I'm not your predecessor.");
		NodeID _dest;
		if (!server->find_closest(_dest, nid, true)) { // don't have any nodes to forward to, let migrate come back to me later
			_dest = server->get_NodeID();
		}
		e.__set_node(_dest);
		throw e;		return;
	}
#ifdef VERBOSE_DEBUG
  printf("migrate_kv_handler end\n");
#endif
}

void WatDHTHandler::gossip_neighbors(std::vector<NodeID> & _return, 
                                     const NodeID& nid, 
                                     const std::vector<NodeID> & neighbors) {
#ifdef VERBOSE_DEBUG
	printf("gossip_neighbors_handler start\n");
#endif
	// populate _return
	_return.insert(_return.begin(), server->get_NodeID());
	pthread_rwlock_rdlock(&(server->rt_mutex));
	_return.insert(_return.end(), server->predecessors.begin(), server->predecessors.end());
	_return.insert(_return.end(), server->successors.begin(), server->successors.end());
	pthread_rwlock_unlock(&(server->rt_mutex));

	std::vector<NodeID> neighbors_copy;
	neighbors_copy.insert(neighbors_copy.end(), neighbors.begin(), neighbors.end());
	neighbors_copy.push_back(nid);
	server->update_connections(neighbors_copy, true);
#ifdef VERBOSE_DEBUG
	printf("gossip_neighbors_handler end\n");
#endif
}

void WatDHTHandler::closest_node_cr(NodeID& _return, const std::string& id) {
	if (server->isOwner(id)) { //
		_return = server->get_NodeID();
	} else {
		NodeID _dest;
		if (!server->find_closest(_dest, id, false)) { //don't have any nodes to pass back, just pass my own id back
			_return = server->get_NodeID();
			return;
		}
		if (!server->closest_node_cr(_return, id, _dest.ip, _dest.port)) {
			server->erase_node(_dest); //comm prob with _dest, erase it and find another
			closest_node_cr(_return, id);
		}
	}

#ifdef VERBOSE_DEBUG
  printf("closest_node_cr\n");
#endif
}

void WatDHTHandler::closest_node_ccr(NodeID& _return, const std::string& id) {
	if (server->isOwner(id)) { //
		_return = server->get_NodeID();
	} else {
		NodeID _dest;
		if (!server->find_closest(_return, id, true)) { //don't have any nodes to pass back, just pass my own id back
			_return = server->get_NodeID();
			return;
		}
		if (!server->closest_node_ccr(_return, id, _dest.ip, _dest.port)) {
			server->erase_node(_dest);
			closest_node_ccr(_return, id);
		}
	}

#ifdef VERBOSE_DEBUG
	printf("closest_node_ccr\n");
#endif
}    
} // namespace WatDHT

