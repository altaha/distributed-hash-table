#include "WatDHTServer.h"
#include "WatDHT.h"
#include "WatID.h"
#include "WatDHTState.h"
#include "WatDHTHandler.h"
#include <unistd.h>
#include <iostream>
#include <pthread.h>
#include <protocol/TBinaryProtocol.h>
#include <server/TSimpleServer.h>
#include <transport/TSocket.h>
#include <transport/TServerSocket.h>
#include <transport/TBufferTransports.h>
#include <thrift/concurrency/ThreadManager.h>
#include <thrift/concurrency/PosixThreadFactory.h>
#include <thrift/server/TThreadedServer.h>

using namespace ::apache::thrift;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;
using namespace ::apache::thrift::server;
using namespace ::apache::thrift::concurrency;

using boost::shared_ptr;

namespace WatDHT {

void printList(std::list<NodeID>& in){
	for( std::list<NodeID>::iterator it=in.begin(); it!=in.end(); it++ ){
		printf("NodeID port: %d\n", it->port);
	}
}
void printVector(std::vector<NodeID>& in){
	for( std::vector<NodeID>::iterator it=in.begin(); it!=in.end(); it++ ){
		printf("NodeID port: %d\n", it->port);
	}
}

bool compNodeCR (const NodeID& i,const NodeID& j, const WatID& reference)
{
	WatID k, l;
	k.copy_from(i.id); l.copy_from(j.id);
	return ( reference.distance_cr(k) < reference.distance_cr(l) );
}

void insSorted (std::list<NodeID>& insList, const NodeID& i, const WatID& reference)
{
	std::list<NodeID>::iterator it = insList.begin();
	while(it != insList.end()){
		if ( compNodeCR (i, *it, reference) ){
			insList.insert(it, i);
			return;
		}
		it++;
	}
	insList.insert(it, i);
}

WatDHTServer::WatDHTServer(const char* id, 
                           const char* ip, 
                           int port) throw (int) : rpc_server(NULL) {
  rt_buckets = 0;
  wat_id.set_using_md5(id);
  wat_id.debug_md5();
  server_node_id.id = wat_id.to_string();
  server_node_id.ip = ip;
  server_node_id.port = port;
  pthread_rwlock_init(&rt_mutex, NULL);
  pthread_rwlock_init(&hash_mutex, NULL);

  pappa_list.push_back(&predecessors);
  pappa_list.push_back(&successors);
  pappa_list.push_back(&rtable);

  int rc = pthread_create(&rpc_thread, NULL, start_rpc_server, this);
  if (rc != 0) {
    throw rc; // Just throw the error code
  }
}

WatDHTServer::~WatDHTServer() {
	pthread_rwlock_destroy(&rt_mutex);
	pthread_rwlock_destroy(&hash_mutex);
  printf("In destructor of WatDHTServer\n");
  delete rpc_server;
}

// Join the DHT network and wait
int WatDHTServer::test(const char* ip, int port) {
  wat_state.wait_ge(WatDHT::SERVER_CREATED);
  
  // The following is an example of sending a PING. This is normally not
  // necessary during the join operation.
  boost::shared_ptr<TSocket> socket(new TSocket(ip, port));
  boost::shared_ptr<TTransport> transport(new TBufferedTransport(socket));
  boost::shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
  WatDHTClient client(protocol);
  try {
    transport->open();
    std::string remote_str;
    client.ping(remote_str);
    transport->close();
    // Create a WatID object from the return value.
    WatID remote_id;
    if (remote_id.copy_from(remote_str) == -1) {
      printf("Received invalid ID\n");
    } else {
      printf("Received:\n");
      remote_id.debug_md5();
    }
  } catch (TTransportException e) {
    printf("Caught exception: %s\n", e.what());
  } 
  return 0;
}

void WatDHTServer::update_connections(const NodeID& input, bool ping_nodes)
{
	std::list<NodeID> sorted;
	sorted.push_back(input);
	do_update(sorted, ping_nodes);
}

void WatDHTServer::update_connections(const std::vector<NodeID>& input, bool ping_nodes)
{
	//Create a sorted list based on distance_cr (use insertion sort)
	std::list<NodeID> sorted;
	std::list<NodeID>::iterator it;
	for(uint i=0; i<input.size(); ++i)
	{
		for (it=sorted.begin(); it!=sorted.end(); it++) {
			if( compNodeCR( input[i], (*it), this->wat_id ) ){
				sorted.insert( it, input[i]);
				break;
			}
		}
		if (it == sorted.end()){
			sorted.push_back( input[i] );
		}
	}
	//make sure list entries are unique
	sorted.unique();

	//Remove instance of this->wat_id in input
	sorted.remove(this->server_node_id);

	printf("Printing what has been received...\n");
	printList(sorted);

	do_update(sorted, ping_nodes);
}

void WatDHTServer::do_update(std::list<NodeID>& sorted, bool ping_nodes)
{
	std::list<NodeID>::iterator it;
	if(ping_nodes){ //ping every entry in sorted to make sure they are alive
		it = sorted.begin();
		while( it!=sorted.end() ){
			if(!this->ping(it->ip,it->port)){
				it = sorted.erase(it);
			}else{
				it++;
			}
		}
	}

	pthread_rwlock_wrlock(&rt_mutex);

	//Make sure no duplicates between sorted and (predecessors, successors, and rtable)
	for (uint i=0; i<pappa_list.size(); i++) {
		if(i<2){ //successors and predecessors
			for (it=pappa_list[i]->begin(); it!=pappa_list[i]->end(); it++)
			{
				sorted.remove(*(it));
			}
		}
		else { //routing table
			for (it=sorted.begin(); it!=sorted.end(); it++)
			{
				rtable.remove(*(it));
			}
		}
	}

	// Update neighbour lists first
	std::list<NodeID>::iterator suc_it = successors.begin();
	std::list<NodeID>::iterator pred_it= predecessors.begin();
	uint i =0;
	while(i<4 && !sorted.empty())
	{
		if(!(i&1)){ //even: compare begin of sorted with successors
			if( successors.empty() || compNodeCR( sorted.front(), *(suc_it), this->wat_id ) ){
				successors.insert( suc_it,sorted.front() );
				sorted.pop_front();
				if( successors.size()>2){
					insSorted (sorted, successors.back(), this->wat_id);
					sorted.unique();
					successors.pop_back();
				}
			}
			else{
				suc_it++;
			}
		}else{ //odd: compare end of sorted with predecessors
			if( predecessors.empty() || compNodeCR( sorted.back(), *(pred_it), this->wat_id ) ){
				predecessors.insert( pred_it,sorted.back() );
				sorted.pop_back();
				if( predecessors.size()>2){
					insSorted (sorted, predecessors.back(), this->wat_id);
					sorted.unique();
					predecessors.pop_back();
				}
			}
			else{
				pred_it++;
			}
		}
		i++;
	}

	//HANDLE routing table updates
	rt_buckets=0;
	if(!sorted.empty()){
		rtable.insert(rtable.end(),sorted.begin(),sorted.end());
	}
	it=rtable.begin();
	while( it!= rtable.end() ){
		WatID bucket;
		bucket.copy_from(it->id);
		int rc = this->wat_id.hmatch_bin(bucket,1);
		if(rc==-1 && !(this->rt_buckets & BUCKET_1) ){ //different MSB
			this->rt_buckets |= BUCKET_1;
		}
		else if( rc==0 && !(this->rt_buckets & BUCKET_2) ){ //different 2nd MSB
			this->rt_buckets |= BUCKET_2;
		}
		else if( rc==1 && !(this->rt_buckets & BUCKET_3) ){ //different 3rd MSB
			this->rt_buckets |= BUCKET_3;
		}
		else if( rc==2 && !(this->rt_buckets & BUCKET_4) ){ //different 4th MSB
			this->rt_buckets |= BUCKET_4;
		}
		else{
			it = rtable.erase(it); //advances it to next
			continue;
		}
		it++;
	}

	pthread_rwlock_unlock(&rt_mutex);

	printf("Predecessors...\n");
	printList(this->predecessors);
	printf("Successors...\n");
	printList(this->successors);
	printf("Routing Table...\n");
	printList(this->rtable);
}

// Join the DHT network and wait
void WatDHTServer::join(std::vector<NodeID>& _return, const NodeID& nid, std::string ip, int port)
{
  wat_state.wait_ge(WatDHT::SERVER_CREATED);

  boost::shared_ptr<TSocket> socket(new TSocket(ip, port));
  boost::shared_ptr<TTransport> transport(new TBufferedTransport(socket));
  boost::shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
  WatDHTClient client(protocol);

  if(nid == this->server_node_id){ //initate join
	  this->wat_state.change_state(MIGRATE_KV);
  }

  try {
	transport->open();
	std::string remote_str;
	client.join( _return, nid);
	transport->close();

  } catch (TTransportException e) {
	printf("Caught exception: %s\n", e.what());
	if( e.getType() == TTransportException::NOT_OPEN ){
		//Failed communication, probably means node is dead
	}
	return;
  }
  if(nid == this->server_node_id) //join initiator
  {
	  //**TODO** use return vector to populate neighbour set
	  std::vector<NodeID>::iterator it;
	  for (it=_return.begin(); it!=_return.end(); it++) {
		  std::cout << "Port number = " << it->port << std::endl;
	  }

	  update_connections(_return, false);


  }
  else{ //forward join
	  _return.push_back(server_node_id); 	//add my nodeID to return
  }
}


void WatDHTServer::migrate_kv(std::map<std::string, std::string>& _return, const std::string& nid,
		  std::string ip, int port)
{
	boost::shared_ptr<TSocket> socket(new TSocket(ip, port));
	boost::shared_ptr<TTransport> transport(new TBufferedTransport(socket));
	boost::shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
	WatDHTClient client(protocol);
	static uint sleepTime;
	try {
		transport->open();
		std::string remote_str;
		try {
			client.migrate_kv(_return, nid);
		} catch (WatDHTException e) {
			std::cout << "Caught exception: " << e.error_message << std::endl;

			if (e.error_code==WatDHTErrorType::INCORRECT_MIGRATION_SOURCE){
				migrate_kv(_return, e.node.id, e.node.ip, e.node.port);
			}
			else if (e.error_code==WatDHTErrorType::OL_MIGRATION_IN_PROGRESS) {
				if (sleepTime<2) { sleepTime=2; }
				sleep(sleepTime);
				sleepTime*=sleepTime;
				migrate_kv(_return, nid, ip, port);
				sleepTime = 0;
			}
		}
		transport->close();
	} catch (TTransportException e) {
		printf("Caught exception: %s\n", e.what());
	}
	pthread_rwlock_wrlock(&hash_mutex);
	hash_table.insert(_return.begin(),_return.end());
	pthread_rwlock_unlock(&hash_mutex);
}

void WatDHTServer::get(std::string& _return, const std::string& key, std::string ip, int port)
{
	boost::shared_ptr<TSocket> socket(new TSocket(ip, port));
	boost::shared_ptr<TTransport> transport(new TBufferedTransport(socket));
	boost::shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
	WatDHTClient client(protocol);
	try {
		transport->open();
		std::string remote_str;
		client.get(_return, key);
		transport->close();
	} catch (TTransportException e) {
		printf("Caught exception: %s\n", e.what());
	}
}

void WatDHTServer::put(const std::string& key, const std::string& val, const int32_t duration, std::string ip, int port)
{
	boost::shared_ptr<TSocket> socket(new TSocket(ip, port));
	boost::shared_ptr<TTransport> transport(new TBufferedTransport(socket));
	boost::shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
	WatDHTClient client(protocol);
	try {
		transport->open();
		std::string remote_str;
		client.put(key, val, duration);
		transport->close();
	} catch (TTransportException e) {
		printf("Caught exception: %s\n", e.what());
	}
}

void WatDHTServer::find_closest(NodeID& _dest, const std::string& key, bool cw)
{
	//** TODO: check if any node in neighbour set is owner of key **//
	//find node in neighbour set and routing table that is closest in distance to key
	std::list<NodeID>::iterator it, closest;
	WatID toFind, curr, temp, closestDist;
	closestDist.copy_from(""); // initializing to a null WatID -- test correctness
	toFind.copy_from(key);
	pthread_rwlock_rdlock(&rt_mutex);

	for (uint i=0; i<pappa_list.size(); i++) {
		for (it=pappa_list[i]->begin(); it!=pappa_list[i]->end(); it++) {
			curr.copy_from(it->ip);
			 if (cw) { temp = curr.distance_cr(toFind); }
			 else	 { temp = curr.distance_ccr(toFind); }
			if (temp < closestDist) {
				closestDist = temp;
				closest = it;
			}
		}
	}

	pthread_rwlock_unlock(&rt_mutex);

	_dest = *closest;
}

void WatDHTServer::maintain(std::vector<NodeID> & _return, const std::string& id,
                             const NodeID& nid, std::string ip, int port)
{
	boost::shared_ptr<TSocket> socket(new TSocket(ip, port));
	boost::shared_ptr<TTransport> transport(new TBufferedTransport(socket));
	boost::shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
	WatDHTClient client(protocol);
	try {
		transport->open();
		std::string remote_str;
		client.maintain(_return, id, nid);
		transport->close();
	} catch (TTransportException e) {
		printf("Caught exception: %s\n", e.what());
	}
}

void WatDHTServer::gossip_neighbours(std::vector<NodeID> & _return, const NodeID& nid,
        const std::vector<NodeID> & neighbors, std::string ip, int port)
{
	boost::shared_ptr<TSocket> socket(new TSocket(ip, port));
	boost::shared_ptr<TTransport> transport(new TBufferedTransport(socket));
	boost::shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
	WatDHTClient client(protocol);
	try {
		transport->open();
		std::string remote_str;
		client.gossip_neighbors(_return, nid, neighbors);
		transport->close();
	} catch (TTransportException e) {
		printf("Caught exception: %s\n", e.what());
	}
}

bool WatDHTServer::ping(std::string ip, int port)
{
	boost::shared_ptr<TSocket> socket(new TSocket(ip, port));
	boost::shared_ptr<TTransport> transport(new TBufferedTransport(socket));
	boost::shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
	WatDHTClient client(protocol);
	try {
		transport->open();
		std::string remote_str;
		client.ping(remote_str);
		transport->close();
		// Create a WatID object from the return value.
		WatID remote_id;
		if (remote_id.copy_from(remote_str) == -1) {
			printf("Received invalid ID\n");
			return false;
		} else {
			printf("Received:\n");
			remote_id.debug_md5();
		}
	} catch (TTransportException e) {
		printf("Caught exception: %s\n", e.what());
		return false;
	}
	return true;
}

void WatDHTServer::closest_node_cr(NodeID& _return, const std::string& id, std::string ip, int port)
{
	boost::shared_ptr<TSocket> socket(new TSocket(ip, port));
	boost::shared_ptr<TTransport> transport(new TBufferedTransport(socket));
	boost::shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
	WatDHTClient client(protocol);
	try {
		transport->open();
		std::string remote_str;
		client.closest_node_cr(_return, id);
		transport->close();
	} catch (TTransportException e) {
		printf("Caught exception: %s\n", e.what());
	}

	//**TODO** use return value to update nearest neighbours
}

void WatDHTServer::closest_node_ccr(NodeID& _return, const std::string& id, std::string ip, int port)
{
	boost::shared_ptr<TSocket> socket(new TSocket(ip, port));
	boost::shared_ptr<TTransport> transport(new TBufferedTransport(socket));
	boost::shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
	WatDHTClient client(protocol);
	try {
		transport->open();
		std::string remote_str;
		client.closest_node_ccr(_return, id);
		transport->close();
	} catch (TTransportException e) {
		printf("Caught exception: %s\n", e.what());
	}

	//**TODO** use return value to update nearest neighbours
}

bool WatDHTServer::isOwner(const std::string& key)
{
	WatID toFind, closest;
	toFind.copy_from(key);
	pthread_rwlock_rdlock(&rt_mutex);
	if (!successors.empty()) {
		closest.copy_from(successors.begin()->id);
	}
	else if (!predecessors.empty()) {
		closest.copy_from(predecessors.begin()->id);
	}
	else {
		pthread_rwlock_unlock(&rt_mutex);
		return true;
	} //**TODO** - look at routing table?

	pthread_rwlock_unlock(&rt_mutex);
	return ( this->wat_id.distance_cr(toFind) < this->wat_id.distance_cr(closest) ) ? true : false;
}

int WatDHTServer::wait() {
  wat_state.wait_ge(WatDHT::SERVER_CREATED);
  // Perhaps perform your periodic tasks in this thread.
  pthread_join(rpc_thread, NULL);
  return 0;
}

void WatDHTServer::set_rpc_server(TThreadedServer* server) {
  rpc_server = server;
  wat_state.change_state(WatDHT::SERVER_CREATED);
}

void* WatDHTServer::start_rpc_server(void* param) {
  WatDHTServer* dht = static_cast<WatDHTServer*>(param);
  shared_ptr<WatDHTHandler> handler(new WatDHTHandler(dht));
  shared_ptr<TProcessor> processor(new WatDHTProcessor(handler)); 
  shared_ptr<TServerTransport> serverTransport(
      new TServerSocket(dht->get_port()));
  shared_ptr<TTransportFactory> transportFactory(
      new TBufferedTransportFactory());
  shared_ptr<TProtocolFactory> protocolFactory(new TBinaryProtocolFactory());
  shared_ptr<ThreadManager> threadManager = 
      ThreadManager::newSimpleThreadManager(num_rpc_threads, 0);
  shared_ptr<PosixThreadFactory> threadFactory = 
      shared_ptr<PosixThreadFactory>(new PosixThreadFactory());
  threadManager->threadFactory(threadFactory);
  threadManager->start();
  TThreadedServer* server = new TThreadedServer(
      processor, serverTransport, transportFactory, protocolFactory);
  dht->set_rpc_server(server);
  server->serve();
  return NULL;
}
} // namespace WatDHT

using namespace WatDHT;

int main(int argc, char **argv) {
  if (argc < 4) {
    printf("Usage: %s server_id ip_address port [ip_address port]\n", argv[0]);
    return -1;
  }
  try {
    // Create the DHT node with the given IP address and port.    
    WatDHTServer server(argv[1], argv[2], atoi(argv[3]));    
    // Join the DHT ring via the bootstrap node.  
    //if (argc >= 6 && server.test(argv[4], atoi(argv[5])) == -1) {
    std::vector<NodeID> _return;
    std::string ip = "";
    if (argc >= 6) {
    	server.join(_return, server.get_NodeID() , (ip+=argv[4]), atoi(argv[5]) );
    }

/*    // neighbour sets have been populated, call migrate_kv
    std::map<std::string, std::string> _return;
    NodeID it = server.predecessors.front();
    try {
    	server.migrate_kv(_return, server.get_NodeID().id, it.ip, it.port);
    } catch (WatDHTException e) {
		std::cout << "Caught exception: " << e.error_message << std::endl;
	}
*/
    server.wait(); // Wait until server shutdown.
  } catch (int rc) {
    printf("Caught exception %d, exiting\n", rc);
    return -1;
  }
  return 0;
}
