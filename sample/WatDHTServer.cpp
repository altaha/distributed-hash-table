#include "WatDHTServer.h"
#include "WatDHT.h"
#include "WatID.h"
#include "WatDHTState.h"
#include "WatDHTHandler.h"
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

WatDHTServer::WatDHTServer(const char* id, 
                           const char* ip, 
                           int port) throw (int) : rpc_server(NULL) {
  wat_id.set_using_md5(id);
  wat_id.debug_md5();
  server_node_id.id = wat_id.to_string();
  server_node_id.ip = ip;
  server_node_id.port = port;
  rt_mutex = hash_mutex = PTHREAD_RWLOCK_INITIALIZER;

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

// Join the DHT network and wait
void WatDHTServer::join(std::vector<NodeID>& _return, const NodeID& nid, std::string ip, int port) {
  wat_state.wait_ge(WatDHT::SERVER_CREATED);

  boost::shared_ptr<TSocket> socket(new TSocket(ip, port));
  boost::shared_ptr<TTransport> transport(new TBufferedTransport(socket));
  boost::shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
  WatDHTClient client(protocol);
  try {
    transport->open();
    std::string remote_str;
    client.join(_return, nid);
    transport->close();
  } catch (TTransportException e) {
    printf("Caught exception: %s\n", e.what());
  }
  //**MISSING** use return vector to populate neighbour set
  this->wat_state.change_state(MIGRATE_KV);
}

void WatDHTServer::migrate_kv(std::map<std::string, std::string>& _return, const std::string& nid,
		  std::string ip, int port)
{
	boost::shared_ptr<TSocket> socket(new TSocket(ip, port));
	boost::shared_ptr<TTransport> transport(new TBufferedTransport(socket));
	boost::shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
	WatDHTClient client(protocol);
	try {
		transport->open();
		std::string remote_str;
		client.migrate_kv(_return, nid);
		transport->close();
	} catch (TTransportException e) {
		printf("Caught exception: %s\n", e.what());
	}
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

void WatDHTServer::forward_join(std::vector<NodeID> & _return, const NodeID& nid, std::string ip, int port)
{
	boost::shared_ptr<TSocket> socket(new TSocket(ip, port));
	boost::shared_ptr<TTransport> transport(new TBufferedTransport(socket));
	boost::shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
	WatDHTClient client(protocol);
	try {
		transport->open();
		std::string remote_str;
		client.join(_return, nid);
		transport->close();
	} catch (TTransportException e) {
		printf("Caught exception: %s\n", e.what());
	}

	_return.push_back(server_node_id); 	//add my nodeID to return
}

void WatDHTServer::find_closest(const std::string& key, NodeID& _dest)
{
	//** MISSING: check if any node in neighbour set is owner of key **//
	//find node in neighbour set and routing table that is closest in distance to key
	std::list<NodeID>::iterator it, closest;
	WatID toFind, curr, temp, closestDist;
	closestDist.copy_from(""); // initializing to a null WatID -- test correctness
	toFind.copy_from(key);
	pthread_rwlock_rdlock(&rt_mutex);
	for (it=successors.begin(); it!=successors.end(); it++) {
		curr.copy_from(it->ip);
		temp = curr.distance(toFind);
		if (temp < closestDist) {
			closestDist = temp;
			closest = it;
		}
	}

	for (it=predecessors.begin(); it!=predecessors.end(); it++) {
		curr.copy_from(it->ip);
		temp = curr.distance(toFind);
		if (temp < closestDist) {
			closestDist = temp;
			closest = it;
		}
	}

	for (it=rtable.begin(); it!=rtable.end(); it++) {
		curr.copy_from(it->ip);
		temp = curr.distance(toFind);
		if (temp < closestDist) {
			closestDist = temp;
			closest = it;
		}
	}
	pthread_rwlock_unlock(&rt_mutex);

	_dest = *closest;
}

bool WatDHTServer::isOwner(const std::string& key)
{
	WatID toFind, successor;
	toFind.copy_from(key);
	successor.copy_from(successors.begin()->id);
	return ( this->wat_id.distance(toFind) < this->wat_id.distance(successor) ) ? true : false;
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
    if (argc >= 6 && server.test(argv[4], atoi(argv[5])) == -1) {
        printf("Unable to connect to join network, exiting\n"); 
        return -1;
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
