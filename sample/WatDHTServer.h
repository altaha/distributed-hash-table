#ifndef _WAT_DHT_SERVER_H_
#define _WAT_DHT_SERVER_H_

#include "WatDHT.h"
#include "WatID.h"
#include "WatDHTState.h"
#include <pthread.h>
#include <string>
#include <list>
#include <map>
#include <pthread.h>
#include <thrift/server/TThreadedServer.h>

namespace WatDHT {

class WatDHTServer {
 public:
  WatDHTServer(const char* id, const char* ip, int port) throw (int);  
  ~WatDHTServer();
  
  std::list<NodeID> predecessors, successors, rtable;
  std::map<std::string,std::string> hash_table;
  pthread_rwlock_t rt_mutex, hash_mutex;

  // Join the DHT network
  int join(const char* ip, int port);
  // Block and wait until the server shutdowns.
  int wait();
  // Set the RPC server once it is created in a child thread.
  void set_rpc_server(apache::thrift::server::TThreadedServer* server);
  
  const std::string& get_ipaddr() { return server_node_id.ip; }
  int get_port() { return server_node_id.port; }
  const WatID& get_id() { return wat_id; } 
  const NodeID& get_NodeID() { return server_node_id; }
  State get_state() { return wat_state.check_state(); }
  
  void get(std::string& _return, const std::string& key, std::string ip, int port);
  void put(const std::string& key, const std::string& val, const int32_t duration, std::string ip, int port);
  void forward_join(std::vector<NodeID> & _return, const NodeID& nid, std::string ip, int port);
  void find_closest(const std::string& key, NodeID& _dest);
  bool isOwner(const std::string& key);

 private:
  WatID wat_id;             // This node's ID on the DHT.
  NodeID server_node_id;    // Include the ID, IP address and port.
  apache::thrift::server::TThreadedServer* rpc_server;
  pthread_t rpc_thread;
  WatDHTState wat_state;    // Tracks the current state of the node.
  static const int num_rpc_threads = 64;
  static void* start_rpc_server(void* param);
};
} // namespace WatDHT

#endif
