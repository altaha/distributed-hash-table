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

#define BUCKET_1 0x8
#define BUCKET_2 0x4
#define BUCKET_3 0x2
#define BUCKET_4 0x1

class WatDHTServer {
 public:
  WatDHTServer(const char* id, const char* ip, int port) throw (int);  
  ~WatDHTServer();
  
  WatDHTState wat_state;    // Tracks the current state of the node.

  std::vector< std::list<NodeID>* > pappa_list;
  std::list<NodeID> predecessors, successors, rtable;

  std::map<std::string,std::string> hash_table;
  std::map<std::string,long> stale_table;
  pthread_rwlock_t rt_mutex, hash_mutex;

  int  test(const char* ip, int port);

  // Block and wait until the server shutdowns.
  int wait();
  // Set the RPC server once it is created in a child thread.
  void set_rpc_server(apache::thrift::server::TThreadedServer* server);
  
  const std::string& get_ipaddr() { return server_node_id.ip; }
  int get_port() { return server_node_id.port; }
  const WatID& get_id() { return wat_id; } 
  const NodeID& get_NodeID() { return server_node_id; }
  State get_state() { return wat_state.check_state(); }

  void run_gossip_neighbors();
  void run_maintain();
  bool find_bucket(NodeID& _dest, const ushort& bucket);
  void find_closest(NodeID& _dest, const std::string& key, bool cw);
  bool isOwner(const std::string& key);
  void genWatID(WatID _return, const ushort& bucket);

  void erase_node(const NodeID& ers);

  void update_connections(const std::vector<NodeID>& input, bool ping_nodes);
  void update_connections(const NodeID& input, bool ping_nodes);

  //RPC functions
  void get(std::string& _return, const std::string& key, std::string ip, int port);
  void put(const std::string& key, const std::string& val, const int32_t duration, std::string ip, int port);
  void join(std::vector<NodeID>& _return, const NodeID& nid, std::string ip, int port);
  void migrate_kv(std::map<std::string, std::string>& _return, const std::string& nid,
		  std::string ip, int port);
  void maintain(std::vector<NodeID> & _return, const std::string& id, const NodeID& nid,
		  std::string ip, int port);
  void gossip_neighbors(std::vector<NodeID> & _return, const NodeID& nid,
          const std::vector<NodeID> & neighbors, std::string ip, int port);
  bool ping(std::string ip, int port);
  void closest_node_cr(NodeID& _return, const std::string& id, std::string ip, int port);
  void closest_node_ccr(NodeID& _return, const std::string& id, std::string ip, int port);
  //void forward_join(std::vector<NodeID> & _return, const NodeID& nid, std::string ip, int port);

 private:
  WatID wat_id;             // This node's ID on the DHT.
  NodeID server_node_id;    // Include the ID, IP address and port.
  apache::thrift::server::TThreadedServer* rpc_server;
  pthread_t rpc_thread;
  static const int num_rpc_threads = 64;
  static void* start_rpc_server(void* param);

  void do_update(std::list<NodeID>& sorted, bool ping_nodes);

  long start_time;

};
} // namespace WatDHT

#endif
