// This autogenerated skeleton file illustrates how to build a server.
// You should copy it to another filename to avoid overwriting it.

#include "WatDHT.h"
#include <protocol/TBinaryProtocol.h>
#include <server/TSimpleServer.h>
#include <transport/TServerSocket.h>
#include <transport/TBufferTransports.h>

using namespace ::apache::thrift;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;
using namespace ::apache::thrift::server;

using boost::shared_ptr;

using namespace WatDHT;

class WatDHTHandler : virtual public WatDHTIf {
 public:
  WatDHTHandler() {
    // Your initialization goes here
  }

  void get(std::string& _return, const std::string& key) {
    // Your implementation goes here
    printf("get\n");
  }

  void put(const std::string& key, const std::string& val, const int32_t duration) {
    // Your implementation goes here
    printf("put\n");
  }

  void join(std::vector<NodeID> & _return, const NodeID& nid) {
    // Your implementation goes here
    printf("join\n");
  }

  void ping(std::string& _return) {
    // Your implementation goes here
    printf("ping\n");
  }

  void maintain(std::vector<NodeID> & _return, const std::string& id, const NodeID& nid) {
    // Your implementation goes here
    printf("maintain\n");
  }

  void migrate_kv(std::map<std::string, std::string> & _return, const std::string& nid) {
    // Your implementation goes here
    printf("migrate_kv\n");
  }

  void gossip_neighbors(std::vector<NodeID> & _return, const NodeID& nid, const std::vector<NodeID> & neighbors) {
    // Your implementation goes here
    printf("gossip_neighbors\n");
  }

  void closest_node_cr(NodeID& _return, const std::string& id) {
    // Your implementation goes here
    printf("closest_node_cr\n");
  }

  void closest_node_ccr(NodeID& _return, const std::string& id) {
    // Your implementation goes here
    printf("closest_node_ccr\n");
  }

};

int main(int argc, char **argv) {
  int port = 9090;
  shared_ptr<WatDHTHandler> handler(new WatDHTHandler());
  shared_ptr<TProcessor> processor(new WatDHTProcessor(handler));
  shared_ptr<TServerTransport> serverTransport(new TServerSocket(port));
  shared_ptr<TTransportFactory> transportFactory(new TBufferedTransportFactory());
  shared_ptr<TProtocolFactory> protocolFactory(new TBinaryProtocolFactory());

  TSimpleServer server(processor, serverTransport, transportFactory, protocolFactory);
  server.serve();
  return 0;
}
