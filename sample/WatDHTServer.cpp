#include "WatDHTServer.h"
#include "WatDHT.h"
#include "WatID.h"
#include "WatDHTState.h"
#include "WatDHTHandler.h"
#include <time.h>
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
		printf( "%d, ", it->port);
	}
	printf("\n");
}
void printVector(std::vector<NodeID>& in){
	for( std::vector<NodeID>::iterator it=in.begin(); it!=in.end(); it++ ){
		printf( "%d, ", it->port);
	}
	printf("\n");
}

void WatDHTServer::printConnections(){
	printf("\nPredecessors...");
	printList(this->predecessors);
	printf("Successors...");
	printList(this->successors);
	printf("Routing Table...");
	printList(this->rtable);
	printf("\n");
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

void WatDHTServer::populate_hash_table() {
       char y[MD5_DIGEST_LENGTH], x[MD5_DIGEST_LENGTH];
       WatID key;//
       for (int i=50; i<60; i++) {
               sprintf(y, "%d", i);
               key.set_using_md5(y);
               sprintf(x, "%d", i);
               pthread_rwlock_wrlock(&hash_mutex);
               hash_table.insert(std::pair<std::string, std::string>(key.to_string(), x));
               pthread_rwlock_unlock(&hash_mutex);
       }
}

void WatDHTServer::print_hash_table()
{
	printf("In my hash table:\n");
	pthread_rwlock_rdlock(&hash_mutex);
   WatID toprint;
   for (std::map<std::string, std::string>::iterator it = \
		   	   hash_table.begin(); it!=hash_table.end(); it++){
	   toprint.copy_from(it->first);
	   toprint.debug_md5();
	   printf(" => %s\n", it->second.c_str());
   }
   pthread_rwlock_unlock(&hash_mutex);
   printf("\n");
}


WatDHTServer::WatDHTServer(const char* id, 
                           const char* ip, 
                           int port) throw (int) : rpc_server(NULL) {
  wat_id.set_using_md5(id);
  wat_id.debug_md5();
  server_node_id.id = wat_id.to_string();
  server_node_id.ip = ip;
  server_node_id.port = port;

  start_time = time(NULL);

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

void WatDHTServer::run_gossip_neighbors()
{
	std::vector<NodeID> _return, neighbors;
	NodeID nid = server_node_id, _dest, _closest;
	pthread_rwlock_rdlock(&rt_mutex);
	neighbors.insert(neighbors.begin(), predecessors.begin(), predecessors.end());
	neighbors.insert(neighbors.end(), successors.begin(), successors.end());
	pthread_rwlock_unlock(&rt_mutex);

	for (uint i=0; i<neighbors.size(); i++) {
		if (!gossip_neighbors(_return, nid, neighbors, neighbors[i].ip, neighbors[i].port)){
			erase_node(neighbors[i]);
		}
	}

	pthread_rwlock_rdlock(&rt_mutex);
	if (successors.empty()) {
		pthread_rwlock_unlock(&rt_mutex);
		if (find_closest(_dest, get_NodeID().id, false)){
			closest_node_cr(_closest, get_NodeID().id, _dest.ip, _dest.port);
		}
	} else {
		pthread_rwlock_unlock(&rt_mutex);
	}

	pthread_rwlock_rdlock(&rt_mutex);
	if (predecessors.empty()) {
		pthread_rwlock_unlock(&rt_mutex);
		if (find_closest(_dest, get_NodeID().id, true)) {
			closest_node_ccr(_closest, get_NodeID().id, _dest.ip, _dest.port);
		}
	} else {
		pthread_rwlock_unlock(&rt_mutex);
	}
}

void WatDHTServer::run_maintain()
{
	ushort i = 0;
	NodeID _dest;
	WatID _key;

	pthread_rwlock_rdlock(&rt_mutex);
	int size = successors.size() + predecessors.size()+ rtable.size();
	pthread_rwlock_unlock(&rt_mutex);

	if(size==0){
		return;
	}
	std::string lastFound = "";

	std::vector<NodeID> _return;
	for (i=0; i<4; i++){
		if ( find_bucket(_dest, i) ) //node existed in routing table
		{
			if (!ping(_dest.ip, _dest.port)) {// but has died since last check
				erase_node(_dest);
				genWatID(_key,i);
				if( find_closest(_dest, _key.to_string(), true) ){
					if(_dest.id != lastFound){
						lastFound = _dest.id;
						maintain(_return, _key.to_string(), this->server_node_id, _dest.ip, _dest.port);
					}
				}
			}
		}
		else { // no node in routing table for bucket
			genWatID(_key,i);
			if( find_closest(_dest, _key.to_string(), true) ){
				if(_dest.id != lastFound){
					lastFound = _dest.id;
					maintain(_return, _key.to_string(), this->server_node_id, _dest.ip, _dest.port);
				}
			}
		}
	}
}

void WatDHTServer::genWatID(WatID _return, const ushort& bucket)
{
	unsigned char key[MD5_DIGEST_LENGTH+1];
	for (int i=0; i<MD5_DIGEST_LENGTH; i++) {
		key[i] = (unsigned char)(255);
	}
	key[MD5_DIGEST_LENGTH] = '\0';

	unsigned char x = this->get_NodeID().id[0];
	x ^= 1<<(7-bucket);		// invert bit matching required bucket
	x |= 255>>(bucket+1);	// set bits right of bucket-th bit to 1
	key[0] = x;

	_return.copy_from(std::string(key));
}

bool WatDHTServer::find_bucket(NodeID& _dest, const ushort& bucket)
{
	WatID curr;
	pthread_rwlock_rdlock(&rt_mutex);
	for (std::list<NodeID>::iterator it = rtable.begin(); it!=rtable.end(); it++) {
		curr.copy_from(it->id);
		if (curr.hmatch_bin(wat_id,1)==bucket){
			_dest = *it;
			pthread_rwlock_unlock(&rt_mutex);
			return true;
		}
	}
	pthread_rwlock_unlock(&rt_mutex);
	return false;
}
void WatDHTServer::update_connections(const NodeID& input, bool ping_nodes)
{
	std::list<NodeID> sorted;
	sorted.push_back(input);
	sorted.remove(this->server_node_id);
	if(!sorted.empty()){
		do_update(sorted, ping_nodes);
	}
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
#ifdef VERBOSE_DEBUG
	printf("Printing what has been received...\n");
	printList(sorted);
#endif

	if(!sorted.empty()){
		do_update(sorted, ping_nodes);
	}
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

	//merge successors, predecessors, and rtable with sorted (use insertion sort)
	for (uint i=0; i<pappa_list.size(); i++) {
		it = pappa_list[i]->begin();
		while(it!=pappa_list[i]->end())
		{
			insSorted(sorted, (*it), this->wat_id);
			it = pappa_list[i]->erase(it);
		}
	}
	sorted.unique();

	// Update neighbour lists first
	uint i = 0;
	while(i<4 && !sorted.empty()){
		if(!(i&1)){ //even: add to successors
			successors.push_back(sorted.front());
			sorted.pop_front();
		}else{
			predecessors.push_back(sorted.back());
			sorted.pop_back();
		}
		i++;
	}

	//HANDLE routing table updates
	uint rt_buckets=0;
	WatID bucket;
	it=sorted.begin();
	while( it!= sorted.end() && rt_buckets != 0xf ){
		bool add_rt = true;
		bucket.copy_from(it->id);
		int rc = this->wat_id.hmatch_bin(bucket,1);
		if(rc==-1 && !(rt_buckets & BUCKET_1) ){ //1st MSB
			rt_buckets |= BUCKET_1;
		}
		else if( rc==0 && !(rt_buckets & BUCKET_2) ){ //2nd MSB
			rt_buckets |= BUCKET_2;
		}
		else if( rc==1 && !(rt_buckets & BUCKET_3) ){ //3rd MSB
			rt_buckets |= BUCKET_3;
		}
		else if( rc==2 && !(rt_buckets & BUCKET_4) ){ //4th MSB
			rt_buckets |= BUCKET_4;
		}
		else{
			add_rt = false;
		}
		if(add_rt){ rtable.push_back(*(it)); }
		it++;
	}
	pthread_rwlock_unlock(&rt_mutex);
}

// Join the DHT network and wait
bool WatDHTServer::join(std::vector<NodeID>& _return, const NodeID& nid, std::string ip, int port)
{
	wat_state.wait_ge(WatDHT::SERVER_CREATED);
#ifdef VERBOSE_DEBUG
	printf("Client join\n");
#endif

	boost::shared_ptr<TSocket> socket(new TSocket(ip, port));
	boost::shared_ptr<TTransport> transport(new TBufferedTransport(socket));
	boost::shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
	WatDHTClient client(protocol);

	if(nid == this->server_node_id){ //initate join
		this->wat_state.change_state(MIGRATE_KV);
	}

	try {
		transport->open();
		client.join(_return, nid);
		transport->close();

	} catch (TTransportException e) {
		printf("Caught exception: %s\n", e.what());
		return false;
	}
	if(nid == this->server_node_id) //join initiator
	{
		//use return vector to populate neighbour set
		update_connections(_return, true);
		printConnections();
	}
	else { //forward join
		update_connections(nid, false);
		_return.push_back(server_node_id); 	//add my nodeID to return
	}

	return true;
}


void WatDHTServer::migrate_kv(std::map<std::string, std::string>& _return, const std::string& nid,
		  std::string ip, int port)
{
#ifdef VERBOSE_DEBUG
	printf("Migrate_Kv client start\n");
#endif

	boost::shared_ptr<TSocket> socket(new TSocket(ip, port));
	boost::shared_ptr<TTransport> transport(new TBufferedTransport(socket));
	boost::shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
	WatDHTClient client(protocol);
	static uint sleepTime;
	try {
		transport->open();
		try {
			client.migrate_kv(_return, nid);
		} catch (WatDHTException e) {
			std::cout << "Caught exception: " << e.error_message << std::endl;

			if (e.error_code==WatDHTErrorType::INCORRECT_MIGRATION_SOURCE){
				update_connections(e.node, true);
				NodeID pred;
				if(!find_closest(pred, get_NodeID().id, false)) { //find predecessor. if not available, just call node returned from exception
					pred = e.node;
				}
				migrate_kv(_return, pred.id, pred.ip, pred.port);
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

#ifdef VERBOSE_DEBUG
	printf("Migrate_Kv client end\n");
#endif
}

void WatDHTServer::init_get(std::string& _return, const std::string& key, std::string ip, int port){
	try {
		this->get(_return, key, ip,port);
	} catch (WatDHTException e1) {
		_return = "";
		std::cout << "Caught exception: " << e1.error_message << std::endl;
	}
}

bool WatDHTServer::get(std::string& _return, const std::string& key, std::string ip, int port)
{
#ifdef VERBOSE_DEBUG
	printf("Client get start\n");
#endif

	boost::shared_ptr<TSocket> socket(new TSocket(ip, port));
	boost::shared_ptr<TTransport> transport(new TBufferedTransport(socket));
	boost::shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
	WatDHTClient client(protocol);
	try {
		transport->open();
		try {
			client.get(_return, key);
		} catch (WatDHTException e1) {
			//_return = "";
			//std::cout << "Caught exception: " << e1.error_message << std::endl;
			throw e1;
		}
		transport->close();
	} catch (TTransportException e) {
		printf("Caught exception: %s\n", e.what());
		return false;
	}

#ifdef VERBOSE_DEBUG
	printf("Client get end\n");
#endif
	return true;
}

bool WatDHTServer::put(const std::string& key, const std::string& val, const int32_t duration, std::string ip, int port)
{
#ifdef VERBOSE_DEBUG
	printf("Client put start\n");
#endif

	boost::shared_ptr<TSocket> socket(new TSocket(ip, port));
	boost::shared_ptr<TTransport> transport(new TBufferedTransport(socket));
	boost::shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
	WatDHTClient client(protocol);
	try {
		transport->open();
		client.put(key, val, duration);
		transport->close();
	} catch (TTransportException e) {
		printf("Caught exception: %s\n", e.what());
		return false;
	}

#ifdef VERBOSE_DEBUG
	printf("Client put end\n");
#endif
	return true;
}

bool WatDHTServer::find_closest(NodeID& _dest, const std::string& key, bool cw)
{
	//find node in neighbour set and routing table that is closest in distance to key
	std::list<NodeID>::iterator it, closest;
	WatID toFind, curr, temp, closestDist;

	unsigned char maxDist[MD5_DIGEST_LENGTH+1];
	for (int i=0; i<MD5_DIGEST_LENGTH; i++) {
		maxDist[i] = (unsigned char)(255);
	}
	maxDist[MD5_DIGEST_LENGTH] = '\0';
	closestDist.copy_from(std::string(maxDist)); // initializing to a null WatID -- test correctness
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

	temp.copy_from(std::string(maxDist));
	if ( closestDist==temp ) { //don't have any nodes in my lists to return
		return false;
	}
	_dest = *closest;
	return true;
}

bool WatDHTServer::maintain(std::vector<NodeID> & _return, const std::string& id,
                             const NodeID& nid, std::string ip, int port)
{
#ifdef VERBOSE_DEBUG
	printf("Client maintain start\n");
#endif

	boost::shared_ptr<TSocket> socket(new TSocket(ip, port));
	boost::shared_ptr<TTransport> transport(new TBufferedTransport(socket));
	boost::shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
	WatDHTClient client(protocol);
	try {
		transport->open();
		client.maintain(_return, id, nid);
		transport->close();
	} catch (TTransportException e) {
		printf("Caught exception: %s\n", e.what());
		return false;
	}
	if(nid == this->server_node_id){ // If i'm the initiator
		update_connections(_return,true);
	}
#ifdef VERBOSE_DEBUG
	printf("Client maintain end\n");
#endif
	return true;
}

bool WatDHTServer::gossip_neighbors(std::vector<NodeID> & _return, const NodeID& nid,
        const std::vector<NodeID> & neighbors, std::string ip, int port)
{
#ifdef VERBOSE_DEBUG
	printf("Client gossip_neighbours start\n");
#endif

	boost::shared_ptr<TSocket> socket(new TSocket(ip, port));
	boost::shared_ptr<TTransport> transport(new TBufferedTransport(socket));
	boost::shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
	WatDHTClient client(protocol);
	try {
		transport->open();
		client.gossip_neighbors(_return, nid, neighbors);
		transport->close();
	} catch (TTransportException e) {
		printf("Caught exception: %s\n", e.what());
		return false;
	}

	update_connections(_return, true);
#ifdef VERBOSE_DEBUG
	printf("Client gossip_neighbours end\n");
#endif
	return true;
}

bool WatDHTServer::ping(std::string ip, int port)
{
#ifdef VERBOSE_DEBUG
	printf("Client pinging\n");
#endif

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
#ifdef VERBOSE_DEBUG
			printf("Ping received invalid ID\n");
#endif
			return false;
		} else {
#ifdef VERBOSE_DEBUG
			printf("Ping received:\n");
			remote_id.debug_md5();
#endif
		}
	} catch (TTransportException e) {
		printf("Caught exception: %s\n", e.what());
		return false;
	}
	return true;
}

bool WatDHTServer::closest_node_cr(NodeID& _return, const std::string& id, std::string ip, int port)
{
#ifdef VERBOSE_DEBUG
	printf("Client closest_node_cr start\n");
#endif

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
		return false;
	}
	if(id == this->server_node_id.id){ // If i'm the initiator
		update_connections(_return, true);
	}

#ifdef VERBOSE_DEBUG
	printf("Client closest_node_cr end\n");
#endif
	return true;
}

bool WatDHTServer::closest_node_ccr(NodeID& _return, const std::string& id, std::string ip, int port)
{
#ifdef VERBOSE_DEBUG
	printf("Client closest_node_ccr start\n");
#endif


	boost::shared_ptr<TSocket> socket(new TSocket(ip, port));
	boost::shared_ptr<TTransport> transport(new TBufferedTransport(socket));
	boost::shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
	WatDHTClient client(protocol);
	try {
		transport->open();
		client.closest_node_ccr(_return, id);
		transport->close();
	} catch (TTransportException e) {
		printf("Caught exception: %s\n", e.what());
		return false;
	}

	if(id == this->server_node_id.id){ // If i'm the initiator
		update_connections(_return, true);
	}

#ifdef VERBOSE_DEBUG
	printf("Client closest_node_ccr end\n");
#endif
	return true;
}

bool WatDHTServer::isOwner(const std::string& key)
{
	WatID toFind, closest;
	toFind.copy_from(key);
	pthread_rwlock_rdlock(&rt_mutex);
	if (!successors.empty()) {
		closest.copy_from(successors.begin()->id);
	}
	else{ //must search through predecessors and rtable
		std::list<NodeID>::iterator it;
		for(uint i=1; i < pappa_list.size(); i++){
			for(it=pappa_list[i]->begin(); it!=pappa_list[i]->end(); it++){
				WatID testID;
				testID.copy_from(it->id);
				if( this->wat_id.distance_cr(testID) < this->wat_id.distance_cr(toFind) ){
					pthread_rwlock_unlock(&rt_mutex);
					return false;
				}
			}
		}
		pthread_rwlock_unlock(&rt_mutex);
		return true;
	}
	pthread_rwlock_unlock(&rt_mutex);
	return ( this->wat_id.distance_cr(closest) < this->wat_id.distance_cr(toFind) ) ? false : true;

	/*WatID test1 = this->wat_id.distance_cr(toFind);
	WatID test2 = this->wat_id.distance_cr(closest);
	if(test1 <= test2){
		return true;
	}
	else{
		return true;
	}*/
}

void WatDHTServer::erase_node(const NodeID& ers)
{
	pthread_rwlock_wrlock(&rt_mutex);
	successors.remove(ers);
	predecessors.remove(ers);
	rtable.remove(ers);
	pthread_rwlock_unlock(&rt_mutex);
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
			server.join(_return, server.get_NodeID(), (ip+=argv[4]), atoi(argv[5]));
			// Initialization Routines
			NodeID it;
			if (!server.find_closest(it, server.get_NodeID().id, true)) {
				server.wat_state.change_state(NODE_READY);
			} else {
				server.migrate_kv(server.hash_table, server.get_NodeID().id, it.ip, it.port);
				server.wat_state.change_state(NODE_READY);
				server.run_gossip_neighbors();
				server.run_maintain();

				WatID putty;
				char x[16];
				sprintf(x, "%d",server.get_NodeID().port );
				putty.set_using_md5( x );

				server.put(putty.to_string(), "baller", -1, it.ip, it.port );
			}
		}else{
			server.wat_state.wait_ge(SERVER_CREATED);
			server.wat_state.change_state(NODE_READY);
			server.populate_hash_table();
			server.print_hash_table();
		}

		// set periods for maintain and gossip independently
		int gossip_period = 10, maintain_period = 30;
		int gossip_elapsed =0, maintain_elapsed =0;
		// Regular Maintenance Schedule
		while(true){
			sleep(1);
			gossip_elapsed++;
			maintain_elapsed++;
			if(gossip_elapsed==gossip_period){
				server.run_gossip_neighbors();
				gossip_elapsed = 0;
				printf("Periodic gossip neighbours");
				server.printConnections();
				server.print_hash_table();
			}
			if(maintain_elapsed==maintain_period){
				server.run_maintain();
				maintain_elapsed = 0;
				printf("Periodic maintain");
				server.printConnections();
			}
		}
		server.wait(); // Wait until server shutdown.
	} catch (int rc) {
		printf("Caught exception %d, exiting\n", rc);
		return -1;
	}
	return 0;
}
