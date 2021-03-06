#ifndef _WAT_DHT_STATE_H_
#define _WAT_DHT_STATE_H_

#include <pthread.h>

namespace WatDHT {

// Add more states as needed.
enum State {INIT, SERVER_CREATED, MIGRATE_KV, NODE_READY};

class WatDHTState {
 public:

  WatDHTState();
  ~WatDHTState();
  // Change the internal state of WatDHTState.
  void change_state(State state);  
  // Wait until state equals the parameter.
  void wait_e(State state);
  // Wait until state is greater than or equal to the parameter.
  void wait_ge(State state);
  // Check current state without blocking
  State check_state(void);

 private:
  State dht_state;
  pthread_cond_t state_change;
  pthread_mutex_t wait_on_state;
};
} // namespace WatDHT

#endif
