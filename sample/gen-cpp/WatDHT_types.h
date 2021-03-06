/**
 * Autogenerated by Thrift Compiler (0.7.0)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 */
#ifndef WatDHT_TYPES_H
#define WatDHT_TYPES_H

#include <Thrift.h>
#include <TApplicationException.h>
#include <protocol/TProtocol.h>
#include <transport/TTransport.h>



namespace WatDHT {

struct WatDHTErrorType {
  enum type {
    KEY_NOT_FOUND = 0,
    INCORRECT_MIGRATION_SOURCE = 1,
    OL_MIGRATION_IN_PROGRESS = 2
  };
};

extern const std::map<int, const char*> _WatDHTErrorType_VALUES_TO_NAMES;

typedef struct _NodeID__isset {
  _NodeID__isset() : id(false), ip(false), port(false) {}
  bool id;
  bool ip;
  bool port;
} _NodeID__isset;

class NodeID {
 public:

  static const char* ascii_fingerprint; // = "343DA57F446177400B333DC49B037B0C";
  static const uint8_t binary_fingerprint[16]; // = {0x34,0x3D,0xA5,0x7F,0x44,0x61,0x77,0x40,0x0B,0x33,0x3D,0xC4,0x9B,0x03,0x7B,0x0C};

  NodeID() : id(""), ip(""), port(0) {
  }

  virtual ~NodeID() throw() {}

  std::string id;
  std::string ip;
  int32_t port;

  _NodeID__isset __isset;

  void __set_id(const std::string& val) {
    id = val;
  }

  void __set_ip(const std::string& val) {
    ip = val;
  }

  void __set_port(const int32_t val) {
    port = val;
  }

  bool operator == (const NodeID & rhs) const
  {
    if (!(id == rhs.id))
      return false;
    if (!(ip == rhs.ip))
      return false;
    if (!(port == rhs.port))
      return false;
    return true;
  }
  bool operator != (const NodeID &rhs) const {
    return !(*this == rhs);
  }

  bool operator < (const NodeID & ) const;

  uint32_t read(::apache::thrift::protocol::TProtocol* iprot);
  uint32_t write(::apache::thrift::protocol::TProtocol* oprot) const;

};

typedef struct _WatDHTException__isset {
  _WatDHTException__isset() : error_code(false), error_message(false), node(false) {}
  bool error_code;
  bool error_message;
  bool node;
} _WatDHTException__isset;

class WatDHTException : public ::apache::thrift::TException {
 public:

  static const char* ascii_fingerprint; // = "D2FE1444C67A4165702A6F29860D57AA";
  static const uint8_t binary_fingerprint[16]; // = {0xD2,0xFE,0x14,0x44,0xC6,0x7A,0x41,0x65,0x70,0x2A,0x6F,0x29,0x86,0x0D,0x57,0xAA};

  WatDHTException() : error_message("") {
  }

  virtual ~WatDHTException() throw() {}

  WatDHTErrorType::type error_code;
  std::string error_message;
  NodeID node;

  _WatDHTException__isset __isset;

  void __set_error_code(const WatDHTErrorType::type val) {
    error_code = val;
  }

  void __set_error_message(const std::string& val) {
    error_message = val;
  }

  void __set_node(const NodeID& val) {
    node = val;
    __isset.node = true;
  }

  bool operator == (const WatDHTException & rhs) const
  {
    if (!(error_code == rhs.error_code))
      return false;
    if (!(error_message == rhs.error_message))
      return false;
    if (__isset.node != rhs.__isset.node)
      return false;
    else if (__isset.node && !(node == rhs.node))
      return false;
    return true;
  }
  bool operator != (const WatDHTException &rhs) const {
    return !(*this == rhs);
  }

  bool operator < (const WatDHTException & ) const;

  uint32_t read(::apache::thrift::protocol::TProtocol* iprot);
  uint32_t write(::apache::thrift::protocol::TProtocol* oprot) const;

};

} // namespace

#endif
