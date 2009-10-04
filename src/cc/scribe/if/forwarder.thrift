#!/opt/thrift/bin/thrift --cpp --php

include "cloudxbase.thrift"

namespace cpp forwarder.thrift

enum ResultCode {
  OK,
  TRY_LATER
}

struct LogEntry {
  1:  string category,
  2:  string message
}

service forwarder extends cloudxbase.CloudxService {
  ResultCode Log(1: list<LogEntry> messages);
}
