/**
 * 公共的include文件
 * @author edisonpeng@tencent.com
 */
#ifndef FORWARDER_COMMON_H
#define FORWARDER_COMMON_H

/*
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <semaphore.h>
#include <errno.h>
#include <stdint.h>
#include <getopt.h>
#include <unistd.h>
#include <signal.h>
#include <fcntl.h>
#include <sys/resource.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/socket.h>
#include <sys/ioctl.h>
#include <sys/signal.h>
#include <netinet/in.h>
#include <net/if.h>
#include <netinet/in.h>
#include <netdb.h>
#include <linux/sockios.h>
#include <arpa/inet.h>
*/

/*
#include <sstream>
#include <iostream>
#include <iomanip>
#include <fstream>
#include <stdexcept>
*/
/*
#include "thrift/protocol/TBinaryProtocol.h"
#include "thrift/server/TNonblockingServer.h"
#include "thrift/transport/TSocket.h"
#include "thrift/transport/TSocketPool.h"
#include "thrift/transport/TServerSocket.h"
#include "thrift/transport/TTransportUtils.h"
#include "thrift/transport/THttpClient.h"
#include "thrift/transport/TFileTransport.h"
*/

#include <vector>
#include <string>

#include <boost/shared_ptr.hpp>

#include "gen-cpp/forwarder.h"



typedef boost::shared_ptr<forwarder::thrift::LogEntry> logentry_ptr_t;
typedef std::vector<logentry_ptr_t> logentry_vector_t;
typedef std::vector<std::pair<std::string, int> > server_vector_t;

//#include "env_default.h"



#endif // !defined FORWARDER_COMMON_H
