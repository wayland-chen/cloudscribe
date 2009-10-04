#include <sstream>
#include <iostream>
#include <iomanip>
#include <fstream>
#include <string>
#include <queue>
#include <vector>
#include <pthread.h>
#include <semaphore.h>
#include <map>
#include <set>
#include <stdexcept>
#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <getopt.h>
#include <sys/resource.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <boost/shared_ptr.hpp>
#include <boost/filesystem/operations.hpp>

#include "thrift/protocol/TBinaryProtocol.h"
#include "thrift/server/TNonblockingServer.h"
#include "thrift/transport/TSocket.h"
#include "thrift/transport/TSocketPool.h"
#include "thrift/transport/TServerSocket.h"
#include "thrift/transport/TTransportUtils.h"
#include "thrift/transport/THttpClient.h"
#include "thrift/transport/TFileTransport.h"

#include "scribe/gen-cpp/forwarder.h"


using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;
using namespace apache::thrift::concurrency;

using namespace forwarder::thrift;
using namespace std;
using boost::shared_ptr;



int main(int argc, char **argv) {
	if(argc != 3) {
		fprintf(stderr, "Usage: forwarder_client host, port");
		exit(1);
	}

	char *host = argv[1];
	int port = atoi(argv[2]);
	printf("dist: %s:%d\n", host, port);
	shared_ptr<TTransport> socket(new TSocket(host, port));
        shared_ptr<TFramedTransport> framedTransport(new TFramedTransport(socket));
        if(!framedTransport) {
                throw std::runtime_error("Failed to create framed transport");
        }

        shared_ptr<TProtocol> protocol(new TBinaryProtocol(framedTransport));
        forwarderClient client(protocol);
        try {
                framedTransport->open();

                timeval bwstart, bwend;

                gettimeofday(&bwstart, NULL);

		ResultCode Log(const std::vector<LogEntry> & messages);
                for(int idx=0;idx<2048;++idx) {
                        vector<LogEntry> messages;
                        for(int i=0;i<200;++i) {
				LogEntry message;
				message.category = "test";
				message.message = "1234567890123456789012345678901234567890123456789012345678901234567890";
				messages.push_back(message);
                        }
                        ResultCode rc = client.Log(messages);
//			printf("rcode: %d\n", rc);
                }


                gettimeofday(&bwend,NULL);
                printf("duration:%lu ms\n", (bwend.tv_sec*1000 +bwend.tv_usec/1000) - (bwstart.tv_sec*1000 + bwstart.tv_usec/1000));

                framedTransport->close();
        } catch (TException &tx) {
                printf("ERROR: %s\n", tx.what());
        }

	return 0;
}
