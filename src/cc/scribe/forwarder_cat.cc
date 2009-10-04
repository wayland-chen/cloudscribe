#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <error.h>
#include <pthread.h>
#include <semaphore.h>
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


#include <sstream>
#include <iostream>
#include <iomanip>
#include <fstream>
#include <stdexcept>

#include "thrift/protocol/TBinaryProtocol.h"
#include "thrift/server/TNonblockingServer.h"
#include "thrift/transport/TSocket.h"
#include "thrift/transport/TSocketPool.h"
#include "thrift/transport/TServerSocket.h"
#include "thrift/transport/TTransportUtils.h"
#include "thrift/transport/THttpClient.h"
#include "thrift/transport/TFileTransport.h"


#include "group_service.h"
#include "utils.h"
#include "common.h"



using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;
using namespace apache::thrift::concurrency;

using namespace forwarder::thrift;
using namespace std;
using boost::shared_ptr;

/* constants */
#define BATCH_LINES 1024

/* utilities */
#define STREQ(a, b) (strcmp (a, b) == 0)

#define TRACE(b,fmt, args...) do {              \
        if((b)) printf(fmt"\n",##args);         \
} while(0)


static void print_usage(const char *program) {
	printf("Usage: %s [OPTION] [FILE] ...\n", program);
	printf("	-d,	--delimeter		split key and message\n");
	printf("	-c,	--category		indicate that all messages own to this cateogry\n");
	printf("	-v,	--verbose		display detail info and error\n");
	printf("	-s,	--service_center	service_center, e.g.: host1:port1,host2:port2\n");
	printf("	-g,	--group			group, e.g.: beijing-xidan\n");
	printf("	-h,	--help			display this help and exit\n");
}


int main(int argc, char **argv) {
	bool ok = true;
	const char* const short_options = "h:d:c:u:g:s:v";
	const struct option long_options[] = {
		{ "help", 0, NULL, 'h' },
		{ "delimeter", 0, NULL, 'd' },
		{ "category", 0, NULL, 'c' },
		{ "verbose", 0, NULL, 'v' },
		{ "service_center", 0, NULL, 's' },
		{ "group", 0, NULL, 'g' }
	};
	int next_option;

	char *delimeter = ":";

	struct stat stat_buf;
	FILE *input_fp;
	char *infile = "-";
	size_t len = 0;
	ssize_t read;
	char *line = NULL;

	char *category = NULL;
	bool verbose = false;

	string sc;
	string group;

	/* before parse options */
	while (0 < (next_option = getopt_long(argc, argv, short_options, long_options, NULL))) {
		switch (next_option) {
			default:
			case 'h':
				print_usage(argv[0]);
				exit(0);
				break;
			case 'c':
				category = optarg;
				break;
			case 'd':
				delimeter = optarg;
				break;
			case 'v':
				verbose = true;
				break;
			case 's':
				sc = optarg;
				break;
			case 'g':
				group= optarg;
				break;
		}
	}
	/* after parse options */

	if(group.size() == 0) {
		print_usage(argv[0]);
		exit(EXIT_FAILURE);
	}


	/* Main loop */
	uint64_t total_messages = 0;
	uint64_t error_raw_messages = 0;
	uint64_t error_sends = 0;
	
	uint64_t total_bytes = 0;

	server_vector_t serverList;

		
	GroupService gs("/cloudscribe", sc);
	int timeout = 5;//sec
	if(!gs.waitForConnected(timeout)) {
		fprintf(stderr, "unable connect to zookeeper quorum, exiting...\n");
		exit(1);
	}

	vector<string> hostStrs;
	gs.getMembers(group, hostStrs);

	if(0==hostStrs.size()) {
		fprintf(stderr, "there is no living host in group %s\n", group);
		exit(2);
	}

	vector<std::pair<std::string,int> > servers;
	for(vector<string>::iterator it=hostStrs.begin(),end=hostStrs.end();it!=end;++it) {
		cout<<"get member: "<<(*it)<<endl;
		vector<string> host_port_pair;
		boost::algorithm::split(host_port_pair, (*it), boost::algorithm::is_any_of("_"));
		if(host_port_pair.size() == 2) {
			servers.push_back(
					std::pair<std::string,int>(
						host_port_pair[0],
						atoi(
							host_port_pair[1].c_str()
						    )
						)
					);
		}
	}
	
	
	shared_ptr<TTransport> socket = shared_ptr<TSocket> (new TSocketPool(servers));
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


		int argind = optind;
		do {
			vector<LogEntry> messages;
			ResultCode rc = OK;

			if(argind < argc) {
				infile = argv[argind];
			}

			if(STREQ (infile, "-")) {
				input_fp = stdin;
			} else {
				input_fp = fopen(infile, "r");
				if (NULL == input_fp) {
					error (0, errno, "%s", infile);
					ok = false;
					continue;
				}
	        	}

			if (fstat(fileno(input_fp), &stat_buf) < 0) {
				error (0, errno, "%s", infile);
				ok = false;
				goto contin;
			}


			while ((read = getline(&line, &len, input_fp)) != -1) {
				total_bytes += read;
				
				++total_messages;
				LogEntry message;
				if(NULL == category) {
					vector<string> cat2message;
					boost::algorithm::split(cat2message, line, boost::algorithm::is_any_of(delimeter));
					if(cat2message.size() != 2) {
						if(verbose) {
							++error_raw_messages;
						}
					} else {

	                                	message.category = cat2message[0];
		                                message.message = cat2message[1];
		                                messages.push_back(message);
					}
				} else {
					message.category = category;
					message.message = line;
					messages.push_back(message);
				}

				if(total_messages % BATCH_LINES == 0) {
					int retry = 3;
					for(;;) {
						rc = client.Log(messages);
						if(OK == rc) {
							vector<LogEntry>().swap(messages);
							break;
						}
						
						if(--retry) continue;
						
						TRACE (verbose, "ERROR: send %d messages, rcode: %d\n", messages.size(), rc);
						error_sends += messages.size();
						break;
					}
				}
			}

			if(messages.size() > 0) {
				int retry = 3;
				for(;;) {
					rc = client.Log(messages);
					if(OK == rc) {
						vector<LogEntry>().swap(messages);
						break;
					} 

					if(--retry) continue;

					TRACE (verbose, "ERROR: send %d messages, rcode: %d\n", messages.size(), rc);
					error_sends += messages.size();
					break;
				}	
			}

contin:

			if (!STREQ (infile, "-") && fclose (input_fp) < 0) {	
				error (0, errno, "%s", infile);
				ok = false;
			}

		} while(++argind < argc);

                gettimeofday(&bwend,NULL);
                printf("duration:%lu ms, total bytes: %llu\n", (bwend.tv_sec*1000 +bwend.tv_usec/1000) - (bwstart.tv_sec*1000 + bwstart.tv_usec/1000), total_bytes);
                framedTransport->close();
        } catch (TException &tx) {
                printf("ERROR: %s\n", tx.what());
        }

	if (line) {
		free(line);
	}

	if(verbose) {
		TRACE (verbose, "==========Verbose Info==========\n");
		TRACE (verbose, "total messages: %llu\n", total_messages);
		TRACE (verbose, "error messages: %llu\n", error_raw_messages);
		TRACE (verbose, "error sends: %llu\n", error_sends);
	}

	exit (ok ? EXIT_SUCCESS : EXIT_FAILURE);
}
