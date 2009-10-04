#ifndef GROUP_SERVICE_H_ 
#define GROUP_SERVICE_H_ 



#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <zookeeper_log.h>
#include <time.h>
#include <sys/time.h>
#include <sys/socket.h>
#include <limits.h>
#include <stdbool.h>
#include <pthread.h>

#ifdef HAVE_SYS_UTSNAME_H
#include <sys/utsname.h>
#endif

#include <iostream>
#include <string>
#include <list>
#include <vector>

using namespace std;

//#define ZOOKEEPER_CONFIG_NAME "zoo.cfg"


/*
class ForwarderInfo {
public:
	inline string ip() const {return _ip;}
	inline void set_ip(const string &ip) {_ip = ip;}
	inline string port() const {return _port;}
	inline void set_port(const string &port) {_port = port;}

	string encode() const {
		return _ip + "_" + _port;
	}
private:
	string _ip;
	string _port;
};
*/




class GroupService {
public:
	GroupService(const string &rootPath, const string &quorumServers, const int timeout=5);

	bool waitForConnected(int timeout);
	
	void ensureRoot();

	virtual ~GroupService() {
		if(zh) {
			zookeeper_close(zh);
		}

		pthread_mutex_destroy(&mutex);
		pthread_cond_destroy(&hasConnectedCond);
	}

	void ensureGroup(const string &znode);
	
	void addMember(const string &group, const string &member, bool autoCreate=false);

	bool getMembers(const string &group, vector<string> &memberList);
	

private:

	
	static void watcher(zhandle_t *, int type, int state, const char *path,void*v);

private:
	zhandle_t *zh;

	pthread_mutex_t mutex;
	pthread_cond_t hasConnectedCond;
	
	string rootPath_;
	string quorumServers_;
	int timeout_;
	bool connected;
};


#endif
