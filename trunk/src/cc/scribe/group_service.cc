#include "group_service.h"

#include <iostream>


GroupService::GroupService(const string &rootPath, const string &quorumServers, const int timeout):
        rootPath_(rootPath),
	quorumServers_(quorumServers),
       	timeout_(timeout),
        connected(false) {
	
	pthread_mutex_init(&mutex, NULL);
        pthread_cond_init(&hasConnectedCond, NULL);

	zh = zookeeper_init(quorumServers_.c_str(), GroupService::watcher, 10000, 0, this, 0);
	if(zh==0) {
		cout<<"GroupService start error"<<endl;
		return;
	}
}

void GroupService::ensureRoot() {
	struct Stat stat;
	int rc = zoo_exists(zh, rootPath_.c_str(), 1, &stat);

	if (rc == ZNONODE) {
		rc = zoo_create(zh, rootPath_.c_str(), "", 0, &ZOO_OPEN_ACL_UNSAFE, 0, 0, 0);
		//FIXME don't hanle rc
	}
}




bool GroupService::waitForConnected(int timeout) {
	struct timespec abs_timeout;
	memset(&abs_timeout, 0, sizeof(struct timespec));

	time_t now;
	time(&now);
	abs_timeout.tv_sec = now + timeout;
	pthread_mutex_lock(&mutex);
	pthread_cond_timedwait(&hasConnectedCond, &mutex, &abs_timeout);
	pthread_mutex_unlock(&mutex);
	return this->connected;
}


void GroupService::ensureGroup(const string &group) {
	struct Stat stat;
	string path = rootPath_ + "/" + group;
	int rc = zoo_exists(zh, path.c_str(), 1, &stat); 
	if (rc == ZNONODE) {
		rc = zoo_create(zh, path.c_str(), "", 0, &ZOO_OPEN_ACL_UNSAFE, 0, 0, 0);
		//FIXME don't hanle rc
	}
}

void GroupService::addMember(const string &group, const string &member,bool autoCreate) {
	if(autoCreate) {
		ensureGroup(group);
	}

	struct Stat stat;
	string path = rootPath_ + "/" + group + "/" + member;
	int rc = zoo_exists(zh, path.c_str(), 1, &stat); 
	if (rc == ZNONODE) {
		rc = zoo_create(zh, path.c_str(), "", 0, &ZOO_OPEN_ACL_UNSAFE, ZOO_EPHEMERAL, 0, 0);
		//FIXME don't hanle rc
		//
	}
}

bool GroupService::getMembers(const string &group, vector<string> &memberList) {
	string path = rootPath_ + "/" + group;
	String_vector children;
	int rc = zoo_get_children(zh, path.c_str(), 0, &children);
	if(ZOK != rc) {
		return false;
	}
	for(int i=0;i<children.count;++i) {
		char *m = children.data[i];
		memberList.push_back(m);
	}
	return true;
}

void GroupService::watcher(zhandle_t *, int type, int state, const char *path,void*v) {
	cout<<"GroupService::watcher"<<endl;
	GroupService *ctx = (GroupService*)v;

	if (state == ZOO_CONNECTED_STATE) {
		ctx->connected = true; 
	} else {
		ctx->connected = false;
	}       
	if (type != ZOO_SESSION_EVENT) {
	}       


	pthread_mutex_lock(&(ctx->mutex));
	pthread_cond_signal(&(ctx->hasConnectedCond));
	pthread_mutex_unlock(&(ctx->mutex));
}
