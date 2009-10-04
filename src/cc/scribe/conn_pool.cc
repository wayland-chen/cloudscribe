#include <stdexcept>
#include <sstream>

#include "logger.h"
#include "forwarder_server.h"
#include "conn_pool.h"

using std::string;
using std::ostringstream;
using std::map;
using boost::shared_ptr;
using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;
using namespace apache::thrift::server;
using namespace forwarder::thrift;

ConnPool::ConnPool() {
	pthread_mutex_init(&mapMutex, NULL);
}

ConnPool::~ConnPool() {
	pthread_mutex_destroy(&mapMutex);
}

string ConnPool::makeKey(const string& hostname, unsigned long port) {
	string key(hostname);
	key += ":";

	ostringstream oss;
	oss << port;
	key += oss.str();
	return key;
}

bool ConnPool::open(const string& hostname, unsigned long port, int timeout) {
	return openCommon(makeKey(hostname, port), shared_ptr<forwarderConn> (new forwarderConn(hostname, port, timeout)));
}

bool ConnPool::open(const string &service, const server_vector_t &servers, int timeout) {
	return openCommon(service, shared_ptr<forwarderConn> (new forwarderConn(service, servers, timeout)));
}

void ConnPool::close(const string& hostname, unsigned long port) {
	closeCommon(makeKey(hostname, port));
}

void ConnPool::close(const string &service) {
	closeCommon(service);
}

bool ConnPool::send(const string& hostname, unsigned long port, shared_ptr<logentry_vector_t> messages) {
	return sendCommon(makeKey(hostname, port), messages);
}

bool ConnPool::send(const string &service, shared_ptr<logentry_vector_t> messages) {
	return sendCommon(service, messages);
}

bool ConnPool::openCommon(const string &key, shared_ptr<forwarderConn> conn) {
	// 在lock时需要注意:
	// mapMutex会锁住所有对connMap的读写.
	// 在connection上的lock会把写和删除时序化.
	// 在connection上的lock不会确保它相关的refcount的一致, 不过refcount的访问都需要加mapMutex锁.
	// 必须在尝试connection的局部锁之前先获得mapMutex锁.

	pthread_mutex_lock(&mapMutex);
	conn_map_t::iterator iter = connMap.find(key);
	if (iter != connMap.end()) {
		(*iter).second->addRef();
		pthread_mutex_unlock(&mapMutex);
		return true;
	} else {
		if (conn->open()) {
			// ref count以1开始,所以这里不需要addRef
			connMap[key] = conn;
			pthread_mutex_unlock(&mapMutex);
			return true;
		} else {
			// conn object that failed to open is deleted
			pthread_mutex_unlock(&mapMutex);
			return false;
		}
	}
}

void ConnPool::closeCommon(const string &key) {
	pthread_mutex_lock(&mapMutex);
	conn_map_t::iterator iter = connMap.find(key);
	if (iter != connMap.end()) {
		(*iter).second->releaseRef();
		if ((*iter).second->getRef() <= 0) {
			(*iter).second->lock();
			(*iter).second->close();
			(*iter).second->unlock();
			connMap.erase(iter);
		}
	} else {
		//这里就很糟糕了,如果一个client close了多次,引用计数清零了, 就可能让其它的client遭殃.
		LOG_OPER("LOGIC ERROR: attempting to close connection <%s> that connPool has no entry for", key.c_str());
	}
	pthread_mutex_unlock(&mapMutex);
}

bool ConnPool::sendCommon(const string &key, shared_ptr<logentry_vector_t> messages) {
	pthread_mutex_lock(&mapMutex);
	conn_map_t::iterator iter = connMap.find(key);
	if (iter != connMap.end()) {
		(*iter).second->lock();
		pthread_mutex_unlock(&mapMutex);
		bool result = (*iter).second->send(messages);
		(*iter).second->unlock();
		return result;
	} else {
		LOG_OPER("send failed. No connection pool entry for <%s>", key.c_str());
		pthread_mutex_unlock(&mapMutex);
		return false;
	}
}

forwarderConn::forwarderConn(const string& hostname, unsigned long port, int timeout_) :
	refCount(1), smcBased(false), remoteHost(hostname), remotePort(port), timeout(timeout_) {
		pthread_mutex_init(&mutex, NULL);
	}

forwarderConn::forwarderConn(const string& service, const server_vector_t &servers, int timeout_) :
	refCount(1), smcBased(true), smcService(service), serverList(servers), timeout(timeout_) {
		pthread_mutex_init(&mutex, NULL);
	}

forwarderConn::~forwarderConn() {
	pthread_mutex_destroy(&mutex);
}

void forwarderConn::addRef() {
	++refCount;
}

void forwarderConn::releaseRef() {
	--refCount;
}

unsigned forwarderConn::getRef() {
	return refCount;
}

void forwarderConn::lock() {
	pthread_mutex_lock(&mutex);
}

void forwarderConn::unlock() {
	pthread_mutex_unlock(&mutex);
}

bool forwarderConn::open() {
	try {
		socket = smcBased ? shared_ptr<TSocket> (new TSocketPool(serverList)) : shared_ptr<TSocket> (new TSocket(remoteHost, remotePort));

		if (!socket) {
			throw std::runtime_error("Failed to create socket");
		}
		socket->setConnTimeout(timeout);
		socket->setRecvTimeout(timeout);
		socket->setSendTimeout(timeout);

		framedTransport = shared_ptr<TFramedTransport> (new TFramedTransport(socket));
		if (!framedTransport) {
			throw std::runtime_error("Failed to create framed transport");
		}
		protocol = shared_ptr<TBinaryProtocol> (new TBinaryProtocol(framedTransport));
		if (!protocol) {
			throw std::runtime_error("Failed to create protocol");
		}
		protocol->setStrict(false, false);
		resendClient = shared_ptr<forwarderClient> (new forwarderClient(protocol));
		if (!resendClient) {
			throw std::runtime_error("Failed to create network client");
		}

		framedTransport->open();

	} catch (TTransportException& ttx) {
		LOG_OPER("failed to open connection to remote forwarder server %s thrift error <%s>", connectionString().c_str(), ttx.what());
		return false;
	} catch (std::exception& stx) {
		LOG_OPER("failed to open connection to remote forwarder server %s std error <%s>", connectionString().c_str(), stx.what());
		return false;
	}
	LOG_OPER("Opened connection to remote forwarder server %s", connectionString().c_str());
	return true;
}

void forwarderConn::close() {
	try {
		framedTransport->close();
	} catch (TTransportException& ttx) {
		LOG_OPER("error <%s> while closing connection to remote forwarder server %s", ttx.what(), connectionString().c_str());
	}
}

bool forwarderConn::send(boost::shared_ptr<logentry_vector_t> messages) {
	int size = messages->size();

	if (size <= 0) {
		return true;
	}

	// 把所有messages中的指针的内容拷贝到vector, 这是因为thrift不支持vector中存放指针的形式.
	// 这里是一个性能有一定影响的地方.
	std::vector<LogEntry> msgs;
	for (logentry_vector_t::iterator iter = messages->begin(); iter != messages->end(); ++iter) {
		msgs.push_back(**iter);
	}

	// 如果发送失败,我们会重新打开连接并发送.
	// 比如我们的机器都连接到一个中央server, 中央server如果失效, 我们便重新连接其它机器即可.
	ResultCode result = TRY_LATER;
	for (int i = 0; i < 2; ++i) {
		try {
			result = resendClient->Log(msgs);

			if (result == OK) {
				g_Handler->incrementCounter("sent", size);
				LOG_OPER("Successfully sent <%d> messages to remote forwarder server %s", size, connectionString().c_str());
				return true;
			} else {
				LOG_OPER("Failed to send <%d> messages, remote forwarder server %s returned error code <%d>", size, connectionString().c_str(), (int) result);
				return false; // 这里别重试了. 如果某台机器过载,那么通常所有其它机器的情况也差不多.
			}
		} catch (TTransportException& ttx) {
			LOG_OPER("Failed to send <%d> messages to remote forwarder server %s error <%s>", size, connectionString().c_str(), ttx.what());
		} catch (...) {
			LOG_OPER("Unknown exception sending <%d> messages to remote forwarder server %s", size, connectionString().c_str());
		}

		// 如果发送时抛了异常,代码就会执行到这里
		close();
		if (open()) {
			LOG_OPER("reopened connection to remote forwarder server %s", connectionString().c_str());
		} else {
			return false;
		}
	}
	return false;
}

std::string forwarderConn::connectionString() {
	if (smcBased) {
		return "<SMC service: " + smcService + ">";
	} else {
		char port[10];
		snprintf(port, 10, "%lu", remotePort);
		return "<" + remoteHost + ":" + string(port) + ">";
	}
}
