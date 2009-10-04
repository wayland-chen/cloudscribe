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
	// ��lockʱ��Ҫע��:
	// mapMutex����ס���ж�connMap�Ķ�д.
	// ��connection�ϵ�lock���д��ɾ��ʱ��.
	// ��connection�ϵ�lock����ȷ������ص�refcount��һ��, ����refcount�ķ��ʶ���Ҫ��mapMutex��.
	// �����ڳ���connection�ľֲ���֮ǰ�Ȼ��mapMutex��.

	pthread_mutex_lock(&mapMutex);
	conn_map_t::iterator iter = connMap.find(key);
	if (iter != connMap.end()) {
		(*iter).second->addRef();
		pthread_mutex_unlock(&mapMutex);
		return true;
	} else {
		if (conn->open()) {
			// ref count��1��ʼ,�������ﲻ��ҪaddRef
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
		//����ͺ������,���һ��client close�˶��,���ü���������, �Ϳ�����������client����.
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

	// ������messages�е�ָ������ݿ�����vector, ������Ϊthrift��֧��vector�д��ָ�����ʽ.
	// ������һ��������һ��Ӱ��ĵط�.
	std::vector<LogEntry> msgs;
	for (logentry_vector_t::iterator iter = messages->begin(); iter != messages->end(); ++iter) {
		msgs.push_back(**iter);
	}

	// �������ʧ��,���ǻ����´����Ӳ�����.
	// �������ǵĻ��������ӵ�һ������server, ����server���ʧЧ, ���Ǳ���������������������.
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
				return false; // �����������. ���ĳ̨��������,��ôͨ�������������������Ҳ���.
			}
		} catch (TTransportException& ttx) {
			LOG_OPER("Failed to send <%d> messages to remote forwarder server %s error <%s>", size, connectionString().c_str(), ttx.what());
		} catch (...) {
			LOG_OPER("Unknown exception sending <%d> messages to remote forwarder server %s", size, connectionString().c_str());
		}

		// �������ʱ�����쳣,����ͻ�ִ�е�����
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
