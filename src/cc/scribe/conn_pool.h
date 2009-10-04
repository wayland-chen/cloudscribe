/**
 * author: edisonpeng@tencent.com
 */
#ifndef FORWARDER_CONN_POOL_H
#define FORWARDER_CONN_POOL_H

#include <string>

#include "thrift/protocol/TBinaryProtocol.h"
#include "thrift/server/TNonblockingServer.h"
#include "thrift/transport/TSocket.h"
#include "thrift/transport/TSocketPool.h"
#include "thrift/transport/TServerSocket.h"
#include "thrift/transport/TTransportUtils.h"

#include "common.h"
#include "gen-cpp/forwarder.h"



/**
 * 网络连接的封装.作为client时使用
 */
class forwarderConn {
	public:
		forwarderConn(const std::string& host, unsigned long port, int timeout);
		forwarderConn(const std::string &service, const server_vector_t &servers, int timeout);
		virtual ~forwarderConn();

		void addRef();
		void releaseRef();
		unsigned getRef();

		void lock();
		void unlock();

		bool open();
		void close();
		bool send(boost::shared_ptr<logentry_vector_t> messages);

	private:
		std::string connectionString();

	protected:
		boost::shared_ptr<apache::thrift::transport::TSocket> socket;
		boost::shared_ptr<apache::thrift::transport::TFramedTransport> framedTransport;
		boost::shared_ptr<apache::thrift::protocol::TBinaryProtocol> protocol;
		boost::shared_ptr<forwarder::thrift::forwarderClient> resendClient;

		unsigned refCount;

		bool smcBased;
		std::string smcService;
		server_vector_t serverList;
		std::string remoteHost;
		unsigned long remotePort;
		int timeout; // connection, send, and recv timeout
		pthread_mutex_t mutex;
};

// key为hostname:port形式
typedef std::map<std::string, boost::shared_ptr<forwarderConn> > conn_map_t;

class ConnPool {
	public:
		ConnPool();
		virtual ~ConnPool();

		bool open(const std::string& host, unsigned long port, int timeout);
		bool open(const std::string &service, const server_vector_t &servers, int timeout);

		void close(const std::string& host, unsigned long port);
		void close(const std::string &service);

		bool send(const std::string& host, unsigned long port,
				boost::shared_ptr<logentry_vector_t> messages);
		bool send(const std::string &service,
				boost::shared_ptr<logentry_vector_t> messages);

	private:
		bool openCommon(const std::string &key, boost::shared_ptr<forwarderConn> conn);
		void closeCommon(const std::string &key);
		bool sendCommon(const std::string &key, boost::shared_ptr<logentry_vector_t> messages);

	protected:
		std::string makeKey(const std::string& name, unsigned long port);

		pthread_mutex_t mapMutex;
		conn_map_t connMap;
};

#endif // !defined FORWARDER_CONN_POOL_H
