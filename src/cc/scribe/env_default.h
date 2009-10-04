/**
 * author: edisonpeng@tencent.com
 */
#ifndef FORWARDER_ENV
#define FORWARDER_ENV


using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;
using namespace apache::thrift::concurrency;

using namespace forwarder::thrift;
using namespace std;
using boost::shared_ptr;


namespace {
/*
 * 基于网络的配置和目录服务
 */
class network_config {
	public:
		// 获取命名服务的machine/port列表, 成功时返回true.
		static bool getService(const std::string& s, server_vector_t& _return) {
			return true;
		}
};
}

#endif // FORWARDER_ENV
