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
 * ������������ú�Ŀ¼����
 */
class network_config {
	public:
		// ��ȡ���������machine/port�б�, �ɹ�ʱ����true.
		static bool getService(const std::string& s, server_vector_t& _return) {
			return true;
		}
};
}

#endif // FORWARDER_ENV
