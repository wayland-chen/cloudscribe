#ifndef CLOUDX_SCRIBEBASE_H
#define CLOUDX_SCRIBEBASE_H


#include "gen-cpp/CloudxService.h"

#include "thrift/server/TServer.h"
#include "thrift/concurrency/Mutex.h"

#include <time.h>
#include <string>
#include <map>

namespace cloudx {
namespace base {
using apache::thrift::concurrency::Mutex;
using apache::thrift::concurrency::ReadWriteMutex;
using apache::thrift::server::TServer;

struct ReadWriteInt: ReadWriteMutex {
	int64_t value;
};
struct ReadWriteCounterMap: ReadWriteMutex, std::map<std::string, ReadWriteInt> {
};

typedef void (*get_static_limref_ptr)(apache::thrift::reflection::limited::Service &);

/**
 * 基础的Tsoss服务实现.
 */
class CloudxBase : virtual public CloudxServiceIf {
protected:
	CloudxBase(std::string name, get_static_limref_ptr reflect_lim = NULL);
	virtual ~CloudxBase() {
	}

public:
	void getName(std::string& _return);
	virtual void getVersion(std::string& _return) {
		_return = "";
	}

	virtual base_status getStatus() = 0;
	virtual void getStatusDetails(std::string& _return) {
		_return = "";
	}

	void setOption(const std::string& key, const std::string& value);
	void getOption(std::string& _return, const std::string& key);
	void getOptions(std::map<std::string, std::string> & _return);

	int64_t aliveSince();

	void getLimitedReflection(apache::thrift::reflection::limited::Service& _return) {
		_return = reflection_limited_;
	}

	virtual void reinitialize() {
	}

	virtual void shutdown() {
		if (server_.get() != NULL) {
			server_->stop();
		}
	}

	int64_t incrementCounter(const std::string& key, int64_t amount = 1);
	int64_t setCounter(const std::string& key, int64_t value);

	void getCounters(std::map<std::string, int64_t>& _return);
	int64_t getCounter(const std::string& key);

	/**
	 * 为shutdown方法设置server句柄
	 */
	void setServer(boost::shared_ptr<TServer> server) {
		server_ = server;
	}

	void getCpuProfile(std::string& _return, int32_t durSecs) {
		_return = "";
	}

private:
	std::string name_;
	apache::thrift::reflection::limited::Service reflection_limited_;
	int64_t aliveSince_;

	std::map<std::string, std::string> options_;
	Mutex optionsLock_;

	ReadWriteCounterMap counters_;

	boost::shared_ptr<TServer> server_;
};
}
} //cloudx.scribe

#endif /* CLOUDX_SCRIBEBASE_H */
