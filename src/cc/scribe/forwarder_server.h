#ifndef FORWARDER_SERVER_H
#define FORWARDER_SERVER_H

//#include "store.h"
//#include "store_queue.h"

#include <string>
#include <vector>
#include <map>

#include <boost/shared_ptr.hpp>

#include <cloudxbase/CloudxBase.h>
#include "gen-cpp/forwarder.h"



class StoreQueue;
class GroupService;

/**
 * string -> vector[StoreQueue]
 * string -> vector[StoreQueue]
 * string -> StoreQueue
 */
typedef std::vector<boost::shared_ptr<StoreQueue> > store_list_t;
typedef std::map<std::string, boost::shared_ptr<store_list_t> > category_map_t;
typedef std::map<std::string, boost::shared_ptr<StoreQueue> > category_prefix_map_t;





class forwarderHandler: virtual public forwarder::thrift::forwarderIf, public cloudx::base::CloudxBase {

public:
	forwarderHandler(unsigned long int port, const std::string& conf_file);
	~forwarderHandler();

	void shutdown();
	void initialize();
	void reinitialize();

	forwarder::thrift::ResultCode Log(const std::vector<forwarder::thrift::LogEntry>& messages);

	void getVersion(std::string& _return) {
		_return = "1.0";
	}
	cloudx::base::base_status getStatus();
	void getStatusDetails(std::string& _return);
	void setStatus(cloudx::base::base_status new_status);
	void setStatusDetails(const std::string& new_status_details);

	std::string eth;
	unsigned long int port; // it's long because that's all I implemented in the conf class
	//TODO must move to a global singleton
	boost::shared_ptr<GroupService> groupServicePtr;
private:
	unsigned long checkPeriod; // 周期性检查所有的缓冲数据

	// pcategories为[type->StoreQueue]的map
	// 每个StoreQueue 包含一个 Store
	// Store => Store [1...*]
	category_map_t* pcategories;
	category_prefix_map_t* pcategory_prefixes;

	// 默认的store
	boost::shared_ptr<StoreQueue> defaultStore;

	std::string configFilename;
	cloudx::base::base_status status;
	std::string statusDetails;
	apache::thrift::concurrency::Mutex statusLock;
	time_t lastMsgTime;
	unsigned long numMsgLastSecond;
	unsigned long maxMsgPerSecond;
	unsigned long maxQueueSize;
	bool newThreadPerCategory;


	//new feature added by edison
	std::string groupServiceRoot;
	std::string groupServiceServers;
	std::string group;

	
	//不允许拷贝，赋值和空构造
	forwarderHandler();
	forwarderHandler(const forwarderHandler& rhs);
	const forwarderHandler& operator=(const forwarderHandler& rhs);

protected:
	bool throttleDeny(int num_messages); // 如果过载则返回true
	void deleteCategoryMap(category_map_t *pcats);
	const char* statusAsString(cloudx::base::base_status new_status);
	bool createCategoryFromModel(const std::string &category, const boost::shared_ptr<StoreQueue> &model);
};

extern boost::shared_ptr<forwarderHandler> g_Handler;

#endif // FORWARDER_SERVER_H
