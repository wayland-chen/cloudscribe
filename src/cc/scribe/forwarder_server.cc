#include "forwarder_server.h"

#include <getopt.h>
#include <sys/resource.h>

#include <boost/filesystem/operations.hpp>

#include "inet_addr.h"
#include "store.h"
#include "store_queue.h"
#include "group_service.h"
#include "logger.h"


using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;
using namespace apache::thrift::server;
using namespace apache::thrift::concurrency;

using namespace cloudx::base;

using namespace forwarder::thrift;
using namespace std;
using boost::shared_ptr;

shared_ptr<forwarderHandler> g_Handler;

#define DEFAULT_CONF_FILE_LOCATION "/usr/local/cloudscribe/scribe.conf"
#define DEFAULT_CHECK_PERIOD       5
#define DEFAULT_MAX_MSG_PER_SECOND 100000
#define DEFAULT_MAX_QUEUE_SIZE     5000000



void print_usage(const char* program_name) {
	cout << "Usage: " << program_name << " [-p port] [-c config_file]"<<endl;
}

int main(int argc, char **argv) {
	try {
		/* 增加fd的数量 */
		struct rlimit r_fd = { 65535, 65535 };
		if (-1 == setrlimit(RLIMIT_NOFILE, &r_fd)) {
			LOG_OPER("setrlimit error (setting max fd size)");
		}

		/* app参数解析 */
		int next_option;
		const char* const short_options = "hp:c:";
		const struct option long_options[] = { { "help", 0, NULL, 'h' }, { "port", 0, NULL, 'p' }, { "config", 0, NULL, 'c' }, { NULL, 0, NULL, 'o' }, };

		unsigned long int port = 0;
		std::string config_file;
		while (0 < (next_option = getopt_long(argc, argv, short_options, long_options, NULL))) {
			switch (next_option) {
			default:
			case 'h':
				print_usage(argv[0]);
				exit(0);
			case 'c':
				config_file = optarg;
				break;
			case 'p':
				port = strtoul(optarg, NULL, 0);
				break;
			}
		}

		// 假设选项参数之后的那个就是配置文件名了
		if (optind < argc && config_file.empty()) {
			config_file = argv[optind];
		}

		g_Handler = shared_ptr<forwarderHandler> (new forwarderHandler(port, config_file));
		g_Handler->initialize();

		shared_ptr<TProcessor> processor(new forwarderProcessor(g_Handler));

		shared_ptr<TProtocolFactory> binaryProtocolFactory(new TBinaryProtocolFactory(0, 0, false, false));
		TNonblockingServer server(processor, binaryProtocolFactory, g_Handler->port);

		LOG_OPER("Starting forwarder server on port %lu", g_Handler->port);
		fflush(stderr);

		server.serve();

	} catch (std::exception const& e) {
		LOG_OPER("Exception in main: %s", e.what());
	}

	LOG_OPER("forwarder server exiting");
	return 0;
}

forwarderHandler::forwarderHandler(unsigned long int server_port, const std::string& config_file) :
	CloudxBase("Forwarder"), port(server_port), checkPeriod(DEFAULT_CHECK_PERIOD),
	pcategories(NULL), pcategory_prefixes(NULL), configFilename(config_file), status(STARTING), statusDetails("initial state"), numMsgLastSecond(0), maxMsgPerSecond(DEFAULT_MAX_MSG_PER_SECOND),
	maxQueueSize(DEFAULT_MAX_QUEUE_SIZE),
	newThreadPerCategory(true) {
	time(&lastMsgTime);
}

forwarderHandler::~forwarderHandler() {
	deleteCategoryMap(pcategories);
	if (pcategory_prefixes) {
		delete pcategory_prefixes;
		pcategory_prefixes = NULL;
	}
}

// 返回handler的状态信息, 如果队列状态为空,则状态返回ACTIVE, 如果队列非空,则状态返回WARNING
base_status forwarderHandler::getStatus() {
	Guard monitor(statusLock);
	base_status return_status(status);
	if (status == ALIVE) {
		for (category_map_t::iterator cat_iter = pcategories->begin(); cat_iter != pcategories->end(); ++cat_iter) {
			for (store_list_t::iterator store_iter = cat_iter->second->begin(); store_iter != cat_iter->second->end(); ++store_iter) {
				if (!(*store_iter)->getStatus().empty()) {
					return_status = WARNING;
					return return_status;
				}
			} // for each store
		} // for each category
	} // 如果status先前不为ALIVE, 那么让相关模块去恢复它
	return return_status;
}

void forwarderHandler::setStatus(base_status new_status) {
	LOG_OPER("STATUS: %s", statusAsString(new_status));
	Guard monitor(statusLock);
	status = new_status;
}

// 返回handler的详细状态信息, 或者返回第一个非空store的状态信息.
void forwarderHandler::getStatusDetails(std::string& _return) {
	Guard monitor(statusLock);
	_return = statusDetails;
	if (_return.empty()) {
		if (pcategories) {
			for (category_map_t::iterator cat_iter = pcategories->begin(); cat_iter != pcategories->end(); ++cat_iter) {
				for (store_list_t::iterator store_iter = cat_iter->second->begin(); store_iter != cat_iter->second->end(); ++store_iter) {
					if (!(_return = (*store_iter)->getStatus()).empty()) {
						return;
					}
				} // for each store
			} // for each category
		} //if categories
	} // if
	return;
}

void forwarderHandler::setStatusDetails(const string& new_status_details) {
	LOG_OPER("STATUS: %s", new_status_details.c_str());
	Guard monitor(statusLock);
	statusDetails = new_status_details;
}

const char* forwarderHandler::statusAsString(base_status status) {
	switch (status) {
	case DEAD:
		return "DEAD";
	case STARTING:
		return "STARTING";
	case ALIVE:
		return "ALIVE";
	case STOPPING:
		return "STOPPING";
	case STOPPED:
		return "STOPPED";
	case WARNING:
		return "WARNING";
	default:
		return "unknown status code";
	}
}

bool forwarderHandler::createCategoryFromModel(const string &category, const boost::shared_ptr<StoreQueue> &model) {
	//如果pcategories为空 或 给定类别已经存在,便直接返回.
	if ((pcategories == NULL) || (pcategories->find(category) != pcategories->end())) {
		return false;
	}

	LOG_OPER("[%s] Creating new category from model %s", category.c_str(), model->getCategoryHandled().c_str());

	// 确保类别名称是合法的.
	try {
		string clean_path = boost::filesystem::path(category).string();

		if (clean_path.compare(category) != 0) {
			LOG_OPER("Category not a valid boost filename");
			return false;
		}
	} catch (std::exception const& e) {
		LOG_OPER("Category not a valid boost filename.  Boost exception:%s", e.what());
		return false;
	}

	shared_ptr<StoreQueue> pstore;
	if (newThreadPerCategory) {//如果是线程每类别
		// 为此类别创建一个线程/存储队列(StoreQueue)
		pstore = shared_ptr<StoreQueue> (new StoreQueue(model, category));
		pstore->open();
	} else {
		//使用现有的store
		pstore = model;
	}

	shared_ptr<store_list_t> pstores = shared_ptr<store_list_t> (new store_list_t);

	(*pcategories)[category] = pstores;
	pstores->push_back(pstore);

	return true;
}

ResultCode forwarderHandler::Log(const vector<LogEntry>& messages) {
	//LOG_OPER("received Log with <%d> messages", (int)messages.size());

	if (throttleDeny(messages.size())) {	//阀值控制
		incrementCounter("denied for rate");
		return TRY_LATER;
	}

	if (!pcategories || !pcategory_prefixes) {
		incrementCounter("invalid requests");
		return TRY_LATER;
	}

	// 采用阀值控制来防止store queue过长. 因为放到队列的消息要么成功,要么失败.
	unsigned long max_count = 0;
	for (category_map_t::iterator cat_iter = pcategories->begin(); cat_iter != pcategories->end(); ++cat_iter) {
		shared_ptr<store_list_t> pstores = cat_iter->second;
		if (!pstores) {
			throw std::logic_error("throttle check: iterator in category map holds null pointer");
		}
		for (store_list_t::iterator store_iter = pstores->begin(); store_iter != pstores->end(); ++store_iter) {
			if (*store_iter == NULL) {
				throw std::logic_error("throttle check: iterator in store map holds null pointer");
			} else {
				unsigned long size = (*store_iter)->getSize();
				if (size > max_count) {
					max_count = size;
				}
			}
		}
	}

	// 一次传递的消息数量不能超过队列最大可允许的长度
	if (max_count > maxQueueSize) {
		incrementCounter("denied for queue size");
		return TRY_LATER;
	}

	for (vector<LogEntry>::const_iterator msg_iter = messages.begin(); msg_iter != messages.end(); ++msg_iter) {
		// 类别不能为空字符串
		if ((*msg_iter).category.empty()) {
			incrementCounter("received blank category");
			continue;
		}

		boost::shared_ptr<store_list_t> store_list;

		string category = (*msg_iter).category;

		// 1). 对类别进行精确匹配
		if (pcategories) {
			category_map_t::iterator cat_iter = pcategories->find(category);
			if (cat_iter != pcategories->end()) {
				store_list = cat_iter->second;
			}
		}

		// 2). 对类别进行前缀匹配
		if (store_list == NULL) {
			category_prefix_map_t::iterator cat_prefix_iter = pcategory_prefixes->begin();
			while (cat_prefix_iter != pcategory_prefixes->end()) {
				string::size_type len = cat_prefix_iter->first.size();
				if (cat_prefix_iter->first.compare(0, len - 1, category, 0, len - 1) == 0) {
					// 找到了一个匹配类别就创建一个新类别
					if (createCategoryFromModel(category, cat_prefix_iter->second)) {
						category_map_t::iterator cat_iter = pcategories->find(category);

						if (cat_iter != pcategories->end()) {
							store_list = cat_iter->second;
						} else {
							LOG_OPER("failed to create new prefix store for category <%s>", category.c_str());
						}
					}

					break;
				}
				++cat_prefix_iter;
			}
		}

		// 3). 如果还没找到，那就看有没有默认的store, 有就根据默认store来构建一个
		if (store_list == NULL) {
			if (defaultStore != NULL) {
				if (createCategoryFromModel(category, defaultStore)) {
					category_map_t::iterator cat_iter = pcategories->find(category);

					if (cat_iter != pcategories->end()) {
						store_list = cat_iter->second;
					} else {
						LOG_OPER("failed to create new default store for category <%s>", category.c_str());
					}
				}
			}
		}

		// 再找不到, 就说明不合法了
		if (store_list == NULL) {
			LOG_OPER("log entry has invalid category <%s>", (*msg_iter).category.c_str());
			incrementCounter("received bad");
			continue;
		}

		int numstores = 0;

		// 把消息添加到store_list
		for (store_list_t::iterator store_iter = store_list->begin(); store_iter != store_list->end(); ++store_iter) {
			++numstores;
			boost::shared_ptr<LogEntry> ptr(new LogEntry);
			ptr->category = (*msg_iter).category;
			ptr->message = (*msg_iter).message;

			(*store_iter)->addMessage(ptr);
		}

		// 检查消息是否被添加到store_list
		if (numstores) {
			incrementCounter("received good");
		} else {
			incrementCounter("received bad");
		}
	}

	return OK;
}

// 如果过载则返回true, 每秒只允许固定数量的消息.
bool forwarderHandler::throttleDeny(int num_messages) {
	time_t now;
	time(&now);
	if (now != lastMsgTime) {
		lastMsgTime = now;
		numMsgLastSecond = 0;
	}

	// 1). 如果收到一个很变态的大消息包, 消息个数超级多, 超过了一秒阀值的一半，那就尽量保存它
	if (num_messages > (int) maxMsgPerSecond / 2) {
		LOG_OPER("throttle allowing rediculously large packet with <%d> messages", num_messages);
		return false;
	}

	// 2). 正常阀值处理
	if (numMsgLastSecond + num_messages > maxMsgPerSecond) {
		LOG_OPER("throttle denying request with <%d> messages. It would exceed max of <%lu> messages this second", num_messages, maxMsgPerSecond);
		return true;
	} else {
		numMsgLastSecond += num_messages;
		return false;
	}
}

void forwarderHandler::shutdown() {
	setStatus(STOPPING);

	// Thrift 当前不支持从handler中终止server, 所以我们就只能这么搞.
	deleteCategoryMap(pcategories);
	pcategories = NULL;
	if (pcategory_prefixes) {
		delete pcategory_prefixes;
		pcategory_prefixes = NULL;
	}
	exit(0);
}

void forwarderHandler::reinitialize() {
	LOG_OPER("reload not supported");
	//initialize();
}

void forwarderHandler::initialize() {
	// 初始化中的初始化，哈哈
	setStatus(STARTING);
	setStatusDetails("configuring");

	bool perfect_config = true;
	bool enough_config_to_run = true;
	int numstores = 0;
	category_map_t *pnew_categories = new category_map_t;
	category_prefix_map_t *pnew_category_prefixes = new category_prefix_map_t;
	shared_ptr<StoreQueue> tmpDefault;

	try {
		// 获取配置数据并解析.
		// 如果指定了配置文件就用配置文件，否则用默认的.
		// 否则我们将会从service management console获取它的配置，再不行，就使用默认配置了.
		StoreConf config;
		string config_file;

		if (configFilename.empty()) {
			config_file = DEFAULT_CONF_FILE_LOCATION;
		} else {
			config_file = configFilename;
		}
		config.parseConfig(config_file);
		
		// 首先装载全局配置
		config.getUnsigned("max_msg_per_second", maxMsgPerSecond);
		config.getUnsigned("max_queue_size", maxQueueSize);
		config.getUnsigned("check_interval", checkPeriod);


		// 如果new_thread_per_category为真, 那么我们将会为唯一消息类别都创建一个thread/StoreQueue对.
		// 否则我们只会为所有store创建一个线程.
		string temp;
		config.getString("new_thread_per_category", temp);
		if (0 == temp.compare("no")) {
			newThreadPerCategory = false;
		}

		//get ip by eth and port
		config.getString("eth", eth);
		unsigned long int old_port = port;
		config.getUnsigned("port", port);
		if (old_port != 0 && port != old_port) {
			LOG_OPER("port %lu from conf file overriding old port %lu", port, old_port);
		}
		if (port <= 0) {
			throw runtime_error("No port number configured");
		}

		//quorum settings
		config.getString("groupServiceRoot", groupServiceRoot);
		config.getString("groupServiceServers", groupServiceServers);
		config.getString("group", group);
		groupServicePtr = shared_ptr<GroupService>(new GroupService(groupServiceRoot, groupServiceServers));

		long groupServiceTimeout = 5;
		config.getInt("groupServiceTimeout", groupServiceTimeout);
		if(groupServiceTimeout ==0) {
			groupServiceTimeout = 5;
		}
		LOG_OPER("waiting for zookeeper");
		if(!groupServicePtr->waitForConnected(groupServiceTimeout)) {
			throw runtime_error("Unable to connect to zookeeper");
		}
		LOG_OPER("connected to zookeeper");
		groupServicePtr->ensureRoot();


		//add me to specified group	
		{
			char pathbuf[256];
			int len = get_ip_by_eth(eth.c_str(), pathbuf, 128);
			char *path1 = pathbuf + len;
			sprintf(path1, "_%lu", port);
			printf("pathbuf: %s\n", pathbuf);
			groupServicePtr->addMember(group, pathbuf,true);
		}
		
		
		std::vector<pStoreConf> store_confs;
		config.getAllStores(store_confs);
		for (std::vector<pStoreConf>::iterator iter = store_confs.begin(); iter != store_confs.end(); ++iter) {
			bool is_default = false;
			pStoreConf store_conf = (*iter);
			std::string category;
			if (!store_conf->getString("category", category) || category.empty()) {
				setStatusDetails("Bad config - store with no category");
				perfect_config = false;
				continue;
			}

			LOG_OPER("CATEGORY : %s", category.c_str());
			if (0 == category.compare("default")) {
				if (tmpDefault != NULL) {
					setStatusDetails("Bad config - multiple default stores specified");
					perfect_config = false;
					continue;
				}
				is_default = true;
			}

			bool is_prefix_category = (!category.empty() && category[category.size() - 1] == '*');

			std::string type;
			if (!store_conf->getString("type", type) || type.empty()) {
				string errormsg("Bad config - no type for store with category: ");
				errormsg += category;
				setStatusDetails(errormsg);
				perfect_config = false;
				continue;
			}

			shared_ptr<StoreQueue> pstore;
			if (!is_prefix_category && pcategories) {
				category_map_t::iterator category_iter = pcategories->find(category);
				if (category_iter != pcategories->end()) {
					shared_ptr<store_list_t> pstores = category_iter->second;

					for (store_list_t::iterator it = pstores->begin(); it != pstores->end(); ++it) {
						if ((*it)->getBaseType() == type && pstores->size() <= 1) { // no good way to match them up if there's more than one
							pstore = (*it);
							pstores->erase(it);
						}
					}
				}
			}

			if (!pstore) {
				try {
					string store_name;
					bool is_model, multi_category;

					/* 把先前所有*匹配的类别都干掉 */
					if (is_prefix_category) {
						store_name = category.substr(0, category.size() - 1);
					} else {
						store_name = category;
					}
					// 检查当前store是否可以处理多个类别
					multi_category = !newThreadPerCategory && (is_default || is_prefix_category);

					// 检查当前store是否是一个原型model
					is_model = newThreadPerCategory && (is_default || is_prefix_category);

					pstore = shared_ptr<StoreQueue> (new StoreQueue(type, store_name, checkPeriod, is_model, multi_category));
				} catch (...) {
					pstore.reset();
				}
			}

			if (!pstore) {
				string errormsg("Bad config - can't create a store of type: ");
				errormsg += type;
				setStatusDetails(errormsg);
				perfect_config = false;
				continue;
			}

			pstore->configureAndOpen(store_conf);
			++numstores;

			if (is_default) {
				LOG_OPER("Creating default store");
				tmpDefault = pstore;
			} else if (is_prefix_category) {
				category_prefix_map_t::iterator category_iter = pnew_category_prefixes->find(category);

				if (category_iter == pnew_category_prefixes->end()) {
					(*pnew_category_prefixes)[category] = pstore;
				} else {
					string errormsg = "Bad config - multiple prefix stores specified for category: ";

					errormsg += category;
					setStatusDetails(errormsg);
					perfect_config = false;
				}
			}

			// 如果不是原型model, 则登记到pnew_categories中
			if (!pstore->isModelStore()) {
				shared_ptr<store_list_t> pstores;
				category_map_t::iterator category_iter = pnew_categories->find(category);
				if (category_iter != pnew_categories->end()) {
					pstores = category_iter->second;
				} else {
					pstores = shared_ptr<store_list_t> (new store_list_t);
					(*pnew_categories)[category] = pstores;
				}
				pstores->push_back(pstore);
			}
		} // for each store in the conf file
	} catch (std::exception const& e) {
		string errormsg("Bad config - exception: ");
		errormsg += e.what();
		setStatusDetails(errormsg);
		perfect_config = false;
		enough_config_to_run = false;
	}

	if (numstores) {
		LOG_OPER("configured <%d> stores", numstores);
	} else {
		setStatusDetails("No stores configured successfully");
		perfect_config = false;
		enough_config_to_run = false;
	}

	if (enough_config_to_run) {
		deleteCategoryMap(pcategories);
		pcategories = pnew_categories;
		if (pcategory_prefixes) {
			delete pcategory_prefixes;
		}
		pcategory_prefixes = pnew_category_prefixes;
		defaultStore = tmpDefault;
	} else {
		// 如果新配置无效, 就不进行配置更新了, 此时把状态设置为WARNING
		deleteCategoryMap(pnew_categories);
		deleteCategoryMap(pcategories);
		pcategories = NULL;
		if (pcategory_prefixes) {
			delete pcategory_prefixes;
			pcategory_prefixes = NULL;
		}
		if (pnew_category_prefixes) {
			delete pnew_category_prefixes;
			pnew_category_prefixes = NULL;
		}
		defaultStore.reset();
	}

	if (!perfect_config || !enough_config_to_run) {
		// 配置是完整的,但不产生效果，不予理睬
		setStatus(WARNING);
	} else {
		setStatusDetails("");
		setStatus(ALIVE);
	}
}

/**
 * 删除pcats和它其中的所有内容
 */
void forwarderHandler::deleteCategoryMap(category_map_t *pcats) {
	if (!pcats) {
		return;
	}
	for (category_map_t::iterator cat_iter = pcats->begin(); cat_iter != pcats->end(); ++cat_iter) {
		shared_ptr<store_list_t> pstores = cat_iter->second;
		if (!pstores) {
			throw std::logic_error("deleteCategoryMap: iterator in category map holds null pointer");
		}
		for (store_list_t::iterator store_iter = pstores->begin(); store_iter != pstores->end(); ++store_iter) {
			if (!*store_iter) {
				throw std::logic_error("deleteCategoryMap: iterator in store map holds null pointer");
			}

			(*store_iter)->stop();
		} // for each store
		pstores->clear();
	} // for each category
	pcats->clear();
	delete pcats;
}
