/**
 * @author: edisonpeng@tencent.com
 */
#ifndef FORWARDER_STORE_QUEUE_H
#define FORWARDER_STORE_QUEUE_H

#include <string>
#include <queue>
#include <vector>
#include <pthread.h>

#include "gen-cpp/forwarder.h"
#include "store.h"

/*
 * 此类实现了一个队列和一个线程用于分发事件到store. 类似ACE中Task的实现
 * 它根据指定类型来创建store.
 */
class StoreQueue {
public:
	StoreQueue(const std::string& type, const std::string& category, unsigned check_period, bool is_model = false, bool multi_category = false);
	StoreQueue(const boost::shared_ptr<StoreQueue> example, const std::string &category);
	virtual ~StoreQueue();

	void addMessage(logentry_ptr_t entry);

	void configureAndOpen(pStoreConf configuration); // closes first if already open

	void open(); // 如果已经打开则先关闭

	void stop();
	boost::shared_ptr<Store> copyStore(const std::string &category);
	std::string getStatus(); // 如果返回空字符串则代表OK, 如果返回其它则代表有错误
	std::string getBaseType();
	std::string getCategoryHandled();
	bool isModelStore() {
		return isModel;
	}

	// 新task线程的主要逻辑处理函数.
	void threadMember();

	// 这个只用作过载评估.
	unsigned long getSize();

private:
	void storeInitCommon();
	void configureInline(pStoreConf configuration);
	void openInline();

	enum store_command_t {
		CMD_CONFIGURE, CMD_OPEN, CMD_STOP
	};

	class StoreCommand {
	public:
		store_command_t command;
		pStoreConf configuration;

		StoreCommand(store_command_t cmd, pStoreConf config = pStoreConf()) :
			command(cmd), configuration(config) {
		}
	};

	typedef std::queue<StoreCommand> cmd_queue_t;

	// 消息和命令放在不同的队列里，这样允许批量处理消息. 不过这样的话, 消息和命令之间就判断不了顺序了.
	cmd_queue_t cmdQueue;
	boost::shared_ptr<logentry_vector_t> msgQueue;
	unsigned long msgQueueSize;
	pthread_t storeThread;

	// Mutexes
	pthread_mutex_t cmdMutex; // 控制对cmdQueue的read/modify操作
	pthread_mutex_t msgMutex; // 控制对msgQueue的read/modify操作
	pthread_mutex_t hasWorkMutex; // 控制对hasWork的操作
	// 如果要获得多个mutex, 确保以如下顺序进行(以免发生死锁):
	// {cmdMutex, msgMutex, hasWorkMutex}

	bool hasWork; // 表征队列中是否有消息或命令存在
	pthread_cond_t hasWorkCond; // 在hasWork上作等待的条件变量

	bool stopping;
	bool isModel;
	bool multiCategory; // 是否可用于处理多个类别

	std::string categoryHandled; // 此store负责处理哪种类别
	time_t checkPeriod; // 调用periodicCheck的周期(时间单位为second)
	unsigned long targetWriteSize; // 单位为byte
	time_t maxWriteInterval; // 单位为second

	// Store负责消息的具体存储.
	boost::shared_ptr<Store> store;
};

#endif //!defined FORWARDER_STORE_QUEUE_H
