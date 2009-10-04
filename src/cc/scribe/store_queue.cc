//#include "common.h"
#include "store_queue.h"

#include "forwarder_server.h"
#include "logger.h"



using namespace std;
using namespace boost;
using namespace forwarder::thrift;

#define DEFAULT_TARGET_WRITE_SIZE  16384
#define DEFAULT_MAX_WRITE_INTERVAL 10

void* threadStatic(void *this_ptr) {
	StoreQueue *queue_ptr = (StoreQueue*) this_ptr;
	queue_ptr->threadMember();
	return NULL;
}

StoreQueue::StoreQueue(const string& type, const string& category, unsigned check_period, bool is_model, bool multi_category) :
	msgQueueSize(0), hasWork(false), stopping(false), isModel(is_model), multiCategory(multi_category), categoryHandled(category), checkPeriod(check_period),
			targetWriteSize(DEFAULT_TARGET_WRITE_SIZE),
			maxWriteInterval(DEFAULT_MAX_WRITE_INTERVAL) {

	store = Store::createStore(type, category, false, multiCategory);
	if (!store) {
		throw std::runtime_error("createStore failed in StoreQueue constructor. Invalid type?");
	}
	storeInitCommon();
}

StoreQueue::StoreQueue(const shared_ptr<StoreQueue> example, const std::string &category) :
	msgQueueSize(0), hasWork(false), stopping(false), isModel(false), multiCategory(example->multiCategory), categoryHandled(category), checkPeriod(example->checkPeriod), targetWriteSize(
			example->targetWriteSize), maxWriteInterval(example->maxWriteInterval) {

	store = example->copyStore(category);
	if (!store) {
		throw std::runtime_error("createStore failed copying model store");
	}
	storeInitCommon();
}

StoreQueue::~StoreQueue() {
	if (!isModel) {
		pthread_mutex_destroy(&cmdMutex);
		pthread_mutex_destroy(&msgMutex);
		pthread_mutex_destroy(&hasWorkMutex);
		pthread_cond_destroy(&hasWorkCond);
	}
}

unsigned long StoreQueue::getSize() {
	unsigned long retval;
	pthread_mutex_lock(&msgMutex);
	retval = msgQueueSize;
	pthread_mutex_unlock(&msgMutex);
	return retval;
}

void StoreQueue::addMessage(boost::shared_ptr<LogEntry> entry) {
	if (isModel) {
		LOG_OPER("ERROR: called addMessage on model store");
	} else {
		pthread_mutex_lock(&msgMutex);
		msgQueue->push_back(entry);
		msgQueueSize += entry->message.size();
		pthread_mutex_unlock(&msgMutex);

		// 如果消息缓冲到一定数量,就唤醒存储线程
		if (msgQueueSize >= targetWriteSize) {
			//hasWork作为唤醒开关,可能先前不久有其它入口已经通知过了
			if (!hasWork) {
				pthread_mutex_lock(&hasWorkMutex);
				hasWork = true;
				pthread_cond_signal(&hasWorkCond);
				pthread_mutex_unlock(&hasWorkMutex);
			}
		}
	}
}

void StoreQueue::configureAndOpen(pStoreConf configuration) {
	// 如果是model,就修改下model的配置
	if (isModel) {
		configureInline(configuration);
	} else {
		pthread_mutex_lock(&cmdMutex);
		StoreCommand cmd(CMD_CONFIGURE, configuration);
		cmdQueue.push(cmd);
		pthread_mutex_unlock(&cmdMutex);

		// 通知队列处理命令
		if (!hasWork) {
			pthread_mutex_lock(&hasWorkMutex);
			hasWork = true;
			pthread_cond_signal(&hasWorkCond);
			pthread_mutex_unlock(&hasWorkMutex);
		}
	}
}

void StoreQueue::stop() {
	if (isModel) {
		LOG_OPER("ERROR: called stop() on model store");
	} else if (!stopping) {
		pthread_mutex_lock(&cmdMutex);
		StoreCommand cmd(CMD_STOP);
		cmdQueue.push(cmd);
		stopping = true;
		pthread_mutex_unlock(&cmdMutex);

		// 通知队列处理命令
		if (!hasWork) {
			pthread_mutex_lock(&hasWorkMutex);
			hasWork = true;
			pthread_cond_signal(&hasWorkCond);
			pthread_mutex_unlock(&hasWorkMutex);
		}

		pthread_join(storeThread, NULL);
	}
}

void StoreQueue::open() {
	if (isModel) { //不要搞原型model阿
		LOG_OPER("ERROR: called open() on model store");
	} else {
		pthread_mutex_lock(&cmdMutex);
		StoreCommand cmd(CMD_OPEN);
		cmdQueue.push(cmd);
		pthread_mutex_unlock(&cmdMutex);

		//
		if (!hasWork) {
			pthread_mutex_lock(&hasWorkMutex);
			hasWork = true;
			pthread_cond_signal(&hasWorkCond);
			pthread_mutex_unlock(&hasWorkMutex);
		}
	}
}

shared_ptr<Store> StoreQueue::copyStore(const std::string &category) {
	return store->copy(category);
}

std::string StoreQueue::getCategoryHandled() {
	return categoryHandled;
}

std::string StoreQueue::getStatus() {
	return store->getStatus();
}

std::string StoreQueue::getBaseType() {
	return store->getType();
}

void StoreQueue::threadMember() {
	LOG_OPER("store thread starting");

	if (isModel) {
		LOG_OPER("ERROR: store thread starting on model store, exiting");
		return;
	}

	if (!store) {
		LOG_OPER("store is NULL, store thread exiting");
		return;
	}

	// 搞下变量,状态的初始化工作
	time_t last_periodic_check = 0;

	time_t last_handle_messages;
	time(&last_handle_messages);

	struct timespec abs_timeout;
	memset(&abs_timeout, 0, sizeof(struct timespec));

	bool stop = false;
	bool open = false;
	while (!stop) {
		// 开始处理命令了
		pthread_mutex_lock(&cmdMutex);
		while (!cmdQueue.empty()) {
			StoreCommand cmd = cmdQueue.front();
			cmdQueue.pop();

			switch (cmd.command) {
			case CMD_CONFIGURE:
				configureInline(cmd.configuration);
				openInline();
				open = true;
				break;
			case CMD_OPEN:
				openInline();
				open = true;
				break;
			case CMD_STOP:
				stop = true;
				break;
			default:
				LOG_OPER("LOGIC ERROR: unknown command to store queue");
				break;
			}
		}

		// 处理周期性任务
		time_t this_loop;
		time(&this_loop);
		if (!stop && open && this_loop - last_periodic_check > checkPeriod) {
			store->periodicCheck();
			last_periodic_check = this_loop;
		}

		pthread_mutex_lock(&msgMutex);
		pthread_mutex_unlock(&cmdMutex);

		// 如果正在stopping状态,或者队列过大,或者时间间隔很久了,那么就要在这里处理一下消息
		if (stop || (this_loop - last_handle_messages > maxWriteInterval) || msgQueueSize >= targetWriteSize) {
			if (msgQueueSize > 0) {
				boost::shared_ptr<logentry_vector_t> messages = msgQueue;
				msgQueue = boost::shared_ptr<logentry_vector_t>(new logentry_vector_t);
				msgQueueSize = 0;

				pthread_mutex_unlock(&msgMutex);

				if (!store->handleMessages(messages)) {
					// 处理消息出了问题, 只好报丢失了
					LOG_OPER("[%s] WARNING: Lost %u messages!", categoryHandled.c_str(), messages->size());
					g_Handler->incrementCounter("lost", messages->size());
				}
				store->flush();
			} else {
				pthread_mutex_unlock(&msgMutex);
			}

			// 重置下计时器
			last_handle_messages = this_loop;
		} else {
			pthread_mutex_unlock(&msgMutex);
		}

		if (!stop) {
			// 设置handle消息或周期检查的超时时间
			abs_timeout.tv_sec = min(last_periodic_check + checkPeriod, last_handle_messages + maxWriteInterval);

			++abs_timeout.tv_sec;

			// 补下时间差而已了
			pthread_mutex_lock(&hasWorkMutex);
			if (!hasWork) {
				pthread_cond_timedwait(&hasWorkCond, &hasWorkMutex, &abs_timeout);
			}
			hasWork = false;
			pthread_mutex_unlock(&hasWorkMutex);
		}

	} // while (!stop)

	store->close();
}

void StoreQueue::storeInitCommon() {
	if (!isModel) {//不要搞原型model, 原型model只用作 原型模式作克隆.
		msgQueue = boost::shared_ptr<logentry_vector_t>(new logentry_vector_t);
		pthread_mutex_init(&cmdMutex, NULL);
		pthread_mutex_init(&msgMutex, NULL);
		pthread_mutex_init(&hasWorkMutex, NULL);
		pthread_cond_init(&hasWorkCond, NULL);

		pthread_create(&storeThread, NULL, threadStatic, (void*) this);
	}
}

void StoreQueue::configureInline(pStoreConf configuration) {
	configuration->getUnsigned("target_write_size", (unsigned long&) targetWriteSize);
	configuration->getUnsigned("max_write_interval", (unsigned long&) maxWriteInterval);

	store->configure(configuration);
}

void StoreQueue::openInline() {
	if (store->isOpen()) {
		store->close();
	}
	if (!isModel) {
		store->open();
	}
}
