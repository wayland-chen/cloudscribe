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
 * ����ʵ����һ�����к�һ���߳����ڷַ��¼���store. ����ACE��Task��ʵ��
 * ������ָ������������store.
 */
class StoreQueue {
public:
	StoreQueue(const std::string& type, const std::string& category, unsigned check_period, bool is_model = false, bool multi_category = false);
	StoreQueue(const boost::shared_ptr<StoreQueue> example, const std::string &category);
	virtual ~StoreQueue();

	void addMessage(logentry_ptr_t entry);

	void configureAndOpen(pStoreConf configuration); // closes first if already open

	void open(); // ����Ѿ������ȹر�

	void stop();
	boost::shared_ptr<Store> copyStore(const std::string &category);
	std::string getStatus(); // ������ؿ��ַ��������OK, �����������������д���
	std::string getBaseType();
	std::string getCategoryHandled();
	bool isModelStore() {
		return isModel;
	}

	// ��task�̵߳���Ҫ�߼�������.
	void threadMember();

	// ���ֻ������������.
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

	// ��Ϣ��������ڲ�ͬ�Ķ����������������������Ϣ. ���������Ļ�, ��Ϣ������֮����жϲ���˳����.
	cmd_queue_t cmdQueue;
	boost::shared_ptr<logentry_vector_t> msgQueue;
	unsigned long msgQueueSize;
	pthread_t storeThread;

	// Mutexes
	pthread_mutex_t cmdMutex; // ���ƶ�cmdQueue��read/modify����
	pthread_mutex_t msgMutex; // ���ƶ�msgQueue��read/modify����
	pthread_mutex_t hasWorkMutex; // ���ƶ�hasWork�Ĳ���
	// ���Ҫ��ö��mutex, ȷ��������˳�����(���ⷢ������):
	// {cmdMutex, msgMutex, hasWorkMutex}

	bool hasWork; // �����������Ƿ�����Ϣ���������
	pthread_cond_t hasWorkCond; // ��hasWork�����ȴ�����������

	bool stopping;
	bool isModel;
	bool multiCategory; // �Ƿ�����ڴ��������

	std::string categoryHandled; // ��store�������������
	time_t checkPeriod; // ����periodicCheck������(ʱ�䵥λΪsecond)
	unsigned long targetWriteSize; // ��λΪbyte
	time_t maxWriteInterval; // ��λΪsecond

	// Store������Ϣ�ľ���洢.
	boost::shared_ptr<Store> store;
};

#endif //!defined FORWARDER_STORE_QUEUE_H
