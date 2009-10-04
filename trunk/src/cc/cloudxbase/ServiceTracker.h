/**
 * ServiceTracker��ǰ�ṩ���¹���:
 *   . ����service������start, end(��duration), �м仹�п�ѡ�Ĳ���.
 *
 *   . ͨ������getStatus()���Զ����server��״̬. ���server�ҵ��˴˷������׳�ServiceException.
 *
 *   . ���ÿ�������Ļ��ڼ����������ͳ����Ϣ�ϱ�
 *
 *   . ����TThreadPoolServer, �������server�̶߳����ڷ�æ״̬.(������server��setThreadManager()����������֮�������Ч.)
 *
 * ���ݴ��빹�캯���Ĳ��������û��ֹ����.
 * ���Ը����캯���ṩһ��logging()����,���û���ṩ,��Ĭ�ϻ��logд��stdout.
 *
 * ServiceTracker�����˷�������,�رյ�˽�з���, ���ServiceMethod����:
 *
 *    #include <ServiceTracker.h>
 *    class MyServiceHandler : virtual public MyServiceIf, public cloudx::base::CloudxBase {
 *    public:
 *      MyServiceHandler::MyServiceHandler() : mServiceTracker(this) {}
 *      void MyServiceHandler::myServiceMethod(int userId) {
 *        // note: ʵ����һ��ServiceMethod����.
 *        ServiceMethod serviceMethod(&mServiceTracker, "myServiceMethod", userId);
 *        ...
 *        // ����step������֪ServiceTracker��¼�²���.
 *        serviceMethod.step("post parsing, begin processing");
 *        ...
 *        // ��ServiceMethod����������,ServiceTracker�ͻ�������ServiceMethodʱ��¼����ʱ���.
 *      }
 *      ...
 *    private:
 *      ServiceTracker mServiceTracker;
 *    }
 *
 * step()�����ǿ�ѡ��,; startService()��finishService()�����ֱ��ɶ���Ĺ��캯��������������ִ��.
 *
 * ServiceTracker�������̰߳�ȫ��.
 */

#ifndef SERVICETRACKER_H
#define SERVICETRACKER_H

#include <iostream>
#include <string>
#include <sstream>
#include <exception>
#include <map>
#include <boost/shared_ptr.hpp>

#include "thrift/concurrency/Mutex.h"

namespace apache {
namespace thrift {
namespace concurrency {
class ThreadManager;
}
}
}

namespace cloudx {
namespace base {

class CloudxBase;
class ServiceMethod;

class Stopwatch {
public:
	enum Unit {
		UNIT_SECONDS, UNIT_MILLISECONDS, UNIT_MICROSECONDS
	};
	Stopwatch();
	uint64_t elapsedUnits(Unit unit, std::string *label = NULL) const;
	void reset();
private:
	timeval startTime_;
};

class ServiceTracker {
	friend class ServiceMethod;

public:

	static uint64_t CHECKPOINT_MINIMUM_INTERVAL_SECONDS;
	static int LOG_LEVEL;

	ServiceTracker(cloudx::base::CloudxBase *handler, void(*logMethod)(int, const std::string &)
	= &ServiceTracker::defaultLogMethod, bool featureCheckpoint = true, bool featureStatusCheck = true, bool featureThreadCheck = true, Stopwatch::Unit stopwatchUnit = Stopwatch::UNIT_MILLISECONDS);

	void setThreadManager(boost::shared_ptr<apache::thrift::concurrency::ThreadManager> threadManager);

private:

	cloudx::base::CloudxBase *handler_;
	void (*logMethod_)(int, const std::string &);
	boost::shared_ptr<apache::thrift::concurrency::ThreadManager> threadManager_;

	bool featureCheckpoint_;
	bool featureStatusCheck_;
	bool featureThreadCheck_;
	Stopwatch::Unit stopwatchUnit_;

	apache::thrift::concurrency::Mutex statisticsMutex_;
	time_t checkpointTime_;
	uint64_t checkpointServices_;
	uint64_t checkpointDuration_;
	std::map<std::string, std::pair<uint64_t, uint64_t> > checkpointServiceDuration_;

	void startService(const ServiceMethod &serviceMethod);
	int64_t stepService(const ServiceMethod &serviceMethod, const std::string &stepName);
	void finishService(const ServiceMethod &serviceMethod);
	void reportCheckpoint();
	static void defaultLogMethod(int level, const std::string &message);
};

class ServiceMethod {
	friend class ServiceTracker;
public:
	ServiceMethod(ServiceTracker *tracker, const std::string &name, const std::string &signature, bool featureLogOnly = false);
	ServiceMethod(ServiceTracker *tracker, const std::string &name, uint64_t id, bool featureLogOnly = false);
	~ServiceMethod();
	uint64_t step(const std::string &stepName);
private:
	ServiceTracker *tracker_;
	std::string name_;
	std::string signature_;
	bool featureLogOnly_;
	Stopwatch timer_;
};

class ServiceException: public std::exception {
public:
	explicit ServiceException(const std::string &message, int code = 0) :
		message_(message), code_(code) {
	}
	~ServiceException() throw() {
	}
	virtual const char *what() const throw() {
		return message_.c_str();
	}
	int code() const throw() {
		return code_;
	}
private:
	std::string message_;
	int code_;
};

}
} // cloudx::base

#endif
