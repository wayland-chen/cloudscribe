/**
 * ServiceTracker当前提供如下功能:
 *   . 记载service方法的start, end(和duration), 中间还有可选的步骤.
 *
 *   . 通过调用getStatus()来自动监测server的状态. 如果server挂掉了此方法会抛出ServiceException.
 *
 *   . 针对每个方法的基于检查点的周期性统计信息上报
 *
 *   . 对于TThreadPoolServer, 如果所有server线程都处于繁忙状态.(必须在server的setThreadManager()方法被调用之后才能生效.)
 *
 * 根据传入构造函数的参数来启用或禁止特性.
 * 可以给构造函数提供一个logging()方法,如果没有提供,则默认会把log写到stdout.
 *
 * ServiceTracker定义了服务启动,关闭等私有方法, 搞个ServiceMethod对象:
 *
 *    #include <ServiceTracker.h>
 *    class MyServiceHandler : virtual public MyServiceIf, public cloudx::base::CloudxBase {
 *    public:
 *      MyServiceHandler::MyServiceHandler() : mServiceTracker(this) {}
 *      void MyServiceHandler::myServiceMethod(int userId) {
 *        // note: 实例化一个ServiceMethod对象.
 *        ServiceMethod serviceMethod(&mServiceTracker, "myServiceMethod", userId);
 *        ...
 *        // 调用step方法告知ServiceTracker记录下步骤.
 *        serviceMethod.step("post parsing, begin processing");
 *        ...
 *        // 当ServiceMethod超过作用域,ServiceTracker就会在析构ServiceMethod时记录调用时间等.
 *      }
 *      ...
 *    private:
 *      ServiceTracker mServiceTracker;
 *    }
 *
 * step()方法是可选的,; startService()和finishService()方法分别由对象的构造函数和析构函数来执行.
 *
 * ServiceTracker必须是线程安全的.
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
