#include <sys/time.h>

#include "CloudxBase.h"
#include "ServiceTracker.h"
#include "thrift/concurrency/ThreadManager.h"

using namespace std;
using namespace cloudx::base;
using namespace apache::thrift::concurrency;


uint64_t ServiceTracker::CHECKPOINT_MINIMUM_INTERVAL_SECONDS = 60;
int ServiceTracker::LOG_LEVEL = 5;



ServiceTracker::ServiceTracker(cloudx::base::CloudxBase *handler, void(*logMethod)(int, const string &), bool featureCheckpoint, bool featureStatusCheck, bool featureThreadCheck,
		Stopwatch::Unit stopwatchUnit) :
	handler_(handler), logMethod_(logMethod), featureCheckpoint_(featureCheckpoint), featureStatusCheck_(featureStatusCheck), featureThreadCheck_(featureThreadCheck), stopwatchUnit_(stopwatchUnit),
			checkpointServices_(0) {
	if (featureCheckpoint_) {
		time_t now = time(NULL);
		checkpointTime_ = now;
	} else {
		checkpointTime_ = 0;
	}
}

/**
 * 控制并发服务并上报统计(通过日志或者 计数器).
 * 如果server没有准备好处理service method, 就抛出异常.
 *
 * @param const ServiceMethod &serviceMethod 引用一个在服务方法开始实例化的ServiceMethod对象.
 */
void ServiceTracker::startService(const ServiceMethod &serviceMethod) {
	// 注意: serviceMethod.timer_在构造函数中自动启动.
	// log服务启动
	logMethod_(5, serviceMethod.signature_);

	// 检查handler是否OK
	if (featureStatusCheck_ && !serviceMethod.featureLogOnly_) {
		// 注意: 一个正在STOPPING状态的server不会接收新连接, 但是它仍然可以处理先前的连接
		cloudx::base::base_status status = handler_->getStatus();
		if (status != cloudx::base::ALIVE && status != cloudx::base::STOPPING) {
			if (status == cloudx::base::STARTING) {
				throw ServiceException("Server starting up; please try again later");
			} else {
				throw ServiceException("Server not alive; please try again later");
			}
		}
	}

	// 检查server的线程
	if (featureThreadCheck_ && !serviceMethod.featureLogOnly_) {
		if (threadManager_ != NULL) {
			size_t idle_count = threadManager_->idleWorkerCount();
			if (idle_count == 0) {
				stringstream message;
				message << "service " << serviceMethod.signature_ << ": all threads (" << threadManager_->workerCount() << ") in use";
				logMethod_(3, message.str());
			}
		}
	}
}

/**
 * 在service方法中记录关键步骤.
 * @param const ServiceMethod &serviceMethod 一个ServiceMethod对象的引用.
 * @return int64_t 离ServiceMethod构造所流逝的时间
 */
int64_t ServiceTracker::stepService(const ServiceMethod &serviceMethod, const string &stepName) {
	stringstream message;
	string elapsed_label;
	int64_t elapsed = serviceMethod.timer_.elapsedUnits(stopwatchUnit_, &elapsed_label);
	message << serviceMethod.signature_ << ' ' << stepName << " [" << elapsed_label << ']';
	logMethod_(5, message.str());
	return elapsed;
}

/**
 * service method的终止
 * @param const ServiceMethod &serviceMethod 一个ServiceMethod对象的引用.
 */
void ServiceTracker::finishService(const ServiceMethod &serviceMethod) {
	// log end of service
	stringstream message;
	string duration_label;
	int64_t duration = serviceMethod.timer_.elapsedUnits(stopwatchUnit_, &duration_label);
	message << serviceMethod.signature_ << " finish [" << duration_label << ']';
	logMethod_(5, message.str());

	// count, record, and maybe report service statistics
	if (!serviceMethod.featureLogOnly_) {
		if (!featureCheckpoint_) {

			// lifetime counters
			// (注意: 如果不做checkpoint的话就不需要lock statisticsMutex_;
			// TsossService::incrementCounter()已经是线程安全的.)
			handler_->incrementCounter("lifetime_services");
		} else {
			statisticsMutex_.lock();
			// 注意: 这里理论上应该不会抛出异常，但是为了保险起见
			try {
				// lifetime 计数器
				// 注意: 最好把这个和检查点服务进行同步, 虽然incrementCounter()是线程安全的,
				// 为了检查点上报的一致性(i.e.  从上次检查点以来, lifetime_services被checkpointServices_增加了).
				handler_->incrementCounter("lifetime_services");

				// 检查点计数器
				++checkpointServices_;
				checkpointDuration_ += duration;

				// per-service timing
				// 注意: 根据我的测试insert()比find()找不到后再insert要快.
				// 但是对于少量数据的map来说差别很小. 高效的方案可读性比较差.
				map<string, pair<uint64_t, uint64_t> >::iterator iter;
				iter = checkpointServiceDuration_.find(serviceMethod.name_);
				if (iter != checkpointServiceDuration_.end()) {
					++iter->second.first;
					iter->second.second += duration;
				} else {
					checkpointServiceDuration_.insert(make_pair(serviceMethod.name_, make_pair(1, duration)));
				}

				// 可能用于报告检查点
				// 注意: ...加入自上次上报以来已经过了很长时间
				time_t now = time(NULL);
				uint64_t check_interval = now - checkpointTime_;
				if (check_interval >= CHECKPOINT_MINIMUM_INTERVAL_SECONDS) {
					reportCheckpoint();
				}

			} catch (...) {
				statisticsMutex_.unlock();
				throw ;
			}
			statisticsMutex_.unlock();

		}
	}
}

/**
 * log一些自上次调用以来的统计信息.
 *
 * 注意: 在此方法上的线程竞争会导致误报或未定义的行为;
 * 调用者必须使用mutex来保护对成员变量和函数的使用.
 */
void ServiceTracker::reportCheckpoint() {
	time_t now = time(NULL);

	uint64_t check_count = checkpointServices_;
	uint64_t check_interval = now - checkpointTime_;
	uint64_t check_duration = checkpointDuration_;

	handler_->setCounter("checkpoint_time", check_interval);
	map<string, pair<uint64_t, uint64_t> >::iterator iter;
	uint64_t count;
	for (iter = checkpointServiceDuration_.begin(); iter != checkpointServiceDuration_.end(); iter++) {
		count = iter->second.first;
		handler_->setCounter(string("checkpoint_count_") + iter->first, count);
		if (count == 0) {
			handler_->setCounter(string("checkpoint_speed_") + iter->first, 0);
		} else {
			handler_->setCounter(string("checkpoint_speed_") + iter->first, iter->second.second / count);
		}
	}

	// 重置检查点变量
	// 注意: 如果清空map时有其它线程在使用此map, 将导致未定义的行为.
	checkpointServiceDuration_.clear();
	checkpointTime_ = now;
	checkpointServices_ = 0;
	checkpointDuration_ = 0;

	// 获取lifetime变量
	uint64_t life_count = handler_->getCounter("lifetime_services");
	uint64_t life_interval = now - handler_->aliveSince();

	// 登记检查点
	stringstream message;
	message << "checkpoint_time:" << check_interval << " checkpoint_services:" << check_count << " checkpoint_speed_sum:" << check_duration << " lifetime_time:" << life_interval
			<< " lifetime_services:" << life_count;
	if (featureThreadCheck_ && threadManager_ != NULL) {
		size_t worker_count = threadManager_->workerCount();
		size_t idle_count = threadManager_->idleWorkerCount();
		message << " total_workers:" << worker_count << " active_workers:" << (worker_count - idle_count);
	}
	logMethod_(4, message.str());
}

/**
 * 把server在使用的thread manager登记下来, 用于监控线程的活动情况.
 *
 * @param shared_ptr<ThreadManager> threadManager server的线程管理器.
 */
void ServiceTracker::setThreadManager(boost::shared_ptr<ThreadManager> threadManager) {
	threadManager_ = threadManager;
}

/**
 * 把消息登记到stdout; 比LOG_LEVEL级别高的消息都会被打印出来.
 * 这是ServiceTracker默认的log方法. 可以在构造函数中指定替代物.
 *
 * @param int level 消息的级别
 * @param string message
 */
void ServiceTracker::defaultLogMethod(int level, const string &message) {
	if (level <= LOG_LEVEL) {
		string level_string;
		time_t now = time(NULL);
		char now_pretty[26];
		ctime_r(&now, now_pretty);
		now_pretty[24] = '\0';
		switch (level) {
		case 1:
			level_string = "CRITICAL";
			break;
		case 2:
			level_string = "ERROR";
			break;
		case 3:
			level_string = "WARNING";
			break;
		case 5:
			level_string = "DEBUG";
			break;
		case 4:
		default:
			level_string = "INFO";
			break;
		}
		cout << '[' << level_string << "] [" << now_pretty << "] " << message << endl;
	}
}

/* Start of Stopwatch */
/**
 * 创建一个看门狗, 用于上报服务运行的时间.
 */
Stopwatch::Stopwatch() {
	gettimeofday(&startTime_, NULL);
}

void Stopwatch::reset() {
	gettimeofday(&startTime_, NULL);
}

uint64_t Stopwatch::elapsedUnits(Stopwatch::Unit unit, string *label) const {
	timeval now_time;
	gettimeofday(&now_time, NULL);
	time_t duration_secs = now_time.tv_sec - startTime_.tv_sec;

	uint64_t duration_units;
	switch (unit) {
	case UNIT_SECONDS:
		duration_units = duration_secs + (now_time.tv_usec - startTime_.tv_usec + 500000) / 1000000;
		if (NULL != label) {
			stringstream ss_label;
			ss_label << duration_units << " secs";
			label->assign(ss_label.str());
		}
		break;
	case UNIT_MICROSECONDS:
		duration_units = duration_secs * 1000000 + now_time.tv_usec - startTime_.tv_usec;
		if (NULL != label) {
			stringstream ss_label;
			ss_label << duration_units << " us";
			label->assign(ss_label.str());
		}
		break;
	case UNIT_MILLISECONDS:
	default:
		duration_units = duration_secs * 1000 + (now_time.tv_usec - startTime_.tv_usec + 500) / 1000;
		if (NULL != label) {
			stringstream ss_label;
			ss_label << duration_units << " ms";
			label->assign(ss_label.str());
		}
		break;
	}
	return duration_units;
}

/* Start of ServiceMethod */

/**
 * 创建一个ServiceMethod, 用于追踪单个方法调用(通过ServiceTracker).
 * 传给ServiceMethod的name参数是为了便于分组统计(e.g. 计数器和调用时间);
 * signature参数用于唯一标识一种调用.
 * @param ServiceTracker *tracker 用于跟踪ServiceMethod.
 * @param const string &name service方法的名称(通常独立于service方法的参数).
 * @param const string &signature 唯一标示方法的签名(通常是名称加上参数).
 */
ServiceMethod::ServiceMethod(ServiceTracker *tracker, const string &name, const string &signature, bool featureLogOnly) :
	tracker_(tracker), name_(name), signature_(signature), featureLogOnly_(featureLogOnly) {
	// 注意: timer_在构造函数中自动启动.

	// 调用tracker来启动service
	// 注意: 可能会抛出异常.  如果抛出异常, 此对象的构造函数将不会被调用.
	tracker_->startService(*this);
}

ServiceMethod::ServiceMethod(ServiceTracker *tracker, const string &name, uint64_t id, bool featureLogOnly) :
	tracker_(tracker), name_(name), featureLogOnly_(featureLogOnly) {
	// 注意: timer_在构造函数中自动启动.
	stringstream ss_signature;
	ss_signature << name << " (" << id << ')';
	signature_ = ss_signature.str();

	// 调用tracker来启动service
	// 注意: 可能会抛出异常.  如果抛出异常, 此对象的构造函数将不会被调用.
	tracker_->startService(*this);
}

ServiceMethod::~ServiceMethod() {
	// 调用tracker来终止service
	// 注意: finishService()应该不会抛出异常,只是或许有抛出out-of-memory的可能.
	try {
		tracker_->finishService(*this);
	} catch (...) {
		// don't throw
	}
}

uint64_t ServiceMethod::step(const std::string &stepName) {
	return tracker_->stepService(*this, stepName);
}
/* End of ServiceMethod */
