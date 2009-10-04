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
 * ���Ʋ��������ϱ�ͳ��(ͨ����־���� ������).
 * ���serverû��׼���ô���service method, ���׳��쳣.
 *
 * @param const ServiceMethod &serviceMethod ����һ���ڷ��񷽷���ʼʵ������ServiceMethod����.
 */
void ServiceTracker::startService(const ServiceMethod &serviceMethod) {
	// ע��: serviceMethod.timer_�ڹ��캯�����Զ�����.
	// log��������
	logMethod_(5, serviceMethod.signature_);

	// ���handler�Ƿ�OK
	if (featureStatusCheck_ && !serviceMethod.featureLogOnly_) {
		// ע��: һ������STOPPING״̬��server�������������, ��������Ȼ���Դ�����ǰ������
		cloudx::base::base_status status = handler_->getStatus();
		if (status != cloudx::base::ALIVE && status != cloudx::base::STOPPING) {
			if (status == cloudx::base::STARTING) {
				throw ServiceException("Server starting up; please try again later");
			} else {
				throw ServiceException("Server not alive; please try again later");
			}
		}
	}

	// ���server���߳�
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
 * ��service�����м�¼�ؼ�����.
 * @param const ServiceMethod &serviceMethod һ��ServiceMethod���������.
 * @return int64_t ��ServiceMethod���������ŵ�ʱ��
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
 * service method����ֹ
 * @param const ServiceMethod &serviceMethod һ��ServiceMethod���������.
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
			// (ע��: �������checkpoint�Ļ��Ͳ���Ҫlock statisticsMutex_;
			// TsossService::incrementCounter()�Ѿ����̰߳�ȫ��.)
			handler_->incrementCounter("lifetime_services");
		} else {
			statisticsMutex_.lock();
			// ע��: ����������Ӧ�ò����׳��쳣������Ϊ�˱������
			try {
				// lifetime ������
				// ע��: ��ð�����ͼ���������ͬ��, ��ȻincrementCounter()���̰߳�ȫ��,
				// Ϊ�˼����ϱ���һ����(i.e.  ���ϴμ�������, lifetime_services��checkpointServices_������).
				handler_->incrementCounter("lifetime_services");

				// ���������
				++checkpointServices_;
				checkpointDuration_ += duration;

				// per-service timing
				// ע��: �����ҵĲ���insert()��find()�Ҳ�������insertҪ��.
				// ���Ƕ����������ݵ�map��˵����С. ��Ч�ķ����ɶ��ԱȽϲ�.
				map<string, pair<uint64_t, uint64_t> >::iterator iter;
				iter = checkpointServiceDuration_.find(serviceMethod.name_);
				if (iter != checkpointServiceDuration_.end()) {
					++iter->second.first;
					iter->second.second += duration;
				} else {
					checkpointServiceDuration_.insert(make_pair(serviceMethod.name_, make_pair(1, duration)));
				}

				// �������ڱ������
				// ע��: ...�������ϴ��ϱ������Ѿ����˺ܳ�ʱ��
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
 * logһЩ���ϴε���������ͳ����Ϣ.
 *
 * ע��: �ڴ˷����ϵ��߳̾����ᵼ���󱨻�δ�������Ϊ;
 * �����߱���ʹ��mutex�������Գ�Ա�����ͺ�����ʹ��.
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

	// ���ü������
	// ע��: ������mapʱ�������߳���ʹ�ô�map, ������δ�������Ϊ.
	checkpointServiceDuration_.clear();
	checkpointTime_ = now;
	checkpointServices_ = 0;
	checkpointDuration_ = 0;

	// ��ȡlifetime����
	uint64_t life_count = handler_->getCounter("lifetime_services");
	uint64_t life_interval = now - handler_->aliveSince();

	// �ǼǼ���
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
 * ��server��ʹ�õ�thread manager�Ǽ�����, ���ڼ���̵߳Ļ���.
 *
 * @param shared_ptr<ThreadManager> threadManager server���̹߳�����.
 */
void ServiceTracker::setThreadManager(boost::shared_ptr<ThreadManager> threadManager) {
	threadManager_ = threadManager;
}

/**
 * ����Ϣ�Ǽǵ�stdout; ��LOG_LEVEL����ߵ���Ϣ���ᱻ��ӡ����.
 * ����ServiceTrackerĬ�ϵ�log����. �����ڹ��캯����ָ�������.
 *
 * @param int level ��Ϣ�ļ���
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
 * ����һ�����Ź�, �����ϱ��������е�ʱ��.
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
 * ����һ��ServiceMethod, ����׷�ٵ�����������(ͨ��ServiceTracker).
 * ����ServiceMethod��name������Ϊ�˱��ڷ���ͳ��(e.g. �������͵���ʱ��);
 * signature��������Ψһ��ʶһ�ֵ���.
 * @param ServiceTracker *tracker ���ڸ���ServiceMethod.
 * @param const string &name service����������(ͨ��������service�����Ĳ���).
 * @param const string &signature Ψһ��ʾ������ǩ��(ͨ�������Ƽ��ϲ���).
 */
ServiceMethod::ServiceMethod(ServiceTracker *tracker, const string &name, const string &signature, bool featureLogOnly) :
	tracker_(tracker), name_(name), signature_(signature), featureLogOnly_(featureLogOnly) {
	// ע��: timer_�ڹ��캯�����Զ�����.

	// ����tracker������service
	// ע��: ���ܻ��׳��쳣.  ����׳��쳣, �˶���Ĺ��캯�������ᱻ����.
	tracker_->startService(*this);
}

ServiceMethod::ServiceMethod(ServiceTracker *tracker, const string &name, uint64_t id, bool featureLogOnly) :
	tracker_(tracker), name_(name), featureLogOnly_(featureLogOnly) {
	// ע��: timer_�ڹ��캯�����Զ�����.
	stringstream ss_signature;
	ss_signature << name << " (" << id << ')';
	signature_ = ss_signature.str();

	// ����tracker������service
	// ע��: ���ܻ��׳��쳣.  ����׳��쳣, �˶���Ĺ��캯�������ᱻ����.
	tracker_->startService(*this);
}

ServiceMethod::~ServiceMethod() {
	// ����tracker����ֹservice
	// ע��: finishService()Ӧ�ò����׳��쳣,ֻ�ǻ������׳�out-of-memory�Ŀ���.
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
