#include "CloudxBase.h"

using namespace cloudx::base;
using apache::thrift::concurrency::Guard;

CloudxBase::CloudxBase(std::string name, get_static_limref_ptr reflect_lim) :
	name_(name) {
	aliveSince_ = (int64_t) time(NULL);
	if (reflect_lim) {
		reflect_lim(reflection_limited_);
	}
}

inline void CloudxBase::getName(std::string& _return) {
	_return = name_;
}

void CloudxBase::setOption(const std::string& key, const std::string& value) {
	Guard g(optionsLock_);
	options_[key] = value;
}

void CloudxBase::getOption(std::string& _return, const std::string& key) {
	Guard g(optionsLock_);
	_return = options_[key];
}

void CloudxBase::getOptions(std::map<std::string, std::string> & _return) {
	Guard g(optionsLock_);
	_return = options_;
}

int64_t CloudxBase::incrementCounter(const std::string& key, int64_t amount) {
	counters_.acquireRead();

	// 如果没有找到key, 就需要获取写锁来创建它, Double-check模式
	ReadWriteCounterMap::iterator it = counters_.find(key);
	if (it == counters_.end()) {
		counters_.release();
		counters_.acquireWrite();

		// 需要再次检查以确保没有人在此过程中创建此key


		if (it == counters_.end()) {
			counters_[key].value = amount;
			counters_.release();
			return amount;
		}
	}

	it->second.acquireWrite();
	int64_t count = it->second.value + amount;
	it->second.value = count;
	it->second.release();
	counters_.release();
	return count;
}

int64_t CloudxBase::setCounter(const std::string& key, int64_t value) {
	counters_.acquireRead();

	// 如果没有找到key, 就需要获取写锁来创建它, 再来Double-check模式
	ReadWriteCounterMap::iterator it = counters_.find(key);
	if (it == counters_.end()) {
		counters_.release();
		counters_.acquireWrite();
		counters_[key].value = value;
		counters_.release();
		return value;
	}

	it->second.acquireWrite();
	it->second.value = value;
	it->second.release();
	counters_.release();
	return value;
}

void CloudxBase::getCounters(std::map<std::string, int64_t>& _return) {
	// 这里只需要加读锁
	counters_.acquireRead();
	for (ReadWriteCounterMap::iterator it = counters_.begin(); it != counters_.end(); it++) {
		_return[it->first] = it->second.value;
	}
	counters_.release();
}

int64_t CloudxBase::getCounter(const std::string& key) {
	int64_t rv = 0;
	counters_.acquireRead();
	ReadWriteCounterMap::iterator it = counters_.find(key);
	if (it != counters_.end()) {
		it->second.acquireRead();
		rv = it->second.value;
		it->second.release();
	}
	counters_.release();
	return rv;
}

inline int64_t CloudxBase::aliveSince() {
	return aliveSince_;
}
