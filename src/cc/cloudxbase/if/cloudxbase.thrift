#!/opt/thrift/bin/thrift --cpp --php

/**
 * 定义了通用的Tsoss数据类型和对所有Tsoss服务都通用的状态上报机制.
 * 可用一个基类来实现大部分接口.应用只需要实现小部分专有接口方法(比如状态上报).

 * @author edisonpeng@tencent.com
 */

include "reflection_limited.thrift"

namespace java com.cloudx.base
namespace cpp cloudx.base

/**
 * 所有服务的通用上报机制
 */
enum base_status {
  DEAD = 0,
  STARTING = 1,
  ALIVE = 2,
  STOPPING = 3,
  STOPPED = 4,
  WARNING = 5,
}

/**
 * Standard base service
 */
service CloudxService {
	/**
	 * 返回service的名称
	 */
	string getName(),

	/**
	 * 返回service的版本
	 */
	string getVersion(),

	/**
	 * 获取service的状态
	 */
	base_status getStatus(),

	/**
	 * 用户可见的状态描述信息,比如指明service因何原因处于dead或warning状态
	 */
	string getStatusDetails(),

	/**
	 * 获取service的计数器
	 */
	map<string, i64> getCounters(),

	/**
	 * 获取单个计数器的值
	 */
	i64 getCounter(1: string key),

	/**
	 * 设置选项
	 */
	void setOption(1: string key, 2: string value),

	/**
	 * 获取选项
	 */
	string getOption(1: string key),

	/**
	 * 获取所有选项信息
	 */
	map<string, string> getOptions(),

	/**
	 * 返回一段时期的CPU性能数据(client和server之间必须自行约定数据格式).
	 */
	string getCpuProfile(1: i32 profileDurationInSec),

	/**
	 * 返回service运行的总时间(UNIX时间格式)
	 */
	i64 aliveSince(),

	/**
	 * 返回关于service的一些元信息
	 */
	reflection_limited.Service getLimitedReflection(),

	/**
	 * 通知server重新装载配置,重新打开日志文件等
	 */
	oneway void reinitialize(),

	/**
	 * 建议server重新启动
	 */
	oneway void shutdown(),
}
