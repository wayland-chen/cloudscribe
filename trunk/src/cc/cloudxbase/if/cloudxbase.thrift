#!/opt/thrift/bin/thrift --cpp --php

/**
 * ������ͨ�õ�Tsoss�������ͺͶ�����Tsoss����ͨ�õ�״̬�ϱ�����.
 * ����һ��������ʵ�ִ󲿷ֽӿ�.Ӧ��ֻ��Ҫʵ��С����ר�нӿڷ���(����״̬�ϱ�).

 * @author edisonpeng@tencent.com
 */

include "reflection_limited.thrift"

namespace java com.cloudx.base
namespace cpp cloudx.base

/**
 * ���з����ͨ���ϱ�����
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
	 * ����service������
	 */
	string getName(),

	/**
	 * ����service�İ汾
	 */
	string getVersion(),

	/**
	 * ��ȡservice��״̬
	 */
	base_status getStatus(),

	/**
	 * �û��ɼ���״̬������Ϣ,����ָ��service���ԭ����dead��warning״̬
	 */
	string getStatusDetails(),

	/**
	 * ��ȡservice�ļ�����
	 */
	map<string, i64> getCounters(),

	/**
	 * ��ȡ������������ֵ
	 */
	i64 getCounter(1: string key),

	/**
	 * ����ѡ��
	 */
	void setOption(1: string key, 2: string value),

	/**
	 * ��ȡѡ��
	 */
	string getOption(1: string key),

	/**
	 * ��ȡ����ѡ����Ϣ
	 */
	map<string, string> getOptions(),

	/**
	 * ����һ��ʱ�ڵ�CPU��������(client��server֮���������Լ�����ݸ�ʽ).
	 */
	string getCpuProfile(1: i32 profileDurationInSec),

	/**
	 * ����service���е���ʱ��(UNIXʱ���ʽ)
	 */
	i64 aliveSince(),

	/**
	 * ���ع���service��һЩԪ��Ϣ
	 */
	reflection_limited.Service getLimitedReflection(),

	/**
	 * ֪ͨserver����װ������,���´���־�ļ���
	 */
	oneway void reinitialize(),

	/**
	 * ����server��������
	 */
	oneway void shutdown(),
}
