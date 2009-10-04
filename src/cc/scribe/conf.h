/**
 * author: edisonpeng@tencent.com
 */
#ifndef FORWARDER_CONF_H
#define FORWARDER_CONF_H

#include <string>
#include <vector>
#include <queue>
#include <iostream>
#include <fstream>

#include "gen-cpp/forwarder.h"

/*
 * ���������ļ��Ķ�ȡ�ͽ���.
 * Ҳ�����滻��xml��ʽ��.ֻ��Ҫ�޸Ĵ���Ĵ��뼴��.
 */
class StoreConf;
typedef boost::shared_ptr<StoreConf> pStoreConf;
typedef std::map<std::string, std::string> string_map_t;
typedef std::map<std::string, pStoreConf> store_conf_map_t;

class StoreConf {
	public:
		StoreConf();
		virtual ~StoreConf();
		void getAllStores(std::vector<pStoreConf>& _return);
		bool getStore(const std::string& storeName, pStoreConf& _return);
		bool getInt(const std::string& intName, long int& _return);
		bool getUnsigned(const std::string& intName, unsigned long int& _return);
		bool getString(const std::string& stringName, std::string& _return);

		void setString(const std::string& stringName, const std::string& value);
		void setUnsigned(const std::string& intName, unsigned long value);

		// ���ļ��ж�ȡ����,���ʧ�����׳��쳣.
		void parseConfig(const std::string& filename);

	private:
		string_map_t values;
		store_conf_map_t stores;

		static bool parseStore(/*in,out*/ std::queue<std::string>& raw_config, /*out*/ StoreConf* parsed_config);
		bool readConfFile(const std::string& filename, std::queue<std::string>& _return);
};

#endif //!defined FORWARDER_CONF_H
