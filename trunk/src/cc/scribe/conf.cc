#include "conf.h"

#include <sstream>

#include <boost/algorithm/string/trim.hpp>

#include "logger.h"
#include "utils.h"


using namespace boost;
using namespace std;

StoreConf::StoreConf() {
}

StoreConf::~StoreConf() {
}

bool StoreConf::getStore(const std::string& storeName, pStoreConf& _return) {
	store_conf_map_t::iterator iter = stores.find(storeName);
	if (iter != stores.end()) {
		_return = iter->second;
		return true;
	} else {
		return false;
	}
}

void StoreConf::getAllStores(std::vector<pStoreConf>& _return) {
	for (store_conf_map_t::iterator iter = stores.begin(); iter != stores.end(); ++iter) {
		_return.push_back(iter->second);
	}
}

bool StoreConf::getInt(const std::string& intName, long int& _return) {
	string str;
	if (getString(intName, str)) {
		_return = strtol(str.c_str(), NULL, 0);
		return true;
	} else {
		return false;
	}
}

bool StoreConf::getUnsigned(const std::string& intName, unsigned long int& _return) {
	string str;
	if (getString(intName, str)) {
		_return = strtoul(str.c_str(), NULL, 0);
		return true;
	} else {
		return false;
	}
}

bool StoreConf::getString(const std::string& stringName, std::string& _return) {
	string_map_t::iterator iter = values.find(stringName);
	if (iter != values.end()) {
		_return = iter->second;
		return true;
	} else {
		return false;
	}
}

void StoreConf::setString(const std::string& stringName, const std::string& value) {
	values[stringName] = value;
}

void StoreConf::setUnsigned(const std::string& stringName, unsigned long value) {
	ostringstream oss;
	oss << value;
	setString(stringName, oss.str());
}

// reads and parses the config data
void StoreConf::parseConfig(const std::string& filename) {

	queue<string> config_strings;

	if (readConfFile(filename, config_strings)) {
		LOG_OPER("got configuration data from file <%s>", filename.c_str());
	} else {
		std::ostringstream msg;
		msg << "Failed to open config file <" << filename << ">";
		throw std::runtime_error(msg.str());
	}

	parseStore(config_strings, this);
}

// Side-effects:  - removes items from raw_config and adds items to parsed_config
//
// Returns true if a valid entry was found
bool StoreConf::parseStore(queue<string>& raw_config, /*out*/ StoreConf* parsed_config) {

	int store_index = 0; // used to give things named "store" different names

	string line;
	while (!raw_config.empty()) {
		line = raw_config.front();
		boost::algorithm::trim(line);

		raw_config.pop();

		int length = line.size();
		if (0 >= length || line[0] == '#') {
			continue;
		}
		if (line[0] == '<') {

			if (length > 1 && line[1] == '/') {
				// This is the end of the current store
				return true;
			}

			// This is the start of a new store
			string::size_type pos = line.find('>');
			if (pos == string::npos) {
				LOG_OPER("Bad config - line %s has a < but not a >", line.c_str());
				continue;
			}
			string store_name = line.substr(1, pos - 1);

			pStoreConf new_store(new StoreConf);
			if (parseStore(raw_config, new_store.get())) {
				if (0 == store_name.compare("store")) {
					// This is a special case for the top-level stores. They share
					// the same name, so we append an index to put them in the map
					std::ostringstream oss;
					oss << store_index;
					store_name += oss.str();
					++store_index;
				}
				if (parsed_config->stores.find(store_name) != parsed_config->stores.end()) {
					LOG_OPER("Bad config - duplicate store name %s", store_name.c_str());
				}
				parsed_config->stores[store_name] = new_store;
			}
		} else {
			string::size_type eq = line.find('=');
			if (eq == string::npos) {
				LOG_OPER("Bad config - line %s is missing an =", line.c_str());
			} else {
				string arg = line.substr(0, eq);
				string val = line.substr(eq + 1, string::npos);
				if (parsed_config->values.find(arg) != parsed_config->values.end()) {
					LOG_OPER("Bad config - duplicate key %s", arg.c_str());
				}
				parsed_config->values[arg] = val;
			}
		}
	}
	return true;
}

// reads every line from the file and pushes then onto _return
// returns false on error
bool StoreConf::readConfFile(const string& filename, queue<string>& _return) {
	std::string line;
	std::ifstream config_file;

	config_file.open(filename.c_str());
	if (!config_file.good()) {
		return false;
	}

	while (std::getline(config_file, line)) {
		_return.push(line);
	}

	config_file.close();
	return true;
}
