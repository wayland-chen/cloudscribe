#ifndef CLOUDSCRIBE_UTILS_H
#define CLOUDSCRIBE_UTILS_H



#include <string>

#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/split.hpp>


/*
std::string strip_empty_spaces(const std::string& s) {
	std::string res = s;
	// Strip the heading and trailing empty spaces
	while (!res.empty()) {
		size_t len = res.length();
		if ((res[0] == ' ') || (res[0] == '\t')) {
			res = res.substr(1, len - 1);
			continue;
		}
		if ((res[len - 1] == ' ') || (res[len - 1] == '\t')) {
			res = res.substr(0, res.length() - 1);
			continue;
		}
		break;
	}
	return res;
}*/

/*
 * Hash functions
 */
class integerhash {
        public:
                static uint32_t hash32(uint32_t key) {
                        return key;
                }
};

class strhash {
        public:
                static uint32_t hash32(const char *s) {
                        return 0;
                }
};


#endif /* CLOUDSCRIBE_UTILS_H */
