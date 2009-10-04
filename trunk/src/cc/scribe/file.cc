/**
 * WARNING: 赶进度用了boost::system和boost::filesystem这两个鸟玩艺
 * edisonpeng@tencent.com
 */
#include "file.h"

#include <boost/filesystem/operations.hpp>

#include "common.h"

#include "logger.h"





// INITIAL_BUFFER_SIZE must always be >= UINT_SIZE
#define INITIAL_BUFFER_SIZE 4096
#define UINT_SIZE 4

using namespace std;
using boost::shared_ptr;

boost::shared_ptr<FileInterface> FileInterface::createFileInterface(const std::string& type, const std::string& name, bool framed) {
	if (0 == type.compare("std")) {
		return shared_ptr<FileInterface> (new StdFile(name, framed));
	} else {
		return shared_ptr<FileInterface> ();
	}
}

std::vector<std::string> FileInterface::list(const std::string& path, const std::string &fsType) {
	std::vector<std::string> files;
	shared_ptr<FileInterface> concrete_file = createFileInterface(fsType, "");
	if (concrete_file) {
		concrete_file->listImpl(path, files);
	}
	return files;
}

FileInterface::FileInterface(const std::string& name, bool frame) :
	framed(frame), filename(name) {
}

FileInterface::~FileInterface() {
}

StdFile::StdFile(const std::string& name, bool frame) :
	FileInterface(name, frame), inputBuffer(NULL), bufferSize(0) {
}

StdFile::~StdFile() {
	if (inputBuffer) {
		delete[] inputBuffer;
		inputBuffer = NULL;
	}
}

bool StdFile::openRead() {
	return open(fstream::in);
}

bool StdFile::openWrite() {
	/* 尝试创建个目录 */
	string::size_type slash;
	if (!filename.empty() && (filename.find_first_of("/") != string::npos) && (filename.find_first_of("/") != (slash = filename.find_last_of("/")))) {
		try {
			boost::filesystem::create_directory(filename.substr(0, slash));
		} catch (std::exception const& e) {
			LOG_OPER("Exception < %s > trying to create directory", e.what());
			return false;
		}
	}

	ios_base::openmode mode = fstream::out | fstream::app;
	return open(mode);
}

bool StdFile::openTruncate() {
	// 打开一个已经存在的文件,准备truncate它的内容
	ios_base::openmode mode = fstream::out | fstream::app | fstream::trunc;
	return open(mode);
}

bool StdFile::open(ios_base::openmode mode) {
	if (file.is_open()) {
		return false;
	}
	file.open(filename.c_str(), mode);
	return file.good();
}

bool StdFile::isOpen() {
	return file.is_open();
}

void StdFile::close() {
	if (file.is_open()) {
		file.close();
	}
}

string StdFile::getFrame(unsigned data_length) {
	if (framed) {
		char buf[UINT_SIZE];
		serializeUInt(data_length, buf);
		return string(buf, UINT_SIZE);

	} else {
		return string();
	}
}

bool StdFile::write(const std::string& data) {
	if (!file.is_open()) {
		return false;
	}
	file << data;
	if (file.bad()) {
		return false;
	}
	return true;
}

void StdFile::flush() {
	if (file.is_open()) {
		file.flush();
	}
}

bool StdFile::readNext(std::string& _return) {
	if (!inputBuffer) {
		bufferSize = INITIAL_BUFFER_SIZE;
		inputBuffer = new char[bufferSize];
	}

	if (framed) {
		unsigned size;
		file.read(inputBuffer, UINT_SIZE); // assumes INITIAL_BUFFER_SIZE > UINT_SIZE
		if (file.good() && (size = unserializeUInt(inputBuffer))) {
			// 检查size是否大于max uint size的一半, 这么大的消息很变态了, 要写条警告信息
			if (size >= (((unsigned) 1) << (UINT_SIZE*8 - 1))) {
				LOG_OPER("WARNING: attempting to read message of size %d bytes", size);

				// 如果这么大的消息,就不能再双倍原则来进行扩展了,否则内存可能会溢出的
				bufferSize = size;
			}

			// 小消息就双倍原则
			while (size > bufferSize) {
				bufferSize = 2 * bufferSize;
				delete[] inputBuffer;
				inputBuffer = new char[bufferSize];
			}
			// TODO 以后看可否修改成aio的方式.
			file.read(inputBuffer, size);
			if (file.good()) {
				_return.assign(inputBuffer, size);
				return true;
			} else {
				int offset = file.tellg();
				LOG_OPER("ERROR: Failed to read file %s at offset %d", filename.c_str(), offset);
				return false;
			}
		}
	} else {
		file.getline(inputBuffer, bufferSize);
		if (file.good()) {
			_return = inputBuffer;
			return true;
		}
	}
	return false;
}

unsigned long StdFile::fileSize() {
	unsigned long size = 0;
	try {
		size = boost::filesystem::file_size(filename.c_str());
	} catch (std::exception const& e) {
		LOG_OPER("Failed to get size for file <%s> error <%s>", filename.c_str(), e.what());
		size = 0;
	}
	return size;
}

/**
 * 把给定目录中的子目录列表返回.
 */
void StdFile::listImpl(const std::string& path, std::vector<std::string>& _return) {
	try {
		if (boost::filesystem::exists(path)) {
			boost::filesystem::directory_iterator dir_iter(path), end_iter;

			for (; dir_iter != end_iter; ++dir_iter) {
				_return.push_back(dir_iter->filename());
			}
		}
	} catch (std::exception const& e) {
		LOG_OPER("exception <%s> listing files in <%s>", e.what(), path.c_str());
	}
}

void StdFile::deleteFile() {
	boost::filesystem::remove(filename);
}

// Buffer最好要小于UINT_SIZE长度!
unsigned FileInterface::unserializeUInt(const char* buffer) {
	unsigned retval = 0;
	int i;
	for (i = 0; i < UINT_SIZE; ++i) {
		retval |= (unsigned char) buffer[i] << (8 * i);
	}
	return retval;
}

// 把uint序列化到buffer中.
void FileInterface::serializeUInt(unsigned data, char* buffer) {
	int i;
	for (i = 0; i < UINT_SIZE; ++i) {
		buffer[i] = (unsigned char) ((data >> (8 * i)) & 0xFF);
	}
}
