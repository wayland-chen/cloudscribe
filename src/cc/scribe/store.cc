/**
 * 他妈的这个鸟代码死磕很久了
 * author: edisonpeng@tencent.com
 */
#include "store.h"

#include <sys/stat.h>

#include <sstream>
#include <iostream>
#include <iomanip>

#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/split.hpp>

#include "forwarder_server.h"
#include "group_service.h"
#include "utils.h"
#include "logger.h"



using namespace std;
using namespace boost;
using namespace boost::filesystem;
using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;
using namespace apache::thrift::server;
using namespace forwarder::thrift;

#define DEFAULT_FILESTORE_MAX_SIZE               1000000000
#define DEFAULT_FILESTORE_ROLL_HOUR              1
#define DEFAULT_FILESTORE_ROLL_MINUTE            15
#define DEFAULT_BUFFERSTORE_MAX_QUEUE_LENGTH     2000000
#define DEFAULT_BUFFERSTORE_SEND_RATE            1
#define DEFAULT_BUFFERSTORE_AVG_RETRY_INTERVAL   300
#define DEFAULT_BUFFERSTORE_RETRY_INTERVAL_RANGE 60
#define DEFAULT_BUCKETSTORE_DELIMITER            ':'

ConnPool g_connPool;

const string meta_logfile_prefix = "forwarder_meta<new_logfile>: ";

/**
 * 工厂方法，来创建各种各样的Store对象.
 */
boost::shared_ptr<Store> Store::createStore(const string& type, const string& category, bool readable, bool multi_category) {
	if (0 == type.compare("file")) {
		return shared_ptr<Store> (new FileStore(category, multi_category, readable));
	} else if (0 == type.compare("buffer")) {
		return shared_ptr<Store> (new BufferStore(category, multi_category));
	} else if (0 == type.compare("network")) {
		return shared_ptr<Store> (new NetworkStore(category, multi_category));
	} else if (0 == type.compare("bucket")) {
		return shared_ptr<Store> (new BucketStore(category, multi_category));
	} else if (0 == type.compare("thriftfile")) {
		return shared_ptr<Store> (new ThriftFileStore(category, multi_category));
	} else if (0 == type.compare("null")) {
		return shared_ptr<Store> (new NullStore(category, multi_category));
	} else if (0 == type.compare("multi")) {
		return shared_ptr<Store> (new MultiStore(category, multi_category));
	} else if (0 == type.compare("category")) {
		return shared_ptr<Store> (new CategoryStore(category, multi_category));
	} else if (0 == type.compare("multifile")) {
		return shared_ptr<Store> (new MultiFileStore(category, multi_category));
	} else if (0 == type.compare("thriftmultifile")) {
		return shared_ptr<Store> (new ThriftMultiFileStore(category, multi_category));
	} else {
		return shared_ptr<Store> ();
	}
}

/* Start of Store */
Store::Store(const string& category, const string &type, bool multi_category) :
	categoryHandled(category), multiCategory(multi_category), storeType(type) {
	pthread_mutex_init(&statusMutex, NULL);
}

Store::~Store() {
	pthread_mutex_destroy(&statusMutex);
}

void Store::setStatus(const std::string& new_status) {
	pthread_mutex_lock(&statusMutex);
	status = new_status;
	pthread_mutex_unlock(&statusMutex);
}

std::string Store::getStatus() {
	pthread_mutex_lock(&statusMutex);
	std::string return_status(status);
	pthread_mutex_unlock(&statusMutex);
	return return_status;
}

bool Store::readOldest(/*out*/boost::shared_ptr<logentry_vector_t> messages, struct tm* now) {
	LOG_OPER("[%s] ERROR: attempting to read from a write-only store", categoryHandled.c_str());
	return false;
}

bool Store::replaceOldest(boost::shared_ptr<logentry_vector_t> messages, struct tm* now) {
	LOG_OPER("[%s] ERROR: attempting to read from a write-only store", categoryHandled.c_str());
	return false;
}

void Store::deleteOldest(struct tm* now) {
	LOG_OPER("[%s] ERROR: attempting to read from a write-only store", categoryHandled.c_str());
}

bool Store::empty(struct tm* now) {
	LOG_OPER("[%s] ERROR: attempting to read from a write-only store", categoryHandled.c_str());
	return true;
}

const std::string& Store::getType() {
	return storeType;
}
/* End of Store */

/**
 *
 * 下面是各种Store的具体实现和装饰器.
 *
 */

/* FileStoreBase */
FileStoreBase::FileStoreBase(const string& category, const string &type, bool multi_category) :
	Store(category, type, multi_category), filePath("/tmp"), baseFileName(category), maxSize(DEFAULT_FILESTORE_MAX_SIZE),
	rollPeriod(ROLL_NEVER), rollHour(DEFAULT_FILESTORE_ROLL_HOUR),
	rollMinute(DEFAULT_FILESTORE_ROLL_MINUTE),
	fsType("std"), chunkSize(0), writeMeta(false), writeCategory(false), createSymlink(true), currentSize(0), lastRollTime(0), eventsWritten(0) {
}

FileStoreBase::~FileStoreBase() {
}

void FileStoreBase::configure(pStoreConf configuration) {
	// 读取配置，有问题的地方就打WARNING警告
	std::string tmp;
	configuration->getString("file_path", filePath);
	if (!configuration->getString("base_filename", baseFileName)) {
		LOG_OPER("[%s] WARNING: Bad config - no base_filename specified for file store", categoryHandled.c_str());
	}
	if (configuration->getString("rotate_period", tmp)) {
		if (0 == tmp.compare("hourly")) {
			rollPeriod = ROLL_HOURLY;
		} else if (0 == tmp.compare("daily")) {
			rollPeriod = ROLL_DAILY;
		} else if (0 == tmp.compare("never")) {
			rollPeriod = ROLL_NEVER;
		}
	}
	if (configuration->getString("write_meta", tmp)) {
		if (0 == tmp.compare("yes")) {
			writeMeta = true;
		}
	}
	if (configuration->getString("write_category", tmp)) {
		if (0 == tmp.compare("yes")) {
			writeCategory = true;
		}
	}
	if (configuration->getString("create_symlink", tmp)) {
		if (0 == tmp.compare("yes")) {
			createSymlink = true;
		} else {
			createSymlink = false;
		}
	}

	configuration->getUnsigned("max_size", maxSize);
	configuration->getString("fs_type", fsType);
	configuration->getUnsigned("rotate_hour", rollHour);
	configuration->getUnsigned("rotate_minute", rollMinute);
	configuration->getUnsigned("chunk_size", chunkSize);
}

void FileStoreBase::copyCommon(const FileStoreBase *base) {
	chunkSize = base->chunkSize;
	maxSize = base->maxSize;
	rollPeriod = base->rollPeriod;
	rollHour = base->rollHour;
	rollMinute = base->rollMinute;
	fsType = base->fsType;
	writeMeta = base->writeMeta;
	writeCategory = base->writeCategory;
	createSymlink = base->createSymlink;

	/*
	 * 搞个唯一文件名出来.
	 */
	filePath = base->filePath + std::string("/") + categoryHandled;
	baseFileName = categoryHandled;
}

bool FileStoreBase::open() {
	return openInternal(false, NULL);
}

void FileStoreBase::periodicCheck() {
	time_t rawtime;
	struct tm *timeinfo;
	time(&rawtime);
	timeinfo = localtime(&rawtime);

	// 如果文件过大或者超过了时间, 就进行滚动
	bool rotate = currentSize > maxSize;
	if (!rotate && rollPeriod != ROLL_NEVER) {
		if (rollPeriod == ROLL_DAILY) {
			rotate = timeinfo->tm_mday != lastRollTime && static_cast<uint> (timeinfo->tm_hour) >= rollHour && static_cast<uint> (timeinfo->tm_min) >= rollMinute;
		} else {
			rotate = timeinfo->tm_hour != lastRollTime && static_cast<uint> (timeinfo->tm_min) >= rollMinute;
		}
	}
	if (rotate) {
		rotateFile(timeinfo);
	}
}

void FileStoreBase::rotateFile(struct tm *timeinfo) {
	LOG_OPER("[%s] %d:%d rotating file <%s> old size <%lu> max size <%lu>", categoryHandled.c_str(),
			timeinfo->tm_hour, timeinfo->tm_min, makeBaseFilename(timeinfo).c_str(),
			currentSize, maxSize);

	//记录统计信息
	printStats();
	//打开新文件，在打开之前关闭老的文件.
	openInternal(true, timeinfo);
}

/**
 * 构造一个完整文件名
 * @param suffix int
 * @param creation_time tm
 * @return string
 */
string FileStoreBase::makeFullFilename(int suffix, struct tm* creation_time) {
	ostringstream filename;

	filename << filePath << '/';
	filename << makeBaseFilename(creation_time);
	filename << '_' << setw(5) << setfill('0') << suffix;
	return filename.str();
}

//连接到当前文件的当前路径软链接
string FileStoreBase::makeBaseSymlink() {
	ostringstream base;
	base << categoryHandled << "_current";
	return base.str();
}

//连接到当前文件的绝对路径软链接
string FileStoreBase::makeFullSymlink() {
	ostringstream filename;
	filename << filePath << '/' << makeBaseSymlink();
	return filename.str();
}

string FileStoreBase::makeBaseFilename(struct tm* creation_time) {
	if (!creation_time) {
		time_t rawtime;
		time(&rawtime);
		creation_time = localtime(&rawtime);
	}

	ostringstream filename;

	filename << baseFileName;
	if (rollPeriod != ROLL_NEVER) {
		filename << '-' << creation_time->tm_year + 1900 << '-' << setw(2) << setfill('0') << creation_time->tm_mon + 1 << '-' << setw(2) << setfill('0') << creation_time->tm_mday;
	}
	return filename.str();
}

// 根据base_filename返回最新的suffix回来
int FileStoreBase::findNewestFile(const string& base_filename) {
	std::vector<std::string> files = FileInterface::list(filePath, fsType);

	int max_suffix = -1;
	std::string retval;
	for (std::vector<std::string>::iterator iter = files.begin(); iter != files.end(); ++iter) {
		int suffix = getFileSuffix(*iter, base_filename);
		if (suffix > max_suffix) {
			max_suffix = suffix;
		}
	}
	return max_suffix;
}

// 查找最旧的文件的suffix出来
int FileStoreBase::findOldestFile(const string& base_filename) {
	std::vector<std::string> files = FileInterface::list(filePath, fsType);

	int min_suffix = -1;
	std::string retval;
	for (std::vector<std::string>::iterator iter = files.begin(); iter != files.end(); ++iter) {
		int suffix = getFileSuffix(*iter, base_filename);
		if (suffix >= 0 && (min_suffix == -1 || suffix < min_suffix)) {
			min_suffix = suffix;
		}
	}
	return min_suffix;
}

// 获取文件的后缀
int FileStoreBase::getFileSuffix(const string& filename, const string& base_filename) {
	int suffix = -1;
	string::size_type suffix_pos = filename.rfind('_');

	bool retVal = (0 == filename.substr(0, suffix_pos).compare(base_filename));

	if (string::npos != suffix_pos && filename.length() > suffix_pos && retVal) {
		stringstream stream;
		stream << filename.substr(suffix_pos + 1);
		stream >> suffix;
	}
	return suffix;
}

//记录统计信息
void FileStoreBase::printStats() {
	string filename(filePath);
	filename += "/forwarder_stats";

	boost::shared_ptr<FileInterface> stats_file = FileInterface::createFileInterface(fsType, filename);
	if (!stats_file || !stats_file->openWrite()) {
		LOG_OPER("[%s] Failed to open stats file <%s> of type <%s> for writing", categoryHandled.c_str(), filename.c_str(), fsType.c_str());
		// This isn't enough of a problem to change our status
		return;
	}

	time_t rawtime;
	time(&rawtime);
	struct tm *local_time = localtime(&rawtime);

	ostringstream msg;
	msg << local_time->tm_year + 1900 << '-' << setw(2) << setfill('0') << local_time->tm_mon + 1 << '-' << setw(2) << setfill('0') << local_time->tm_mday << '-' << setw(2) << setfill('0')
			<< local_time->tm_hour << ':' << setw(2) << setfill('0') << local_time->tm_min;

	msg << " wrote <" << currentSize << "> bytes in <" << eventsWritten << "> events to file <" << currentFilename << ">" << endl;

	stats_file->write(msg.str());
	stats_file->close();
}

// 给定chunk尺寸, 返回需要补齐的字节数
unsigned long FileStoreBase::bytesToPad(unsigned long next_message_length, unsigned long current_file_size, unsigned long chunk_size) {
	if (chunk_size > 0) {
		unsigned long space_left_in_chunk = chunk_size - current_file_size % chunk_size;
		if (next_message_length > space_left_in_chunk) {
			return space_left_in_chunk;
		} else {
			return 0;
		}
	}
	// chunk_size <= 0 means don't do any chunking
	return 0;
}

FileStore::FileStore(const string& category, bool multi_category, bool is_buffer_file) :
	FileStoreBase(category, "file", multi_category), isBufferFile(is_buffer_file), addNewlines(false) {
}

FileStore::~FileStore() {
}

void FileStore::configure(pStoreConf configuration) {
	FileStoreBase::configure(configuration);

	// 有诡异的地方目前都打WARNING log出来.
	if (isBufferFile) {
		// 调度buffer文件的滚动会有很多麻烦，先别搞
		rollPeriod = ROLL_NEVER;

		// buffer文件目前没有chunk机制, FileStore负责处理chunk padding, FileInterface负责处理framing.
		// buffer文件需要framed.
		// (framed的意思是指消息的存放格式是(尺寸, 消息内容)).
		// (Chunked的意思是我们对一定数量的消息以chunk size字节对齐,这样, 每个chunk的开头都是一个消息的起始.
		// 这有助于二进制数据修复, 也有利于在大文件中seek)
		chunkSize = 0;

		// 如果是buffer文件，就把所有类别的消息都合并在一个文件中.
		if (multiCategory) {
			writeCategory = true;
		}
	}

	unsigned long inttemp = 0;
	configuration->getUnsigned("add_newlines", inttemp);
	addNewlines = inttemp ? true : false;
}

bool FileStore::openInternal(bool incrementFilename, struct tm* current_time) {
	bool success = false;
	if (!current_time) {
		time_t rawtime;
		time(&rawtime);
		current_time = localtime(&rawtime);
	}
	try {
		int suffix = findNewestFile(makeBaseFilename(current_time));

		if (incrementFilename) {
			++suffix;
		}

		// 如果还没有文件存在或非递增模式的情况下
		if (suffix < 0) {
			suffix = 0;
		}

		string file = makeFullFilename(suffix, current_time);

		if (rollPeriod == ROLL_DAILY) {
			lastRollTime = current_time->tm_mday;
		} else {
			// default to hourly if rollPeriod is garbage
			lastRollTime = current_time->tm_hour;
		}

		if (writeFile) {
			if (writeMeta) {
				//在当前文件的最后写上一些元信息: 指名它的下一个递增文件.
				writeFile->write(meta_logfile_prefix + file);
			}
			writeFile->close();
		}

		writeFile = FileInterface::createFileInterface(fsType, file, isBufferFile);
		if (!writeFile) {
			LOG_OPER("[%s] Failed to create file <%s> of type <%s> for writing",
					categoryHandled.c_str(), file.c_str(), fsType.c_str());
			setStatus("file open error");
			return false;
		}

		// 以写模式打开文件
		success = writeFile->openWrite();

		if (!success) {
			LOG_OPER("[%s] Failed to open file <%s> for writing", categoryHandled.c_str(), file.c_str());
			setStatus("File open error");
		} else {
			/* 给当前log文件建立符号链接, 不过buffer文件是不需要符号链接的 */
			if (createSymlink && !isBufferFile) {
				string symlinkName = makeFullSymlink();
				unlink(symlinkName.c_str());
				symlink(file.c_str(), symlinkName.c_str());
			}

			LOG_OPER("[%s] Opened file <%s> for writing", categoryHandled.c_str(), file.c_str());

			currentSize = writeFile->fileSize();
			currentFilename = file;
			eventsWritten = 0;
			setStatus("");
		}
	} catch (std::exception const& e) {
		LOG_OPER("[%s] Failed to create/open file of type <%s> for writing", categoryHandled.c_str(), fsType.c_str());
		LOG_OPER("Exception: %s", e.what());
		setStatus("file create/open error");

		return false;
	}
	return success;
}

bool FileStore::isOpen() {
	return writeFile && writeFile->isOpen();
}

void FileStore::close() {
	if (writeFile) {
		writeFile->close();
	}
}

void FileStore::flush() {
	if (writeFile) {
		writeFile->flush();
	}
}

shared_ptr<Store> FileStore::copy(const std::string &category) {
	FileStore *store = new FileStore(category, multiCategory, isBufferFile);
	shared_ptr<Store> copied = shared_ptr<Store> (store);

	store->addNewlines = addNewlines;
	store->copyCommon(this);
	return copied;
}

bool FileStore::handleMessages(boost::shared_ptr<logentry_vector_t> messages) {
	if (!isOpen()) {
		LOG_OPER("[%s] File failed to open FileStore::handleMessages()", categoryHandled.c_str());
		return false;
	}

	// 把消息写到当前文件
	return writeMessages(messages, writeFile);
}

// 把消息写到指定的文件
bool FileStore::writeMessages(boost::shared_ptr<logentry_vector_t> messages, boost::shared_ptr<FileInterface> write_file) {
	// 数据首先被写入到缓冲, 然后再单次调用写入磁盘.
	// 在基于网络传递数据时, 缓冲写能提高性能. (例如写nfs).
	// 这也意味着整块消息要么写成功要么失败.
	string write_buffer;
	unsigned long current_size_buffered = currentSize; // 当前缓冲的数据大小

	for (logentry_vector_t::iterator iter = messages->begin(); iter != messages->end(); ++iter) {
		// 这里要小心检查一下长度. getFrame只需要消息的长度, bytesToPad需要frame的长度和消息本身.
		unsigned long length = 0;
		unsigned long message_length = (*iter)->message.length();
		string frame, category_frame;

		if (addNewlines) {
			++message_length;
		}

		length += message_length;

		if (writeCategory) {
			//为category+newline和category frame预留空间
			unsigned long category_length = (*iter)->category.length() + 1;
			length += category_length;

			category_frame = write_file->getFrame(category_length);
			length += category_frame.length();
		}

		frame = write_file->getFrame(message_length);

		length += frame.length();

		// 把消息进行chunk对齐
		unsigned long padding = bytesToPad(length, current_size_buffered, chunkSize);

		length += padding;

		if (padding) {
			write_buffer += string(padding, 0);
		}

		if (writeCategory) {
			write_buffer += category_frame;
			write_buffer += (*iter)->category + "\n";
		}

		write_buffer += frame;
		write_buffer += (*iter)->message;

		if (addNewlines) {//如果需要加换行符
			write_buffer += "\n";
		}

		current_size_buffered += length;
	}

	if (!write_file->write(write_buffer)) {
		LOG_OPER("[%s] File store failed to write (%u) messages to file", categoryHandled.c_str(), messages->size());
		setStatus("File write error");
		close();
		return false;
	}
	currentSize = current_size_buffered;
	eventsWritten += messages->size();

	// 当前数据超过阀值,就要强制rotate.
	if (currentSize > maxSize) {
		time_t rawtime;
		struct tm *timeinfo;
		time(&rawtime);
		timeinfo = localtime(&rawtime);
		rotateFile(timeinfo);
	}

	return true;
}

void FileStore::deleteOldest(struct tm* now) {
	int index = findOldestFile(makeBaseFilename(now));
	if (index < 0) {
		return;
	}
	shared_ptr<FileInterface> deletefile = FileInterface::createFileInterface(fsType, makeFullFilename(index, now));
	deletefile->deleteFile();
}

// 用给定时间戳的消息来替换先前文件中的消息.
bool FileStore::replaceOldest(boost::shared_ptr<logentry_vector_t> messages, struct tm* now) {
	string base_name = makeBaseFilename(now);
	int index = findOldestFile(base_name);
	if (index < 0) {
		LOG_OPER("[%s] Could not find files <%s>", categoryHandled.c_str(), base_name.c_str());
		return false;
	}

	string filename = makeFullFilename(index, now);

	// 需要先close再reopen
	close();

	// 删除此文件, 然后再打开了写消息
	shared_ptr<FileInterface> infile = FileInterface::createFileInterface(fsType, filename, isBufferFile);
	infile->deleteFile();

	bool success;
	if (infile->openWrite()) {
		success = writeMessages(messages, infile);
	} else {
		LOG_OPER("[%s] Failed to open file <%s> for writing and truncate", categoryHandled.c_str(), filename.c_str());
		success = false;
	}

	// 关闭此文件并重新打开
	infile->close();
	open();

	return success;
}

bool FileStore::readOldest(/*out*/boost::shared_ptr<logentry_vector_t> messages, struct tm* now) {
	int index = findOldestFile(makeBaseFilename(now));
	if (index < 0) {
		//如果没有文件存在, 那就直接返回
		return true;
	}
	std::string filename = makeFullFilename(index, now);

	shared_ptr<FileInterface> infile = FileInterface::createFileInterface(fsType, filename, isBufferFile);

	if (!infile->openRead()) {
		LOG_OPER("[%s] Failed to open file <%s> for reading", categoryHandled.c_str(), filename.c_str());
		return false;
	}

	/* 第一次readNext读消息内容，第二次readNext读category信息. */
	std::string message;
	while (infile->readNext(message)) {
		if (!message.empty()) {
			logentry_ptr_t entry = logentry_ptr_t(new LogEntry);

			if (writeCategory) {
				// 提取category,把尾部的\n去掉
				entry->category = message.substr(0, message.length() - 1);

				if (!infile->readNext(message)) {
					LOG_OPER("[%s] category not stored with message <%s>", categoryHandled.c_str(), entry->category.c_str());
				}
			} else {
				entry->category = categoryHandled;
			}
			entry->message = message;

			messages->push_back(entry);
		}
	}
	infile->close();

	LOG_OPER("[%s] successfully read <%u> entries from file <%s>", categoryHandled.c_str(), messages->size(), filename.c_str());
	return true;
}

bool FileStore::empty(struct tm* now) {
	std::vector<std::string> files = FileInterface::list(filePath, fsType);

	std::string base_filename = makeBaseFilename(now);
	for (std::vector<std::string>::iterator iter = files.begin(); iter != files.end(); ++iter) {
		int suffix = getFileSuffix(*iter, base_filename);
		if (-1 != suffix) {
			std::string fullname = makeFullFilename(suffix, now);
			shared_ptr<FileInterface> file = FileInterface::createFileInterface(fsType, fullname);
			if (file->fileSize()) {
				return false;
			}
		} // else 没有找到匹配当前store的文件
	}
	return true;
}
/* End of FileStore */

/* Start of ThriftFileStore */
ThriftFileStore::ThriftFileStore(const std::string& category, bool multi_category) :
	FileStoreBase(category, "thriftfile", multi_category), flushFrequencyMs(0), msgBufferSize(0) {
}

ThriftFileStore::~ThriftFileStore() {
}

// 搞个prototype模式!
shared_ptr<Store> ThriftFileStore::copy(const std::string &category) {
	ThriftFileStore *store = new ThriftFileStore(category, multiCategory);
	shared_ptr<Store> copied = shared_ptr<Store> (store);

	store->flushFrequencyMs = flushFrequencyMs;
	store->msgBufferSize = msgBufferSize;
	store->copyCommon(this);
	return copied;
}

bool ThriftFileStore::handleMessages(boost::shared_ptr<logentry_vector_t> messages) {
	if (!isOpen()) {
		return false;
	}

	unsigned long messages_handled = 0;

	for (logentry_vector_t::iterator iter = messages->begin(); iter != messages->end(); ++iter) {
		// 长度是估算出来的，因为thrift对我们来说是黑盒
		uint32_t length = (*iter)->message.size();

		try {
			thriftFileTransport->write(reinterpret_cast<const uint8_t*> ((*iter)->message.data()), length);
			currentSize += length;
			++eventsWritten;
			++messages_handled;
		} catch (TException te) {
			LOG_OPER("[%s] Thrift file store failed to write to file: %s\n", categoryHandled.c_str(), te.what());
			setStatus("File write error");

			// 如果已经处理过部分消息, 那就在返回错误之前从原始消息中把成功的消息清除掉
			if (messages_handled) {
				messages->erase(messages->begin(), iter);
			}
			return false;
		}
	}

	// 阀值触发，要rotate了.
	if (currentSize > maxSize) {
		time_t rawtime;
		struct tm *timeinfo;
		time(&rawtime);
		timeinfo = localtime(&rawtime);
		rotateFile(timeinfo);
	}

	return true;
}

bool ThriftFileStore::open() {
	return openInternal(true, NULL);
}

bool ThriftFileStore::isOpen() {
	return thriftFileTransport && thriftFileTransport->isOpen();
}

void ThriftFileStore::configure(pStoreConf configuration) {
	FileStoreBase::configure(configuration);
	configuration->getUnsigned("flush_frequency_ms", flushFrequencyMs);
	configuration->getUnsigned("msg_buffer_size", msgBufferSize);
}

void ThriftFileStore::close() {
	thriftFileTransport.reset();
}

void ThriftFileStore::flush() {
	// TFileTransport有它自己的周期性flushing机制，所以这里不要多管闲事了.
	return;
}

bool ThriftFileStore::openInternal(bool incrementFilename, struct tm* current_time) {
	if (!current_time) {
		time_t rawtime;
		time(&rawtime);
		current_time = localtime(&rawtime);
	}

	int suffix = findNewestFile(makeBaseFilename(current_time));

	if (incrementFilename) {
		++suffix;
	}

	// 尚无文件 or 非增量模式
	if (suffix < 0) {
		suffix = 0;
	}

	string filename = makeFullFilename(suffix, current_time);
	/* 创建存放log的目录阿 */
	string::size_type slash;
	if (!filename.empty() && (filename.find_first_of("/") != string::npos) && (filename.find_first_of("/") != (slash = filename.find_last_of("/")))) {
		try {
			boost::filesystem::create_directory(filename.substr(0, slash));
		} catch (std::exception const& e) {
			LOG_OPER("Exception < %s > trying to create directory", e.what());
			return false;
		}
	}

	if (rollPeriod == ROLL_DAILY) {
		lastRollTime = current_time->tm_mday;
	} else { // 默认周期为hour
		lastRollTime = current_time->tm_hour;
	}

	try {
		thriftFileTransport.reset(new TFileTransport(filename));
		if (chunkSize != 0) {
			thriftFileTransport->setChunkSize(chunkSize);
		}
		if (flushFrequencyMs > 0) {
			thriftFileTransport->setFlushMaxUs(flushFrequencyMs * 1000);
		}
		if (msgBufferSize > 0) {
			thriftFileTransport->setEventBufferSize(msgBufferSize);
		}
		LOG_OPER("[%s] Opened file <%s> for writing", categoryHandled.c_str(), filename.c_str());

		struct stat st;
		if (stat(filename.c_str(), &st) == 0) {
			currentSize = st.st_size;
		} else {
			currentSize = 0;
		}
		currentFilename = filename;
		eventsWritten = 0;
		setStatus("");
	} catch (TException te) {
		LOG_OPER("[%s] Failed to open file <%s> for writing: %s\n", categoryHandled.c_str(), filename.c_str(), te.what());
		setStatus("File open error");
		return false;
	}

	if (createSymlink) {
		string symlinkName = makeFullSymlink();
		unlink(symlinkName.c_str());
		symlink(filename.c_str(), symlinkName.c_str());
	}

	return true;
}
/* End of ThriftFileStore */

/* Start of BufferStore */
BufferStore::BufferStore(const string& category, bool multi_category) :
	Store(category, "buffer", multi_category), maxQueueLength(DEFAULT_BUFFERSTORE_MAX_QUEUE_LENGTH),
	bufferSendRate(DEFAULT_BUFFERSTORE_SEND_RATE),
	avgRetryInterval(DEFAULT_BUFFERSTORE_AVG_RETRY_INTERVAL),
	retryIntervalRange(DEFAULT_BUFFERSTORE_RETRY_INTERVAL_RANGE),
	state(DISCONNECTED) {

	time(&lastWriteTime);
	time(&lastOpenAttempt);
	srand(lastWriteTime);
	retryInterval = getNewRetryInterval();
}

BufferStore::~BufferStore() {
}

void BufferStore::configure(pStoreConf configuration) {
	// 如果没有设置这些配置, 就会使用构造函数中设置的那些默认值
	configuration->getUnsigned("max_queue_length", (unsigned long&) maxQueueLength);
	configuration->getUnsigned("buffer_send_rate", (unsigned long&) bufferSendRate);
	configuration->getUnsigned("retry_interval", (unsigned long&) avgRetryInterval);
	configuration->getUnsigned("retry_interval_range", (unsigned long&) retryIntervalRange);

	if (retryIntervalRange > avgRetryInterval) {
		LOG_OPER("[%s] Bad config - retry_interval_range must be less than retry_interval. Using <%d> as range instead of <%d>", categoryHandled.c_str(), (int)avgRetryInterval, (int)retryIntervalRange);
		retryIntervalRange = avgRetryInterval;
	}

	pStoreConf secondary_store_conf;
	if (!configuration->getStore("secondary", secondary_store_conf)) {
		string msg("Bad config - buffer store doesn't have secondary store");
		setStatus(msg);
		cout << msg << endl;
	} else {
		string type;
		if (!secondary_store_conf->getString("type", type)) {
			string msg("Bad config - buffer secondary store doesn't have a type");
			setStatus(msg);
			cout << msg << endl;
		} else {
			secondaryStore = createStore(type, categoryHandled, true, multiCategory);
			secondaryStore->configure(secondary_store_conf);
		}
	}

	pStoreConf primary_store_conf;
	if (!configuration->getStore("primary", primary_store_conf)) {
		string msg("Bad config - buffer store doesn't have primary store");
		setStatus(msg);
		cout << msg << endl;
	} else {
		string type;
		if (!primary_store_conf->getString("type", type)) {
			string msg("Bad config - buffer primary store doesn't have a type");
			setStatus(msg);
			cout << msg << endl;
		} else if (0 == type.compare("multi")) {
			// 对于buffer存储，不能使用multistore, 这样可能会产生局部失败而导致不一致状态.
			string msg("Bad config - buffer primary store cannot be multistore");
			setStatus(msg);
		} else {
			primaryStore = createStore(type, categoryHandled, false, multiCategory);
			primaryStore->configure(primary_store_conf);
		}
	}

	// 如果配置无效，我们便会启用默认配置把数据写到默认路径.
	if (!secondaryStore) {
		secondaryStore = createStore("file", categoryHandled, true, multiCategory);
	}
	if (!primaryStore) {
		primaryStore = createStore("file", categoryHandled, false, multiCategory);
	}
}

bool BufferStore::isOpen() {
	return primaryStore->isOpen() || secondaryStore->isOpen();
}

bool BufferStore::open() {
	// 尝试打开primaryStore, 并设置相应状态
	if (primaryStore->open()) {
		// 之所以转换到此状态是为了防止前一个实例有遗留文件没有发送完
		changeState(SENDING_BUFFER);
	} else {
		secondaryStore->open();
		changeState(DISCONNECTED);
	}
	return isOpen();
}

void BufferStore::close() {
	if (primaryStore->isOpen()) {
		primaryStore->flush();
		primaryStore->close();
	}
	if (secondaryStore->isOpen()) {
		secondaryStore->flush();
		secondaryStore->close();
	}
}

void BufferStore::flush() {
	if (primaryStore->isOpen()) {
		primaryStore->flush();
	}
	if (secondaryStore->isOpen()) {
		secondaryStore->flush();
	}
}

shared_ptr<Store> BufferStore::copy(const std::string &category) {
	BufferStore *store = new BufferStore(category, multiCategory);
	shared_ptr<Store> copied = shared_ptr<Store> (store);

	store->maxQueueLength = maxQueueLength;
	store->bufferSendRate = bufferSendRate;
	store->avgRetryInterval = avgRetryInterval;
	store->retryIntervalRange = retryIntervalRange;

	store->primaryStore = primaryStore->copy(category);
	store->secondaryStore = secondaryStore->copy(category);
	return copied;
}

bool BufferStore::handleMessages(boost::shared_ptr<logentry_vector_t> messages) {
	time_t now;
	time(&now);
	lastWriteTime = now;

	// 如果队列太长，说明primaryStore的处理能力跟不上.
	// 当前的处理手段是主动断开和primaryStore的连接, 转而存储到secondaryStore上.
	if (state == STREAMING && messages->size() > maxQueueLength) {
		LOG_OPER("[%s] BufferStore queue backing up, switching to secondary store (%u messages)", categoryHandled.c_str(), (unsigned)messages->size());
		changeState(DISCONNECTED);
	}

	if (state == STREAMING) {
		if (primaryStore->handleMessages(messages)) {
			return true;
		} else {
			changeState(DISCONNECTED);
		}
	}

	if (state != STREAMING) {
		return secondaryStore->handleMessages(messages);
	}
	return false;
}

// 处理状态转换
void BufferStore::changeState(buffer_state_t new_state) {
	switch (state) {
	case STREAMING:
		secondaryStore->open();
		break;
	case DISCONNECTED:
		// 在离开当前状态之前的处理.
		setStatus("");
		break;
	case SENDING_BUFFER:
		break;
	default:
		break;
	}

	// 进入新的状态
	switch (new_state) {
	case STREAMING:
		secondaryStore->close();
		break;
	case DISCONNECTED:
		g_Handler->incrementCounter("retries");
		time(&lastOpenAttempt);
		retryInterval = getNewRetryInterval();
		if (!secondaryStore->isOpen()) {
			secondaryStore->open();
		}
		break;
	case SENDING_BUFFER:
		if (!secondaryStore->isOpen()) {
			secondaryStore->open();
		}
		break;
	default:
		break;
	}

	LOG_OPER("[%s] Changing state from <%s> to <%s>", categoryHandled.c_str(), stateAsString(state), stateAsString(new_state));
	state = new_state;
}

void BufferStore::periodicCheck() {
	primaryStore->periodicCheck();
	secondaryStore->periodicCheck();

	time_t now;
	struct tm* nowinfo;
	time(&now);
	nowinfo = localtime(&now);

	//如果因为意外断开连接了, 那就在retryInterval之后再尝试primaryStore->open()
	if (state == DISCONNECTED) {
		if (now - lastOpenAttempt > retryInterval) {
			if (primaryStore->open()) {
				changeState(SENDING_BUFFER);
			} else {
				// 重置retry定时器
				changeState(DISCONNECTED);
			}
		}
	}

	if (state == SENDING_BUFFER) {
		// 从secondary store读取一组消息发送给primary store.
		// 如果primary store繁忙,它可能会返回 稍后再试, 所以这里也不能一次性读太多消息.
		// 如果secondary store是一个文件, 那么消息数量可以由max file size来控制.
		unsigned sent = 0;
		// 每次最多只搞bufferSendRate这么多个buffer文件
		for (sent = 0; sent < bufferSendRate; ++sent) {
			boost::shared_ptr<logentry_vector_t> messages(new logentry_vector_t);
			//把遗留消息读出来
			if (secondaryStore->readOldest(messages, nowinfo)) {
				time(&lastWriteTime);

				unsigned long size = messages->size();
				if (size) {
					//如果有遗留消息那就让primaryStore去处理
					if (primaryStore->handleMessages(messages)) {
						//如果处理成功,就干掉遗留消息
						secondaryStore->deleteOldest(nowinfo);
					} else {
						if (messages->size() != size) {
							// 可能只有一部分消息被处理了, 只好把这部分消息再放回去
							LOG_OPER("[%s] buffer store primary store processed %lu/%lu messages", categoryHandled.c_str(), size - messages->size(), size);

							if (!secondaryStore->replaceOldest(messages, nowinfo)) {
								// 如果把数据保存回去还出错,那只好报告说数据丢失了
								LOG_OPER("[%s] buffer store secondary store lost %u messages", categoryHandled.c_str(), messages->size());
								g_Handler->incrementCounter("lost", messages->size());
								secondaryStore->deleteOldest(nowinfo);
							}
						}

						changeState(DISCONNECTED);
						break;
					}
				} else {
					// 没有遗留消息需要转发, 除非buffer消息文件为空才可能出现这种情况
					secondaryStore->deleteOldest(nowinfo);
				}
			} else {
				// 到这里就不太好了
				setStatus("Failed to read from secondary store");
				LOG_OPER("[%s] WARNING: buffer store can't read from secondary store", categoryHandled.c_str());
				break;
			}

			// 如果buffer消息都被发送走了,就break
			if (secondaryStore->empty(nowinfo)) {
				LOG_OPER("[%s] No more buffer files to send, switching to streaming mode", categoryHandled.c_str());
				changeState(STREAMING);

				primaryStore->flush();
				break;
			}
		}
	}// if state == SENDING_BUFFER
}

time_t BufferStore::getNewRetryInterval() {
	time_t interval = avgRetryInterval - retryIntervalRange / 2 + rand() % retryIntervalRange;
	LOG_OPER("[%s] choosing new retry interval <%d> seconds", categoryHandled.c_str(), (int)interval);
	return interval;
}

const char* BufferStore::stateAsString(buffer_state_t state) {
	switch (state) {
	case STREAMING:
		return "STREAMING";
	case DISCONNECTED:
		return "DISCONNECTED";
	case SENDING_BUFFER:
		return "SENDING_BUFFER";
	default:
		return "unknown state";
	}
}

std::string BufferStore::getStatus() {
	// secondary出问题时影响会大一些
	std::string return_status = secondaryStore->getStatus();
	if (return_status.empty()) {
		return_status = Store::getStatus();
	}
	if (return_status.empty()) {
		return_status = primaryStore->getStatus();
	}
	return return_status;
}
/* End of BufferStore */

/* Start of NetworkStore */
NetworkStore::NetworkStore(const string& category, bool multi_category) :
	Store(category, "network", multi_category), useConnPool(false), smcBased(false), remotePort(0), opened(false) {
	// opened标志来确保我们不会重复关闭连接池中的连接,从而多次干掉引用计数.
}

NetworkStore::~NetworkStore() {
	close();
}

void NetworkStore::configure(pStoreConf configuration) {
	// smc的优先级要高于host + port的模式
	if (configuration->getString("smc_service", smcService)) {
		smcBased = true;
	} else {
		smcBased = false;
		configuration->getString("remote_host", remoteHost);
		configuration->getUnsigned("remote_port", remotePort);
	}

	if (!configuration->getInt("timeout", timeout)) {
		timeout = DEFAULT_SOCKET_TIMEOUT_MS;
	}

	string temp;
	if (configuration->getString("use_conn_pool", temp)) {
		if (0 == temp.compare("yes")) {
			useConnPool = true;
		}
	}
}

bool NetworkStore::open() {
	cout<<"open()"<<endl;
	//service manage center, 到时候需要实现一个
	if (smcBased) { //env_default是不支持smc方式的.
		vector<string> hostStrs;
		bool success = g_Handler->groupServicePtr->getMembers(smcService, hostStrs);
		// 如果没有找到任何server.
		if (!success || hostStrs.empty()) {
			LOG_OPER("[%s] Failed to get servers from smc", categoryHandled.c_str());
			setStatus("Could not get list of servers from smc");
			return false;
		}
	

		server_vector_t servers;
		for(vector<string>::iterator it=hostStrs.begin(),end=hostStrs.end();it!=end;++it) {
			cout<<"get member: "<<(*it)<<endl;
			vector<string> host_port_pair;
			boost::algorithm::split(host_port_pair, (*it), boost::algorithm::is_any_of("_"));
			if(host_port_pair.size() == 2) { 
				servers.push_back(
					std::pair<std::string,int>(
						host_port_pair[0],
						atoi(   
							host_port_pair[1].c_str()
						)
					)       
				);      
			}
		}
		

		if (useConnPool) {
			opened = g_connPool.open(smcService, servers, static_cast<int> (timeout));
		} else {
			unpooledConn = shared_ptr<forwarderConn> (new forwarderConn(smcService, servers, static_cast<int> (timeout)));
			opened = unpooledConn->open();
		}

	} else if (remotePort <= 0 || remoteHost.empty()) {
		LOG_OPER("[%s] Bad config - won't attempt to connect to <%s:%lu>", categoryHandled.c_str(), remoteHost.c_str(), remotePort);
		setStatus("Bad config - invalid location for remote server");
		return false;

	} else {
		if (useConnPool) {
			opened = g_connPool.open(remoteHost, remotePort, static_cast<int> (timeout));
		} else {
			unpooledConn = shared_ptr<forwarderConn> (new forwarderConn(remoteHost, remotePort, static_cast<int> (timeout)));
			opened = unpooledConn->open();
		}
	}

	if (opened) {
		setStatus("");
	} else {
		setStatus("Failed to connect");
	}
	return opened;
}

void NetworkStore::close() {
	if (!opened) {
		return;
	}
	opened = false;
	if (useConnPool) {
		if (smcBased) {
			g_connPool.close(smcService);
		} else {
			g_connPool.close(remoteHost, remotePort);
		}
	} else {
		if (unpooledConn != NULL) {
			unpooledConn->close();
		}
	}
}

bool NetworkStore::isOpen() {
	return opened;
}

shared_ptr<Store> NetworkStore::copy(const std::string &category) {
	NetworkStore *store = new NetworkStore(category, multiCategory);
	shared_ptr<Store> copied = shared_ptr<Store> (store);

	store->useConnPool = useConnPool;
	store->smcBased = smcBased;
	store->timeout = timeout;
	store->remoteHost = remoteHost;
	store->remotePort = remotePort;
	store->smcService = smcService;

	return copied;
}

bool NetworkStore::handleMessages(boost::shared_ptr<logentry_vector_t> messages) {
	if (!isOpen()) {
		LOG_OPER("[%s] Logic error: NetworkStore::handleMessages called on closed store", categoryHandled.c_str());
		return false;
	} else if (useConnPool) {
		if (smcBased) {
			return g_connPool.send(smcService, messages);
		} else {
			return g_connPool.send(remoteHost, remotePort, messages);
		}
	} else {
		if (unpooledConn) {
			return unpooledConn->send(messages);
		} else {
			LOG_OPER("[%s] Logic error: NetworkStore::handleMessages unpooledConn is NULL", categoryHandled.c_str());
			return false;
		}
	}
}

void NetworkStore::flush() {
}
/* End of NetworkStore */

/* Start of BucketStore */
BucketStore::BucketStore(const string& category, bool multi_category) :
	Store(category, "bucket", multi_category), bucketType(context_log), delimiter(DEFAULT_BUCKETSTORE_DELIMITER),
	removeKey(false), opened(false), numBuckets(1) {
}

BucketStore::~BucketStore() {
}

void BucketStore::configure(pStoreConf configuration) {
	string error_msg, type, path, bucket_subdir, bucketizer_str, remove_key_str;
	unsigned long delim_long = 0;
	pStoreConf bucket_conf;
	bool needs_bucket_subdir = false;
	bool need_delimiter = false; //set this to true for bucket types that have a delimiter

	if (!configuration->getUnsigned("num_buckets", numBuckets)) {
		error_msg = "Bad config - bucket store must have num_buckets";
		goto handle_error;
	}

	if (!configuration->getStore("bucket", bucket_conf)) {
		error_msg = "Bad config - bucket store must contain another store called <bucket>";
		goto handle_error;
	}

	configuration->getString("bucket_type", bucketizer_str);

	// 散列类型
	if (0 == bucketizer_str.compare("context_log")) {
		bucketType = context_log;
	} else if (0 == bucketizer_str.compare("key_hash")) {
		bucketType = key_hash;
		need_delimiter = true;
	} else if (0 == bucketizer_str.compare("key_modulo")) {
		bucketType = key_modulo;
		need_delimiter = true;
	}

	// 要么散列要么取模
	if (need_delimiter) {
		configuration->getUnsigned("delimiter", delim_long);
		if (delim_long > 255) {
			LOG_OPER("[%s] config warning - delimiter is too large to fit in a char, using default", categoryHandled.c_str());
			delimiter = DEFAULT_BUCKETSTORE_DELIMITER;
		} else if (delim_long == 0) {
			LOG_OPER("[%s] config warning - delimiter is zero, using default", categoryHandled.c_str());
			delimiter = DEFAULT_BUCKETSTORE_DELIMITER;
		} else {
			delimiter = (char) delim_long;
		}

		bucket_conf->setUnsigned("delimiter", delimiter);
	}

	configuration->getString("remove_key", remove_key_str);
	if (remove_key_str == "yes") {
		removeKey = true;

		if (bucketType == context_log) {
			error_msg = "Bad config - bucketizer store of type context_log do not support remove_key";
			goto handle_error;
		}
	}

	bucket_conf->getString("type", type);
	if (0 == type.compare("file") || 0 == type.compare("thriftfile")) {
		needs_bucket_subdir = true;
		if (!configuration->getString("bucket_subdir", bucket_subdir)) {
			error_msg = "Bad config - bucketizer containing file stores must have a bucket_subdir";
			goto handle_error;
		}
		if (!bucket_conf->getString("file_path", path)) {
			error_msg = "Bad config - file store contained by bucketizer must have a file_path";
			goto handle_error;
		}
	} else {
		error_msg = "Bad config - store contained in a bucket store must have a type of either file or thriftfile";
		goto handle_error;
	}

	// 创建 numBuckets + 1 个store. 消息被散列到各个bucket中.
	for (unsigned int i = 0; i <= numBuckets; ++i) {
		shared_ptr<Store> newstore = createStore(type, categoryHandled, false, multiCategory);

		if (!newstore) {
			error_msg = "Bad config - can't create store of type: ";
			error_msg += type;
			goto handle_error;
		}

		// 对于file/thrift等文件桶, 会针对每个bucket创建一个唯一的文件路径
		if (needs_bucket_subdir) {
			// 桶号被追加到文件路径后面
			ostringstream oss(path);
			oss << path << '/' << bucket_subdir << setw(3) << setfill('0') << i;
			bucket_conf->setString("file_path", oss.str());
		}

		buckets.push_back(newstore);
		newstore->configure(bucket_conf);
	}
	return;

	handle_error: setStatus(error_msg);
	LOG_OPER("[%s] %s", categoryHandled.c_str(), error_msg.c_str());
	numBuckets = 0;
	buckets.clear();
}

bool BucketStore::open() {
	// 我们有个专门的桶用来存放不能做散列的消息
	if (numBuckets <= 0 || buckets.size() != numBuckets + 1) {
		LOG_OPER("[%s] Can't open bucket store with <%d> of <%lu> buckets", categoryHandled.c_str(), (int)buckets.size(), numBuckets);
		return false;
	}

	for (std::vector<shared_ptr<Store> >::iterator iter = buckets.begin(); iter != buckets.end(); ++iter) {
		if (!(*iter)->open()) {
			close();
			opened = false;
			return false;
		}
	}
	opened = true;
	return true;
}

bool BucketStore::isOpen() {
	return opened;
}

void BucketStore::close() {
	// 这里不用检查各个store是否打开, 因为在已经关闭的store上调用close没有任何影响.
	for (std::vector<shared_ptr<Store> >::iterator iter = buckets.begin(); iter != buckets.end(); ++iter) {
		(*iter)->close();
	}
	opened = false;
}

void BucketStore::flush() {
	for (std::vector<shared_ptr<Store> >::iterator iter = buckets.begin(); iter != buckets.end(); ++iter) {
		(*iter)->flush();
	}
}

/**
 * 正常情况下store的状态返回空字符串，那么这里要遍历所有store的状态，直到发现一个不为空的status,这时就说明有问题了
 */
string BucketStore::getStatus() {
	string retval = Store::getStatus();

	std::vector<shared_ptr<Store> >::iterator iter = buckets.begin();
	while (retval.empty() && iter != buckets.end()) {
		retval = (*iter)->getStatus();
		++iter;
	}
	return retval;
}

//委托一下就OK
void BucketStore::periodicCheck() {
	for (std::vector<shared_ptr<Store> >::iterator iter = buckets.begin(); iter != buckets.end(); ++iter) {
		(*iter)->periodicCheck();
	}
}

shared_ptr<Store> BucketStore::copy(const std::string &category) {
	BucketStore *store = new BucketStore(category, multiCategory);
	shared_ptr<Store> copied = shared_ptr<Store> (store);

	store->numBuckets = numBuckets;
	store->bucketType = bucketType;
	store->delimiter = delimiter;

	for (std::vector<shared_ptr<Store> >::iterator iter = buckets.begin(); iter != buckets.end(); ++iter) {
		store->buckets.push_back((*iter)->copy(category));
	}

	return copied;
}

bool BucketStore::handleMessages(boost::shared_ptr<logentry_vector_t> messages) {
	bool success = true;

	boost::shared_ptr<logentry_vector_t> failed_messages(new logentry_vector_t);
	boost::shared_ptr<logentry_vector_t> outvector(new logentry_vector_t);
	outvector->resize(1);

	for (logentry_vector_t::iterator iter = messages->begin(); iter != messages->end(); ++iter) {
		unsigned bucket = bucketize((*iter)->message);

		(*outvector)[0] = *iter;

		if (removeKey) {//如果不需要key
			(*outvector)[0]->message = getMessageWithoutKey((*iter)->message);
		}

		if (bucket > numBuckets || !buckets[bucket]->handleMessages(outvector)) {
			LOG_OPER("[%s] Failed to write a message to bucket <%u>", categoryHandled.c_str(), bucket);
			setStatus("Failed write to bucket store");
			success = false;
			failed_messages->push_back(*iter);
		}
	}

	if (!success) {
		//返回失败的消息
		messages->swap(*failed_messages);
	}

	return success;
}

/**
 * 对消息作散列
 */
unsigned long BucketStore::bucketize(const std::string& message) {
	string::size_type length = message.length();

	if (bucketType == context_log) {
		// key为第三个分隔符后的ascii字符串,稍后转换为uint32_t
		char delim = 1;
		string::size_type pos = 0;
		for (int i = 0; i < 3; ++i) {
			pos = message.find(delim, pos);
			if (pos == string::npos || length <= pos + 1) {
				return 0;
			}
			++pos;
		}
		if (message[pos] == delim) {
			return 0;
		}

		uint32_t id = strtoul(message.substr(pos).c_str(), NULL, 10);
		if (id == 0) {
			return 0;
		}

		if (numBuckets == 0) {
			return 0;
		} else {
			return (integerhash::hash32(id) % numBuckets) + 1;
		}

	} else {
		// 对用户定义的第一个分隔符之前的内容作散列
		string::size_type pos = message.find(delimiter);
		if (pos == string::npos) {
			LOG_OPER("[%s] didn't find delimiter <%d> for key_hash of <%s>", categoryHandled.c_str(), delimiter, message.c_str());
			return 0;
		}

		string key = message.substr(0, pos).c_str();
		if (key.empty()) {
			LOG_OPER("[%s] key_hash failed for message <%s>, key is empty string", categoryHandled.c_str(), message.c_str());
			return 0;
		}

		if (numBuckets == 0) {
			LOG_OPER("[%s] skipping key hash because number of buckets is zero", categoryHandled.c_str());
			return 0;
		} else {
			switch (bucketType) {
			case key_modulo:
				// 简单取模而已
				return (atol(key.c_str()) % numBuckets) + 1;
				break;
			case key_hash:
			default:
				// 用个字符串散列函数来搞下.
				return (strhash::hash32(key.c_str()) % numBuckets) + 1;
				break;
			}
		}
	}
	return 0;
}

/**
 * 辅助函数, 干掉用户定义的key部分，只保留消息体部分
 */
string BucketStore::getMessageWithoutKey(const std::string& message) {
	string::size_type pos = message.find(delimiter);

	if (pos == string::npos) {
		LOG_OPER("[%s] didn't find delimiter <%d> for key_hash of <%s>",
				categoryHandled.c_str(), delimiter, message.c_str());
		return message;
	}

	return message.substr(pos + 1);
}
/* End of BucketStore */

/* Start of NullStore */
NullStore::NullStore(const std::string& category, bool multi_category) :
	Store(category, "null", multi_category) {
}

NullStore::~NullStore() {
}

boost::shared_ptr<Store> NullStore::copy(const std::string &category) {
	NullStore *store = new NullStore(category, multiCategory);
	shared_ptr<Store> copied = shared_ptr<Store> (store);
	return copied;
}

bool NullStore::open() {
	return true;
}

bool NullStore::isOpen() {
	return true;
}

void NullStore::configure(pStoreConf) {
}

void NullStore::close() {
}

bool NullStore::handleMessages(boost::shared_ptr<logentry_vector_t>) {
	return true;
}

void NullStore::flush() {
}

bool NullStore::readOldest(/*out*/boost::shared_ptr<logentry_vector_t> messages, struct tm* now) {
	return true;
}

bool NullStore::replaceOldest(boost::shared_ptr<logentry_vector_t> messages, struct tm* now) {
	return true;
}

void NullStore::deleteOldest(struct tm* now) {
}

bool NullStore::empty(struct tm* now) {
	return true;
}
/* End of NullStore */

/* Start of MultiStore */
MultiStore::MultiStore(const std::string& category, bool multi_category) :
	Store(category, "multi", multi_category) {
}

MultiStore::~MultiStore() {
}

boost::shared_ptr<Store> MultiStore::copy(const std::string &category) {
	MultiStore *store = new MultiStore(category, multiCategory);
	store->report_success = this->report_success;
	boost::shared_ptr<Store> tmp_copy;
	for (std::vector<boost::shared_ptr<Store> >::iterator iter = stores.begin(); iter != stores.end(); ++iter) {
		tmp_copy = (*iter)->copy(category);
		store->stores.push_back(tmp_copy);
	}

	return shared_ptr<Store> (store);
}

bool MultiStore::open() {
	bool all_result = true;
	bool any_result = false;
	bool cur_result;
	for (std::vector<boost::shared_ptr<Store> >::iterator iter = stores.begin(); iter != stores.end(); ++iter) {
		cur_result = (*iter)->open();
		any_result |= cur_result;
		all_result &= cur_result;
	}
	return (report_success == SUCCESS_ALL) ? all_result : any_result;
}

bool MultiStore::isOpen() {
	bool all_result = true;
	bool any_result = false;
	bool cur_result;
	for (std::vector<boost::shared_ptr<Store> >::iterator iter = stores.begin(); iter != stores.end(); ++iter) {
		cur_result = (*iter)->isOpen();
		any_result |= cur_result;
		all_result &= cur_result;
	}
	return (report_success == SUCCESS_ALL) ? all_result : any_result;
}

void MultiStore::configure(pStoreConf configuration) {
	/**
	 * 配置格式如下示范:
	 *	<store>
	 *		type=multi
	 *   	report_success=all|any			//必须全部成功，或者只要任意一个成功
	 *   	<store0>
	 *			...
	 *   	</store0>
	 *			...
	 *   	<storen>
	 *			...
	 *		</storen>
	 * </store>
	 */
	pStoreConf cur_conf;
	string cur_type;
	boost::shared_ptr<Store> cur_store;
	string report_preference;

	if (configuration->getString("report_success", report_preference)) {
		if (0 == report_preference.compare("all")) {
			report_success = SUCCESS_ALL;
			LOG_OPER("[%s] MULTI: Logging success only if all stores succeed.", categoryHandled.c_str());
		} else if (0 == report_preference.compare("any")) {
			report_success = SUCCESS_ANY;
			LOG_OPER("[%s] MULTI: Logging success if any store succeeds.", categoryHandled.c_str());
		} else {
			LOG_OPER("[%s] MULTI: %s is an invalid value for report_success.", categoryHandled.c_str(), report_preference.c_str());
			setStatus("MULTI: Invalid report_success value.");
			return;
		}
	} else {
		report_success = SUCCESS_ALL;
	}

	// 遍历所有store的配置
	for (int i = 0;; ++i) {
		stringstream ss;
		ss << "store" << i;
		if (!configuration->getStore(ss.str(), cur_conf)) {
			// 配置可以从0或者1开始都行
			if (i == 0) {
				continue;
			}

			// 到这里就说明配置有问题
			break;
		} else {
			// 获取store的类型
			if (!cur_conf->getString("type", cur_type)) {
				LOG_OPER("[%s] MULTI: Store %d is missing type.", categoryHandled.c_str(), i);
				setStatus("MULTI: Store is missing type.");
				return;
			} else {
				// 创建store, 并添加到store列表
				cur_store = createStore(cur_type, categoryHandled, false, multiCategory);
				LOG_OPER("[%s] MULTI: Configured store of type %s successfully.", categoryHandled.c_str(), cur_type.c_str());
				cur_store->configure(cur_conf);
				stores.push_back(cur_store);
			}
		}
	}

	if (stores.size() == 0) {
		setStatus("MULTI: No stores found, invalid store.");
		LOG_OPER("[%s] MULTI: No stores found, invalid store.", categoryHandled.c_str());
	}
}

void MultiStore::close() {
	for (std::vector<boost::shared_ptr<Store> >::iterator iter = stores.begin(); iter != stores.end(); ++iter) {
		(*iter)->close();
	}
}

bool MultiStore::handleMessages(boost::shared_ptr<logentry_vector_t> messages) {
	bool all_result = true;
	bool any_result = false;
	bool cur_result;
	for (std::vector<boost::shared_ptr<Store> >::iterator iter = stores.begin(); iter != stores.end(); ++iter) {
		cur_result = (*iter)->handleMessages(messages);
		any_result |= cur_result;
		all_result &= cur_result;
	}

	// 注意: 因为可能有的成功，有的部分或完全失败, 这种情况时要考虑.
	return (report_success == SUCCESS_ALL) ? all_result : any_result;
}

void MultiStore::periodicCheck() {
	for (std::vector<boost::shared_ptr<Store> >::iterator iter = stores.begin(); iter != stores.end(); ++iter) {
		(*iter)->periodicCheck();
	}
}

void MultiStore::flush() {
	for (std::vector<boost::shared_ptr<Store> >::iterator iter = stores.begin(); iter != stores.end(); ++iter) {
		(*iter)->flush();
	}
}
/* End of MultiStore */

/* Start of CategoryStore */
CategoryStore::CategoryStore(const std::string& category, bool multiCategory) :
	Store(category, "category", multiCategory) {
}

CategoryStore::CategoryStore(const std::string& category, const std::string& name, bool multiCategory) :
	Store(category, name, multiCategory) {
}

CategoryStore::~CategoryStore() {
}

boost::shared_ptr<Store> CategoryStore::copy(const std::string &category) {
	CategoryStore *store = new CategoryStore(category, multiCategory);

	store->modelStore = modelStore->copy(category);

	return shared_ptr<Store> (store);
}

bool CategoryStore::open() {
	bool result = true;

	for (map<string, shared_ptr<Store> >::iterator iter = stores.begin(); iter != stores.end(); ++iter) {
		result &= iter->second->open();
	}

	return result;
}

bool CategoryStore::isOpen() {
	for (map<string, shared_ptr<Store> >::iterator iter = stores.begin(); iter != stores.end(); ++iter) {
		if (!iter->second->isOpen()) {
			return false;
		}
	}
	return true;
}

void CategoryStore::configure(pStoreConf configuration) {
	/**
	 * 用一个原型model来创建新的对象，原型模式.
	 *  <store>
	 *    type=category
	 *    <model>
	 *      type=...
	 *      ...
	 *    </model>
	 *  </store>
	 */
	pStoreConf cur_conf;

	if (!configuration->getStore("model", cur_conf)) {
		setStatus("CATEGORYSTORE: NO stores found, invalid store.");
		LOG_OPER("[%s] CATEGORYSTORE: No stores found, invalid store.", categoryHandled.c_str());
	} else {
		string cur_type;

		if (!cur_conf->getString("type", cur_type)) {
			LOG_OPER("[%s] CATEGORYSTORE: Store is missing type.", categoryHandled.c_str());
			setStatus("CATEGORYSTORE: Store is missing type.");
			return;
		}

		configureCommon(cur_conf, cur_type);
	}
}

void CategoryStore::configureCommon(pStoreConf configuration, const string type) {
	// initialize model store
	modelStore = createStore(type, categoryHandled, false, false);
	LOG_OPER("[%s] %s: Configured store of type %s successfully.", categoryHandled.c_str(), getType().c_str(), type.c_str());
	modelStore->configure(configuration);
}

void CategoryStore::close() {
	for (map<string, shared_ptr<Store> >::iterator iter = stores.begin(); iter != stores.end(); ++iter) {
		iter->second->close();
	}
}

bool CategoryStore::handleMessages(boost::shared_ptr<logentry_vector_t> messages) {
	shared_ptr<logentry_vector_t> singleMessage(new logentry_vector_t);
	shared_ptr<logentry_vector_t> failed_messages(new logentry_vector_t);
	logentry_vector_t::iterator message_iter;

	for (message_iter = messages->begin(); message_iter != messages->end(); ++message_iter) {
		map<string, shared_ptr<Store> >::iterator store_iter;
		shared_ptr<Store> store;
		string category = (*message_iter)->category;

		store_iter = stores.find(category);

		if (store_iter == stores.end()) {
			// 为新的类别构建一个新的store, 复制一个原型model而已
			store = modelStore->copy(category);
			store->open();
			stores[category] = store;
		} else {
			store = store_iter->second;
		}

		if (store == NULL || !store->isOpen()) {
			LOG_OPER("[%s] Failed to open store for category <%s>", categoryHandled.c_str(), category.c_str());
			failed_messages->push_back(*message_iter);
			continue;
		}

		// 把消息发送到指定类别
		(*singleMessage)[0] = (*message_iter);

		if (!store->handleMessages(singleMessage)) {
			LOG_OPER("[%s] Failed to handle message for category <%s>", categoryHandled.c_str(), category.c_str());
			failed_messages->push_back(*message_iter);
			continue;
		}
	}

	if (!failed_messages->empty()) {
		// 如果有失效的就记录下来
		messages->swap(*failed_messages);
		return false;
	} else {
		return true;
	}
}

void CategoryStore::periodicCheck() {
	for (map<string, shared_ptr<Store> >::iterator iter = stores.begin(); iter != stores.end(); ++iter) {
		iter->second->periodicCheck();
	}
}

void CategoryStore::flush() {
	for (map<string, shared_ptr<Store> >::iterator iter = stores.begin(); iter != stores.end(); ++iter) {
		iter->second->flush();
	}
}
/* End of CategoryStore */

/* Start of MultiFileStore */
MultiFileStore::MultiFileStore(const std::string& category, bool multi_category) :
	CategoryStore(category, "MultiFileStore", multi_category) {
}

MultiFileStore::~MultiFileStore() {
}

void MultiFileStore::configure(pStoreConf configuration) {
	configureCommon(configuration, "file");
}
/* Start of MultiFileStore */

/* Start of ThriftMultiFileStore */
ThriftMultiFileStore::ThriftMultiFileStore(const std::string& category, bool multi_category) :
	CategoryStore(category, "ThriftMultiFileStore", multi_category) {
}

ThriftMultiFileStore::~ThriftMultiFileStore() {
}

void ThriftMultiFileStore::configure(pStoreConf configuration) {
	configureCommon(configuration, "thriftfile");
}
/* End of ThriftMultiFileStore */
