/**
 * author: edisonpeng@tencent.com
 */
#ifndef FORWARDER_STORE_H
#define FORWARDER_STORE_H

//#include "common.h" // includes std libs, thrift, and stl typedefs
#include <boost/shared_ptr.hpp>
#include <boost/filesystem/operations.hpp>

#include "conf.h"
#include "file.h"
#include "conn_pool.h"

/* defines used by the store class */
enum roll_period_t {
	ROLL_NEVER, ROLL_HOURLY, ROLL_DAILY
};

/*
 * 抽象类,定义了具体store必须实现的接口,并完成了一些基本功能.
 */
class Store {
public:
	// 创建一个相应的具体子类.
	static boost::shared_ptr<Store> createStore(const std::string& type, const std::string& category, bool readable = false, bool multi_category = false);

	Store(const std::string& category, const std::string &type, bool multi_category = false);
	virtual ~Store();

	virtual boost::shared_ptr<Store> copy(const std::string &category) = 0;
	virtual bool open() = 0;
	virtual bool isOpen() = 0;
	virtual void configure(pStoreConf configuration) = 0;
	virtual void close() = 0;

	// 尝试存储消息, 如果成功则返回true.
	// 如果失败则返回false, 此时,messages中包含的是没有经过处理的消息.
	virtual bool handleMessages(boost::shared_ptr<logentry_vector_t> messages) = 0;
	virtual void periodicCheck() {
	}

	virtual void flush() = 0;

	virtual std::string getStatus();

	//以下方法必须由具体子类来实现.
	virtual bool readOldest(/*out*/boost::shared_ptr<logentry_vector_t> messages, struct tm* now);
	virtual void deleteOldest(struct tm* now);
	virtual bool replaceOldest(boost::shared_ptr<logentry_vector_t> messages, struct tm* now);
	virtual bool empty(struct tm* now);

	// don't need to override
	virtual const std::string& getType();

protected:
	virtual void setStatus(const std::string& new_status);
	std::string status;
	std::string categoryHandled;
	bool multiCategory; // 是否支持多个类别的处理
	std::string storeType;

	// 不要同时锁定多个store
	pthread_mutex_t statusMutex;

private:
	// 禁止拷贝,赋值,也不允许用空构造函数
	Store(Store& rhs);
	Store& operator=(Store& rhs);
};

/*
 * 基于文件存储的抽象类.
 * 此类负责命名逻辑,并决定什么时候进行滚动.
 */
class FileStoreBase: public Store {
public:
	FileStoreBase(const std::string& category, const std::string &type, bool multi_category);
	~FileStoreBase();

	virtual void copyCommon(const FileStoreBase *base);
	bool open();
	void configure(pStoreConf configuration);
	void periodicCheck();

protected:
	// 这里需要传递一些额外的参数.
	// 外部的open函数只需要传递默认参数即可.
	virtual bool openInternal(bool incrementFilename, struct tm* current_time) = 0;
	virtual void rotateFile(struct tm *timeinfo);

	// 把当前文件的一些信息写下log,也存放在与log文件相同的目录.
	virtual void printStats();

	// 返回需要对齐到block尺寸的剩余字节大小
	unsigned long bytesToPad(unsigned long next_message_length, unsigned long current_file_size, unsigned long chunk_size);

	// 一个完整文件名，包括绝对路径和序列号后缀
	std::string makeBaseFilename(struct tm* creation_time = NULL);
	std::string makeFullFilename(int suffix, struct tm* creation_time = NULL);
	std::string makeBaseSymlink();
	std::string makeFullSymlink();
	int findOldestFile(const std::string& base_filename);
	int findNewestFile(const std::string& base_filename);
	int getFileSuffix(const std::string& filename, const std::string& base_filename);

	// 配置
	std::string filePath;
	std::string baseFileName;
	unsigned long maxSize;
	roll_period_t rollPeriod;
	unsigned long rollHour;
	unsigned long rollMinute;
	std::string fsType;
	unsigned long chunkSize;
	bool writeMeta;
	bool writeCategory;
	bool createSymlink;

	// 状态
	unsigned long currentSize;
	int lastRollTime; // hour 或 day, 取决于rollPeriod
	std::string currentFilename; // 这个并不作为下一个文件的命名选择之用，纯粹只用作状态报告
	unsigned long eventsWritten; // 当前文件的写请求数

private:
	//不允许拷贝，赋值和空构造
	FileStoreBase(FileStoreBase& rhs);
	FileStoreBase& operator=(FileStoreBase& rhs);
};

/*
 * 基于文件的store实现, 把操作都委托到FileInterface, FileInterface负责与具体文件系统的交互. (see file.h)
 */
class FileStore: public FileStoreBase {
public:
	FileStore(const std::string& category, bool multi_category, bool is_buffer_file = false);
	~FileStore();

	boost::shared_ptr<Store> copy(const std::string &category);
	bool handleMessages(boost::shared_ptr<logentry_vector_t> messages);
	bool isOpen();
	void configure(pStoreConf configuration);
	void close();
	void flush();

	// 每次读都会open并close文件，并且是把文件整个读入进来.
	bool readOldest(/*out*/boost::shared_ptr<logentry_vector_t> messages, struct tm* now);
	virtual bool replaceOldest(boost::shared_ptr<logentry_vector_t> messages, struct tm* now);
	void deleteOldest(struct tm* now);
	bool empty(struct tm* now);

protected:
	// 实现FileStoreBase的virtual方法
	bool openInternal(bool incrementFilename, struct tm* current_time);
	bool writeMessages(boost::shared_ptr<logentry_vector_t> messages, boost::shared_ptr<FileInterface> write_file);

	bool isBufferFile;
	bool addNewlines;

	// 状态
	boost::shared_ptr<FileInterface> writeFile;

private:
	//不允许拷贝，赋值和空构造
	FileStore(FileStore& rhs);
	FileStore& operator=(FileStore& rhs);
};

/*
 * 基于thrift TFileTransport的文件store
 */
class ThriftFileStore: public FileStoreBase {
public:
	ThriftFileStore(const std::string& category, bool multi_category);
	~ThriftFileStore();

	boost::shared_ptr<Store> copy(const std::string &category);
	bool handleMessages(boost::shared_ptr<logentry_vector_t> messages);
	bool open();
	bool isOpen();
	void configure(pStoreConf configuration);
	void close();
	void flush();

protected:
	// 实现FileStoreBase的virtual方法
	bool openInternal(bool incrementFilename, struct tm* current_time);

	boost::shared_ptr<apache::thrift::transport::TFileTransport> thriftFileTransport;

	unsigned long flushFrequencyMs;
	unsigned long msgBufferSize;

private:
	//不允许拷贝，赋值和空构造
	ThriftFileStore(ThriftFileStore& rhs);
	ThriftFileStore& operator=(ThriftFileStore& rhs);
};

/*
 * BufferStore负责合并消息并把它们转发到另外的forwarder.
 * 如果发送时失败,它会把消息放入secondary store, 稍后再来重传.
 *
 * 这里有两种buffer, 消息主要是buffer在内存中,  如果主store down掉了, 那就启用secondary store.
 */
class BufferStore: public Store {

public:
	BufferStore(const std::string& category, bool multi_category);
	~BufferStore();

	boost::shared_ptr<Store> copy(const std::string &category);
	bool handleMessages(boost::shared_ptr<logentry_vector_t> messages);
	bool open();
	bool isOpen();
	void configure(pStoreConf configuration);
	void close();
	void flush();
	void periodicCheck();

	std::string getStatus();

protected:
	boost::shared_ptr<Store> primaryStore;

	boost::shared_ptr<Store> secondaryStore;

	// buffer的状态机
	enum buffer_state_t {
		STREAMING, // 连接到primary store并直接发送
		DISCONNECTED, // 与primary store失去连接, 此时启用secondary store
		SENDING_BUFFER
	// 处于往主store发送secondary store中数据的状态.
	};

	void changeState(buffer_state_t new_state); // 状态转换
	const char* stateAsString(buffer_state_t state);

	time_t getNewRetryInterval(); // 基于配置来生成随机interval

	// 配置
	unsigned long maxQueueLength; // 消息总数量
	unsigned long bufferSendRate; // 每次periodicCheck时可以发送的buffer文件数
	time_t avgRetryInterval; // in seconds, for retrying primary store open
	time_t retryIntervalRange; // in seconds

	// 状态
	buffer_state_t state;
	time_t lastWriteTime;
	time_t lastOpenAttempt;
	time_t retryInterval;

private:
	//不允许拷贝，赋值和空构造
	BufferStore();
	BufferStore(BufferStore& rhs);
	BufferStore& operator=(BufferStore& rhs);
};

/*
 * NetworkStore发送消息到另外的forwarder server. 此类只是全局连接池g_connPool的一个adapter角色.
 */
class NetworkStore: public Store {
public:
	NetworkStore(const std::string& category, bool multi_category);
	~NetworkStore();

	boost::shared_ptr<Store> copy(const std::string &category);
	bool handleMessages(boost::shared_ptr<logentry_vector_t> messages);
	bool open();
	bool isOpen();
	void configure(pStoreConf configuration);
	void close();
	void flush();

protected:
	static const long int DEFAULT_SOCKET_TIMEOUT_MS = 5000; // 5 sec timeout

	// 配置
	bool useConnPool;
	bool smcBased;
	long int timeout;
	std::string remoteHost;
	unsigned long remotePort; // long because it works with config code
	std::string smcService;

	// 状态
	bool opened;
	boost::shared_ptr<forwarderConn> unpooledConn; // null if useConnPool

private:
	//不允许拷贝，赋值和空构造
	NetworkStore();
	NetworkStore(NetworkStore& rhs);
	NetworkStore& operator=(NetworkStore& rhs);
};

/*
 * BucketStore把消息用hash函数散列后分到很多个组中, 并把每个消息组发送到不同的store中.
 */
class BucketStore: public Store {
public:
	BucketStore(const std::string& category, bool multi_category);
	~BucketStore();

	boost::shared_ptr<Store> copy(const std::string &category);
	bool handleMessages(boost::shared_ptr<logentry_vector_t> messages);
	bool open();
	bool isOpen();
	void configure(pStoreConf configuration);
	void close();
	void flush();
	void periodicCheck();
	std::string getStatus();

protected:
	enum bucketizer_type {
		context_log, key_hash, key_modulo
	};

	bucketizer_type bucketType;
	char delimiter;
	bool removeKey;
	bool opened;
	unsigned long numBuckets;
	std::vector<boost::shared_ptr<Store> > buckets;

	unsigned long bucketize(const std::string& message);
	std::string getMessageWithoutKey(const std::string& message);

private:
	//不允许拷贝，赋值和空构造
	BucketStore();
	BucketStore(BucketStore& rhs);
	BucketStore& operator=(BucketStore& rhs);
};

/*
 * 空洞.
 */
class NullStore: public Store {
public:
	NullStore(const std::string& category, bool multi_category);
	virtual ~NullStore();

	boost::shared_ptr<Store> copy(const std::string &category);
	bool open();
	bool isOpen();
	void configure(pStoreConf configuration);
	void close();

	bool handleMessages(boost::shared_ptr<logentry_vector_t> messages);
	void flush();

	// 读出来的东西是空的
	virtual bool readOldest(/*out*/boost::shared_ptr<logentry_vector_t> messages, struct tm* now);
	virtual bool replaceOldest(boost::shared_ptr<logentry_vector_t> messages, struct tm* now);
	virtual void deleteOldest(struct tm* now);
	virtual bool empty(struct tm* now);

private:
	//不允许拷贝，赋值和空构造
	NullStore();
	NullStore(Store& rhs);
	NullStore& operator=(Store& rhs);
};

/*
 * 把消息发送到多个store
 */
class MultiStore: public Store {
public:
	MultiStore(const std::string& category, bool multi_category);
	~MultiStore();

	boost::shared_ptr<Store> copy(const std::string &category);
	bool open();
	bool isOpen();
	void configure(pStoreConf configuration);
	void close();

	bool handleMessages(boost::shared_ptr<logentry_vector_t> messages);
	void periodicCheck();
	void flush();

	// 这个读是没有意义的，因为有多个store,我们根本不知道应该从哪个store中读取，也不适合把所有store的消息都拿过来
	bool readOldest(/*out*/boost::shared_ptr<logentry_vector_t> messages, struct tm* now) {
		return false;
	}
	void deleteOldest(struct tm* now) {
	}
	bool empty(struct tm* now) {
		return true;
	}

protected:
	std::vector<boost::shared_ptr<Store> > stores;
	enum report_success_value {
		SUCCESS_ANY = 1, SUCCESS_ALL
	};
	report_success_value report_success;

private:
	//不允许拷贝，赋值和空构造
	MultiStore();
	MultiStore(Store& rhs);
	MultiStore& operator=(Store& rhs);
};

/*
 * 会对每个类别都使用单独的store.
 */
class CategoryStore: public Store {
public:
	CategoryStore(const std::string& category, bool multi_category);
	CategoryStore(const std::string& category, const std::string& name, bool multiCategory);
	~CategoryStore();

	boost::shared_ptr<Store> copy(const std::string &category);
	bool open();
	bool isOpen();
	void configure(pStoreConf configuration);
	void close();

	bool handleMessages(boost::shared_ptr<logentry_vector_t> messages);
	void periodicCheck();
	void flush();

protected:
	void configureCommon(pStoreConf configuration, const std::string type);
	boost::shared_ptr<Store> modelStore;
	std::map<std::string, boost::shared_ptr<Store> > stores;

private:
	CategoryStore();
	CategoryStore(Store& rhs);
	CategoryStore& operator=(Store& rhs);
};

/*
 * 对每个类别使用一个单独的文件. 主要用在一个store对应多个类别的情况.
 */
class MultiFileStore: public CategoryStore {
public:
	MultiFileStore(const std::string& category, bool multi_category);
	~MultiFileStore();
	void configure(pStoreConf configuration);

private:
	MultiFileStore();
	MultiFileStore(Store& rhs);
	MultiFileStore& operator=(Store& rhs);
};

/*
 * ThriftMultiFileStore类似于ThriftFileStore, 区别在于它对每个类别使用单独的thrift文件.
 */
class ThriftMultiFileStore: public CategoryStore {
public:
	ThriftMultiFileStore(const std::string& category, bool multi_category);
	~ThriftMultiFileStore();
	void configure(pStoreConf configuration);

private:
	ThriftMultiFileStore();
	ThriftMultiFileStore(Store& rhs);
	ThriftMultiFileStore& operator=(Store& rhs);
};
#endif // FORWARDER_STORE_H
