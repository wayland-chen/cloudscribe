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
 * ������,�����˾���store����ʵ�ֵĽӿ�,�������һЩ��������.
 */
class Store {
public:
	// ����һ����Ӧ�ľ�������.
	static boost::shared_ptr<Store> createStore(const std::string& type, const std::string& category, bool readable = false, bool multi_category = false);

	Store(const std::string& category, const std::string &type, bool multi_category = false);
	virtual ~Store();

	virtual boost::shared_ptr<Store> copy(const std::string &category) = 0;
	virtual bool open() = 0;
	virtual bool isOpen() = 0;
	virtual void configure(pStoreConf configuration) = 0;
	virtual void close() = 0;

	// ���Դ洢��Ϣ, ����ɹ��򷵻�true.
	// ���ʧ���򷵻�false, ��ʱ,messages�а�������û�о����������Ϣ.
	virtual bool handleMessages(boost::shared_ptr<logentry_vector_t> messages) = 0;
	virtual void periodicCheck() {
	}

	virtual void flush() = 0;

	virtual std::string getStatus();

	//���·��������ɾ���������ʵ��.
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
	bool multiCategory; // �Ƿ�֧�ֶ�����Ĵ���
	std::string storeType;

	// ��Ҫͬʱ�������store
	pthread_mutex_t statusMutex;

private:
	// ��ֹ����,��ֵ,Ҳ�������ÿչ��캯��
	Store(Store& rhs);
	Store& operator=(Store& rhs);
};

/*
 * �����ļ��洢�ĳ�����.
 * ���ฺ�������߼�,������ʲôʱ����й���.
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
	// ������Ҫ����һЩ����Ĳ���.
	// �ⲿ��open����ֻ��Ҫ����Ĭ�ϲ�������.
	virtual bool openInternal(bool incrementFilename, struct tm* current_time) = 0;
	virtual void rotateFile(struct tm *timeinfo);

	// �ѵ�ǰ�ļ���һЩ��Ϣд��log,Ҳ�������log�ļ���ͬ��Ŀ¼.
	virtual void printStats();

	// ������Ҫ���뵽block�ߴ��ʣ���ֽڴ�С
	unsigned long bytesToPad(unsigned long next_message_length, unsigned long current_file_size, unsigned long chunk_size);

	// һ�������ļ�������������·�������кź�׺
	std::string makeBaseFilename(struct tm* creation_time = NULL);
	std::string makeFullFilename(int suffix, struct tm* creation_time = NULL);
	std::string makeBaseSymlink();
	std::string makeFullSymlink();
	int findOldestFile(const std::string& base_filename);
	int findNewestFile(const std::string& base_filename);
	int getFileSuffix(const std::string& filename, const std::string& base_filename);

	// ����
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

	// ״̬
	unsigned long currentSize;
	int lastRollTime; // hour �� day, ȡ����rollPeriod
	std::string currentFilename; // ���������Ϊ��һ���ļ�������ѡ��֮�ã�����ֻ����״̬����
	unsigned long eventsWritten; // ��ǰ�ļ���д������

private:
	//������������ֵ�Ϳչ���
	FileStoreBase(FileStoreBase& rhs);
	FileStoreBase& operator=(FileStoreBase& rhs);
};

/*
 * �����ļ���storeʵ��, �Ѳ�����ί�е�FileInterface, FileInterface����������ļ�ϵͳ�Ľ���. (see file.h)
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

	// ÿ�ζ�����open��close�ļ��������ǰ��ļ������������.
	bool readOldest(/*out*/boost::shared_ptr<logentry_vector_t> messages, struct tm* now);
	virtual bool replaceOldest(boost::shared_ptr<logentry_vector_t> messages, struct tm* now);
	void deleteOldest(struct tm* now);
	bool empty(struct tm* now);

protected:
	// ʵ��FileStoreBase��virtual����
	bool openInternal(bool incrementFilename, struct tm* current_time);
	bool writeMessages(boost::shared_ptr<logentry_vector_t> messages, boost::shared_ptr<FileInterface> write_file);

	bool isBufferFile;
	bool addNewlines;

	// ״̬
	boost::shared_ptr<FileInterface> writeFile;

private:
	//������������ֵ�Ϳչ���
	FileStore(FileStore& rhs);
	FileStore& operator=(FileStore& rhs);
};

/*
 * ����thrift TFileTransport���ļ�store
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
	// ʵ��FileStoreBase��virtual����
	bool openInternal(bool incrementFilename, struct tm* current_time);

	boost::shared_ptr<apache::thrift::transport::TFileTransport> thriftFileTransport;

	unsigned long flushFrequencyMs;
	unsigned long msgBufferSize;

private:
	//������������ֵ�Ϳչ���
	ThriftFileStore(ThriftFileStore& rhs);
	ThriftFileStore& operator=(ThriftFileStore& rhs);
};

/*
 * BufferStore����ϲ���Ϣ��������ת���������forwarder.
 * �������ʱʧ��,�������Ϣ����secondary store, �Ժ������ش�.
 *
 * ����������buffer, ��Ϣ��Ҫ��buffer���ڴ���,  �����store down����, �Ǿ�����secondary store.
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

	// buffer��״̬��
	enum buffer_state_t {
		STREAMING, // ���ӵ�primary store��ֱ�ӷ���
		DISCONNECTED, // ��primary storeʧȥ����, ��ʱ����secondary store
		SENDING_BUFFER
	// ��������store����secondary store�����ݵ�״̬.
	};

	void changeState(buffer_state_t new_state); // ״̬ת��
	const char* stateAsString(buffer_state_t state);

	time_t getNewRetryInterval(); // �����������������interval

	// ����
	unsigned long maxQueueLength; // ��Ϣ������
	unsigned long bufferSendRate; // ÿ��periodicCheckʱ���Է��͵�buffer�ļ���
	time_t avgRetryInterval; // in seconds, for retrying primary store open
	time_t retryIntervalRange; // in seconds

	// ״̬
	buffer_state_t state;
	time_t lastWriteTime;
	time_t lastOpenAttempt;
	time_t retryInterval;

private:
	//������������ֵ�Ϳչ���
	BufferStore();
	BufferStore(BufferStore& rhs);
	BufferStore& operator=(BufferStore& rhs);
};

/*
 * NetworkStore������Ϣ�������forwarder server. ����ֻ��ȫ�����ӳ�g_connPool��һ��adapter��ɫ.
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

	// ����
	bool useConnPool;
	bool smcBased;
	long int timeout;
	std::string remoteHost;
	unsigned long remotePort; // long because it works with config code
	std::string smcService;

	// ״̬
	bool opened;
	boost::shared_ptr<forwarderConn> unpooledConn; // null if useConnPool

private:
	//������������ֵ�Ϳչ���
	NetworkStore();
	NetworkStore(NetworkStore& rhs);
	NetworkStore& operator=(NetworkStore& rhs);
};

/*
 * BucketStore����Ϣ��hash����ɢ�к�ֵ��ܶ������, ����ÿ����Ϣ�鷢�͵���ͬ��store��.
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
	//������������ֵ�Ϳչ���
	BucketStore();
	BucketStore(BucketStore& rhs);
	BucketStore& operator=(BucketStore& rhs);
};

/*
 * �ն�.
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

	// �������Ķ����ǿյ�
	virtual bool readOldest(/*out*/boost::shared_ptr<logentry_vector_t> messages, struct tm* now);
	virtual bool replaceOldest(boost::shared_ptr<logentry_vector_t> messages, struct tm* now);
	virtual void deleteOldest(struct tm* now);
	virtual bool empty(struct tm* now);

private:
	//������������ֵ�Ϳչ���
	NullStore();
	NullStore(Store& rhs);
	NullStore& operator=(Store& rhs);
};

/*
 * ����Ϣ���͵����store
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

	// �������û������ģ���Ϊ�ж��store,���Ǹ�����֪��Ӧ�ô��ĸ�store�ж�ȡ��Ҳ���ʺϰ�����store����Ϣ���ù���
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
	//������������ֵ�Ϳչ���
	MultiStore();
	MultiStore(Store& rhs);
	MultiStore& operator=(Store& rhs);
};

/*
 * ���ÿ�����ʹ�õ�����store.
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
 * ��ÿ�����ʹ��һ���������ļ�. ��Ҫ����һ��store��Ӧ����������.
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
 * ThriftMultiFileStore������ThriftFileStore, ������������ÿ�����ʹ�õ�����thrift�ļ�.
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
