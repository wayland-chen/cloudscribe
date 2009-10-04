#ifndef FORWARDER_FILE_H
#define FORWARDER_FILE_H

#include <fstream>
#include <iostream>
#include <vector>
#include <string>

#include <boost/shared_ptr.hpp>



class FileInterface {
public:
	FileInterface(const std::string& name, bool framed);
	virtual ~FileInterface();

	static boost::shared_ptr<FileInterface> createFileInterface(const std::string& type, const std::string& name, bool framed = false);
	static std::vector<std::string> list(const std::string& path, const std::string& fsType);

	virtual bool openRead() = 0;
	virtual bool openWrite() = 0;
	virtual bool openTruncate() = 0;
	virtual bool isOpen() = 0;
	virtual void close() = 0;
	virtual bool write(const std::string& data) = 0;
	virtual void flush() = 0;
	virtual unsigned long fileSize() = 0;
	virtual bool readNext(std::string& _return) = 0; // returns a line if unframed or a record if framed
	virtual void deleteFile() = 0;
	virtual void listImpl(const std::string& path, std::vector<std::string>& _return) = 0;
	virtual std::string getFrame(unsigned data_size) {
		return std::string();
	}
	;

protected:
	bool framed;
	std::string filename;

	unsigned unserializeUInt(const char* buffer);
	void serializeUInt(unsigned data, char* buffer);
};

class StdFile: public FileInterface {
public:
	StdFile(const std::string& name, bool framed);
	virtual ~StdFile();

	bool openRead();
	bool openWrite();
	bool openTruncate();
	bool isOpen();
	void close();
	bool write(const std::string& data);
	void flush();
	unsigned long fileSize();
	bool readNext(std::string& _return);
	void deleteFile();
	void listImpl(const std::string& path, std::vector<std::string>& _return);
	std::string getFrame(unsigned data_size);

private:
	bool open(std::ios_base::openmode mode);

	char* inputBuffer;
	unsigned bufferSize;
	std::fstream file;

	// 不允许拷贝，赋值和空构造
	StdFile();
	StdFile(StdFile& rhs);
	StdFile& operator=(StdFile& rhs);
};

#endif // !defined FORWARDER_FILE_H
