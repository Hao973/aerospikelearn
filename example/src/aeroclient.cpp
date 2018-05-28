//AeroClient.cpp
#include "aeroclient.hpp"
#include "stdio.h"

AeroClient::AeroClient(int connnum, int timeout, int port, const std::string &hosts) {
	// 初始化信息
	as_config t_config;
	as_config_init(&t_config);
	as_config_add_hosts(&t_config, hosts.c_str(), port);
	t_config.max_conns_per_node = connnum;
	t_config.async_max_conns_per_node = 0;
	t_config.pipe_max_conns_per_node = 0;
	t_config.conn_timeout_ms = timeout;
	t_config.policies.read.timeout = timeout;
	t_config.tender_interval = 1000;
	t_config.thread_pool_size = 0;
	aerospike_init(&m_asContext, &t_config);
	m_strHosts = hosts;        
};

AeroClient::~AeroClient() {
	if (m_asContext.cluster != NULL) {
		aerospike_close(&m_asContext, &m_asError);
	}
	aerospike_destroy(&m_asContext);
};

bool AeroClient::Connect(std::string &str_reason) {
	if (aerospike_connect(&m_asContext, &m_asError) != AEROSPIKE_OK) {
		str_reason = m_asError.message;
		return false;
	}
	return true;
};

bool AeroClient::Close(std::string &str_reason) {
	if (aerospike_close(&m_asContext, &m_asError) != AEROSPIKE_OK) {
		str_reason = m_asError.message;
		return false;
	}
	return true;
}

bool AeroClient::GetValMultiSet(const std::string& str_namespace, 
					const std::string &str_set, 
					const std::string str_key, 
					std::string &str_result, 
					std::string &str_reason) {
	if (!m_asContext.cluster)
		return false;
	
	if (str_key.empty())
		return false;

	as_key askey;
	as_key_init(&askey, str_namespace.c_str(), str_set.c_str(), str_key.c_str());
	as_record* p_rec = NULL;
	if (aerospike_key_get(&m_asContext, &m_asError, NULL, &askey, &p_rec) != AEROSPIKE_OK || !p_rec) {
		str_reason = m_asError.message;
		return false;
	}
	str_result.clear(); 
	_DecodeRecordWithBin(p_rec, str_result);
	as_record_destroy(p_rec);
	return true;
}

bool AeroClient::GetValBin(const std::string& str_namespace, 
			   const std::string &str_set,
			   const std::string &str_bin, 
			   const std::string str_key, 
			   std::string &str_result, 
			   std::string &str_reason) {
	std::vector<std::string> vec_bin;
	vec_bin.push_back(str_bin);

	return GetValMultiBin(str_namespace, str_set, vec_bin, str_key, str_result, str_reason);
}

bool AeroClient::GetValMultiBin(const std::string& str_namespace, 
			   const std::string &str_set,
			   std::vector<std::string> &vec_bin, 
			   const std::string str_key, 
			   std::string &str_result, 
			   std::string &str_reason) {
	if (!m_asContext.cluster)
		return false;

	if (str_key.empty())
		return false;

	as_key askey;
	as_key_init(&askey, str_namespace.c_str(), str_set.c_str(), str_key.c_str());
	as_record* p_rec = NULL;
	// const char* bin[1] = {str_bin.c_str()};
	char* select[100]; // = {"bin1", "bin2", "bin3", NULL};
	for (int i = 0; i < vec_bin.size(); ++i)
	{
		select[i] = (char*)vec_bin[i].c_str();
	}
	select[vec_bin.size()] = NULL;
	if (aerospike_key_select(&m_asContext, &m_asError, NULL, &askey, (const char**)select, &p_rec) != AEROSPIKE_OK || !p_rec) {
		str_reason = m_asError.message;
		return false;
	} 
	str_result.clear();
	_DecodeRecord(p_rec, str_result);
	as_record_destroy(p_rec);
	return true;
}

bool AeroClient::PutValBin(const std::string& str_namespace, 
				const std::string &str_set,
				const std::string &str_bin, 
				const std::string str_key,
				std::string &str_val,
				std::string &str_reason,
				int n_ttl) {
	if (!m_asContext.cluster)
		return false;

	as_key askey;
	as_key_init(&askey, str_namespace.c_str(), str_set.c_str(), str_key.c_str());
	as_record rec;
	as_record_inita(&rec, 1);
	as_record_set_str(&rec, str_bin.c_str(), str_val.c_str());
	rec.ttl = n_ttl;
	if (aerospike_key_put(&m_asContext, &m_asError, NULL, &askey, &rec) != AEROSPIKE_OK) {
		str_reason = m_asError.message;
		return false;
	}
	as_record_destroy(&rec);
	return true;
}

void AeroClient::WrietRecords()
{
	//Initializing Record Data
	as_record rec;
	as_record_inita(&rec, 2);
	as_record_set_int64(&rec, "test-bin-1", 1234);
	as_record_set_str(&rec, "test-bin-2", "test-bin-2-data");
	
	//Initializing the Key
	as_key key;
	as_key_init_str(&key, "test", "test-set", "test-key");
	as_error err;
	//Writing to the Database
	if (aerospike_key_put(&m_asContext, &err, NULL, &key, &rec) != AEROSPIKE_OK) {
		fprintf(stderr, "aerospike_key_put err(%d) %s at [%s:%d]\n", err.code, err.message, err.file, err.line);
		return;
	}
	printf("aerospike_key_put ok\n");
	return;
}

void AeroClient::ReadRecords()
{
	std::string str_result;
	//Initializing the Key
	as_key key;
	as_key_init_str(&key, "test", "test-set", "test-key");
	
	//Reading All Bins in a Record
	as_record* p_rec = NULL;
	as_error err;
	
	//Reading All Bins in a Record
	if (aerospike_key_get(&m_asContext, &err, NULL, &key, &p_rec) != AEROSPIKE_OK) {
		fprintf(stderr, "err(%d) %s at [%s:%d]\n", err.code, err.message, err.file, err.line);
	}else{	
		str_result.clear();
		_DecodeRecord(p_rec, str_result);
		printf("aerospike_key_get result:%s\n", str_result.c_str());
	}
	as_record_destroy(p_rec);
	
	//Reading the Specific Bins in a Record
	static const char* bins_1_3[] = { "test-bin-1", "test-bin-3", NULL };

	as_record* p_rec_slt = NULL;
	if (aerospike_key_select(&m_asContext, &err, NULL, &key, bins_1_3, &p_rec_slt) != AEROSPIKE_OK) {
		fprintf(stderr, "err(%d) %s at [%s:%d]\n", err.code, err.message, err.file, err.line);
	}else{
		str_result.clear();
		_DecodeRecord(p_rec_slt, str_result);
		printf("aerospike_key_select result:%s\n", str_result.c_str());
	}
	as_record_destroy(p_rec_slt);
	return;
}

void AeroClient::_DecodeRecord(const as_record* p_rec, std::string &str_result) {
	if (p_rec->key.valuep) {
		char* key_val_as_str = as_val_tostring(p_rec->key.valuep);
		free(key_val_as_str);
	}
	uint16_t num_bins = as_record_numbins(p_rec);
	as_record_iterator it;
	as_record_iterator_init(&it, p_rec);
	std::string str_val;
	while (as_record_iterator_has_next(&it)) {
		str_val.clear();
		if (_GetValByBin(as_record_iterator_next(&it), str_val)) {
			if (str_result.empty())
				str_result = str_val;
			else
				str_result += "," + str_val;
		}
	}
	as_record_iterator_destroy(&it);
}

void AeroClient::_DecodeRecordWithBin(const as_record* p_rec, std::string &str_result) {
	if (p_rec->key.valuep) {
		char* key_val_as_str = as_val_tostring(p_rec->key.valuep);
		free(key_val_as_str);
	}
	uint16_t num_bins = as_record_numbins(p_rec);
	as_record_iterator it;
	as_record_iterator_init(&it, p_rec);
	std::string str_val;
	while (as_record_iterator_has_next(&it)) {
		as_bin* p_bin = as_record_iterator_next(&it);
		if (_GetValByBin(p_bin, str_val)) {
			str_val = std::string(p_bin->name)+ "|"+ str_val;
			if (str_result.empty())
				str_result = str_val;
			else
				str_result += ";" + str_val;
		}
		str_val.clear();
	}
	as_record_iterator_destroy(&it);
}

bool AeroClient::_GetValByBin(const as_bin* p_bin, std::string &str_val) {
	if (!p_bin)
		return false;

	char* val_as_str = NULL;
	val_as_str = as_val_tostring(as_bin_get_value(p_bin));
	str_val = std::string(val_as_str);
	free(val_as_str);
	_StrTrim(str_val);
	return !str_val.empty();
}


