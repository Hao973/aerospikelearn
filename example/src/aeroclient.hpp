#ifndef _AERO_CLIENT_H_
#define _AERO_CLIENT_H_

#include <string>
#include <vector>
#include <aerospike/aerospike.h>
#include <aerospike/aerospike_key.h>
#include <aerospike/as_record_iterator.h>

// 动态优先级信息
class AeroClient {
public:
    AeroClient(int connnum, int timeout, int port, const std::string &hosts);

    ~AeroClient();

    bool Connect(std::string &str_reason);

    bool Close(std::string &str_reason);

    bool GetValMultiSet(const std::string& str_namespace, 
                        const std::string &str_set, 
                        const std::string str_key, 
                        std::string &str_result, 
                        std::string &str_reason);

    bool GetValBin(const std::string& str_namespace, 
                   const std::string &str_set,
                   const std::string &str_bin, 
                   const std::string str_key, 
                   std::string &str_result, 
                   std::string &str_reason);

    bool GetValMultiBin(const std::string& str_namespace, 
                   const std::string &str_set,
                   std::vector<std::string> &vec_bin, 
                   const std::string str_key, 
                   std::string &str_result, 
                   std::string &str_reason);

     bool PutValBin(const std::string& str_namespace, 
                    const std::string &str_set,
                    const std::string &str_bin, 
                    const std::string str_key,
                    std::string &str_val,
                    std::string &str_reason,
                    int n_ttl = 0);

    std::string _GetHosts() {
        return m_strHosts;
    }
	//add test begin
	void WrietRecords();
	void ReadRecords();
	//add test end
    
private:
    void _DecodeRecord(const as_record* p_rec, std::string &str_result);

    void _DecodeRecordWithBin(const as_record* p_rec, std::string &str_result);

    bool _GetValByBin(const as_bin* p_bin, std::string &str_val);

    void _StrTrim(std::string& str) {
        str.erase(0, str.find_first_not_of("\""));
        str.erase(str.find_last_not_of("\"") + 1);
        str.erase(0, str.find_first_not_of("\t"));
        str.erase(str.find_last_not_of("\t") + 1);
    }

private:
    aerospike       m_asContext;
    as_error        m_asError;
    std::string     m_strHosts;
};

#endif//_AERO_CLIENT_H_
