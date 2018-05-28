#include "aeroclient.hpp"
#include <stdio.h>
#include <string>
#include <memory>

int main()
{
	printf("Aeroclient begin.\n");
	int num = 1;
	int readtime = 100;
	std::string str_hosts = "127.0.0.1";
	std::string str_error;
	std::shared_ptr<AeroClient> p_service(new AeroClient(num, readtime, 3000, str_hosts));
	if(!p_service->Connect(str_error)){
		printf("init AeroService failed, hosts=%s, error=%s\n", str_hosts.c_str(), str_error.c_str());
		return 0;
	}
	printf("init AeroService ok, hosts=%s\n", str_hosts.c_str());
	p_service->WrietRecords();
	p_service->ReadRecords();
	std::string str_key = "test-key";
	std::string str_ns = "test";
	std::string str_set = "test-set";
	std::string str_result;
	std::string str_reason;
	bool b_ret = p_service->GetValMultiSet(str_ns, str_set, str_key, str_result, str_reason);
	if(!b_ret){
		printf("GetValMultiSet error:%s\n", str_reason.c_str());
	}else{
		printf("GetValMultiSet result:%s\n", str_result.c_str());
	}
	printf("Aeroclient end.\n");
	return 0;
}