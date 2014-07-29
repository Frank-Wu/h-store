#ifndef __HYPERSTORE_HYPERVISOR_H__
#define __HYPERSTORE_HYPERVISOR_H__

#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mman.h>

class Hypervisor{
public:
	Hypervisor(){}
	void printHello();
	void* myAlloc(int tenantId, size_t size){
		int fd=shm_open("shm", O_RDWR, (mode_t)0600);
		if(tenantId==10){
			return (char*)mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 2*4096);
		}else{
			return (char*)mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, tenantId*4096);
		}
	}
};

#endif
