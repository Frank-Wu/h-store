#ifndef __HYPERSTORE_HYPERVISOR_H__
#define __HYPERSTORE_HYPERVISOR_H__

#include <fcntl.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mman.h>

class Hypervisor{
public:
	Hypervisor(){}
	void printHello();
	static void* myAlloc(size_t size){
		int fd=shm_open("shm", O_RDWR, (mode_t)0600);
		int tenantId=0;
		return (char*)mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, tenantId*0);
	}
};

#endif
