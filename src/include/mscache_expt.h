/*
  * Modules name : Multistage Cache
  * Filename : readcache_expt.h
  * Author : Derekyu
  * Date : 2015-11-30
  */

#ifndef _MSCACHE_EXPT_H_
#define _MSCACHE_EXPT_H_

#include <memory>
#include <string>
#include <tr1/memory>
#include "os/FDCache.h"

enum {
    MSCACHE_WRITE_API_SYNC = 0xb1,
    MSCACHE_WRITE_API_ASYNC,    
};

enum {
    MSCACHE_WRITE_HINT_DATA = 0xa0,
    MSCACHE_WRITE_HINT_METADATA,
    MSCACHE_WRITE_HINT_TRUNCATE,
};



extern int mscache_read(FDRef &fdref, const char* obj_info, void* buff, size_t length, off_t offset, int pref_flag);

extern int mscache_write(FDRef &fdref, const char* obj_info, const void* buff, size_t length, off_t offset, void* arg, int async_flag, int write_hint, int truncate_length);

extern int mscache_modules_init(void);

extern void mscache_modules_exit(void);


#endif /* _MSCACHE_EXPT_H_ */
