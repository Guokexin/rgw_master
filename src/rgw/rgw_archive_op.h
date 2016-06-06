#ifndef _CEPH_RGW_ARCHIVE_TASK_H_
#define _CEPH_RGW_ARCHIVE_TASK_H_

#include <limits.h>

#include <string>
#include <set>
#include <map>

#include "rgw_common.h"
#include "rgw_rados.h"
#include "rgw_user.h"
#include "rgw_bucket.h"
#include "rgw_archive_task.h"


//小文件归档类
//2016.2.14
class RGWArchiveOp {
  public:
    string bucket_name;
    string obj_name;
    rgw_obj_key object;
    RGWBucketInfo bucket_info;
    rgw_obj  obj;
    RGWRados *store;
    RGWObjectCtx* obj_ctx;
    rgw_bucket bucket;
    uint64_t total_len;
    time_t lastmod;
    map<string,bufferlist> attrs; 
    uint64_t obj_size;
    off_t ofs;
    off_t end; 
    ArchiveTask* task;
    uint64_t item;
 		void* client_cb;
  //bucket_name
 public:
    RGWArchiveOp(): client_cb(NULL) {}
    virtual ~RGWArchiveOp();
		void set_op_result(bool result){task->worker->sfm_index[item].result = result;}
    virtual int pre_exec(RGWRados* store,std::string bucket_name,std::string obj_name);
    virtual int execute() { return 0;}
    virtual void complete() { } 
    virtual int get_complete_data_cb(list<bufferlist>& lbl, uint64_t size) ;
    virtual int get_data_cb(bufferlist& bl, off_t bl_ofs, off_t bl_len) {return 0;}
};

class RGWReadArchiveOp : public RGWArchiveOp{
   public:
    std::map <uint64_t, RGWSfmIndex*>  sfm_index;
		
    RGWReadArchiveOp(ArchiveTask* _task, uint64_t _item)
		{ 
			task = _task; item = _item; 
		} 
    ~RGWReadArchiveOp() { } 
    int execute();
};


class RGWDelArchiveOp : public RGWArchiveOp{
 
  public:
    RGWDelArchiveOp(ArchiveTask* _task, uint64_t _item) { task = _task; item = _item;} 
    ~RGWDelArchiveOp() {}
    int execute();
};



#endif
