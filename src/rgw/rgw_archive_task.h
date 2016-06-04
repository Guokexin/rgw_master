#ifndef _RGW_ARCHIVE_TASK_H_
#define _RGW_ARCHIVE_TASK_H_
#include "include/atomic.h"
#include "common/Mutex.h"
#include "common/Cond.h"
#include "common/Thread.h"
#include <semaphore.h>
#include "include/rados/librados.hpp"


class CephContext;
class RGWRados;
class RGWBgtWorker;
//set task queue size 
const uint32_t QUEUE_SIZE = 50;

typedef struct _obj_info{
  std::string obj_name;
  std::string bucket_name;
}obj_info;


class ArchiveTask : public Thread{
	friend class RGWArchiveOp;
  protected:
    CephContext* cct;
    Mutex task_archive_lock;
    Cond cond;
    bool  task_archive_stop;
    uint32_t queue_size;
    RGWRados *store;
    RGWBgtWorker *worker;
    list<obj_info*>  archive_queue;

    sem_t sem_full;

    int task_type;

    void* task_thread_entry();
  public:
    //std::map<std::string,std::map<uint64_t,obj_stripe_t*>> complete_queue;
    void start();

    void stop(); 

    void dispatch_task(int _type);

    ArchiveTask(CephContext* _cct,RGWRados* _store, RGWBgtWorker* _worker):
      cct(_cct),
      task_archive_lock("ArchiveTask::task_archive_task"),
      task_archive_stop(false),
      queue_size(0),store(_store), worker(_worker),task_type(-1){
        sem_init(&sem_full,0,QUEUE_SIZE);
      }

    void* entry(){
      return task_thread_entry();

    }
    void handle_complete_task(list<bufferlist>& lbl,uint64_t size,uint64_t item);

};


#endif
