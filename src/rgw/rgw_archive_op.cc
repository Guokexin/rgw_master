#include "rgw_archive_op.h"
#include <sstream>
#include <semaphore.h>
#include "rgw_archiveop_cb.h"
#define dout_subsys ceph_subsys_rgw

//pre_exec op
int RGWArchiveOp::pre_exec(RGWRados* store,std::string _bucket_name, std::string _obj_name){
  if( store == NULL )
    return -1;

  ldout(store->ctx(), 10) << "op pre_exec:" << _bucket_name << ", " << _obj_name << dendl;
  
  bucket_name = _bucket_name;
  obj_name = _obj_name;
  object = rgw_obj_key(obj_name,"");
  this->store = store;
  obj_ctx = new RGWObjectCtx(store);
  
  RGWBucketInfo bucket_info;
  map<string, bufferlist> bucket_attrs;
  //RGWObjectCtx obj_ctx(store);

  int ret = store->get_bucket_info(*obj_ctx, bucket_name, bucket_info, NULL, &bucket_attrs);
  if(ret < 0) {
    ldout(store->ctx(), 0) << "=== fail to get_bucket_info ====" <<dendl;
    return -1;
  }
  bucket = bucket_info.bucket;
  obj =  rgw_obj(bucket, object);
  obj_ctx->objs_state[obj].prefetch_data = (1 == task->task_type) ? true : false;
  return 0; 
}
//

RGWArchiveOp::~RGWArchiveOp()
{
  if (NULL != client_cb)
  {
    RGWArchiveOp_CB* cb = (RGWArchiveOp_CB*)client_cb;
    delete cb;
    client_cb = NULL;
  }
}

//
int RGWReadArchiveOp::execute(){

  int64_t new_ofs, new_end;
  RGWArchiveOp_CB* cb = new RGWArchiveOp_CB(this);
  if (NULL == cb)
  {
    ldout(store->ctx(), 0) << "allocate RGWArchiveOp_CB failed"<< dendl;
    return -1;
  }
  this->client_cb = (void*)cb;
  cb->set_op_type(1);
  map<string, bufferlist>::iterator attr_iter;  

  new_ofs = 0;
  new_end = -1;

  RGWRados::Object op_target(store, bucket_info, *static_cast<RGWObjectCtx *>(obj_ctx), obj);
  RGWRados::Object::Read read_op(&op_target,1);

  read_op.params.attrs = &attrs;
  read_op.params.lastmod = &lastmod;
  read_op.params.read_size = &total_len;
  read_op.params.obj_size = &obj_size;

  int ret = read_op.prepare(&new_ofs,&new_end);
  if(ret < 0)
  {
    ldout(store->ctx(), 0) << "op prepare failed: "<< ret << dendl;
    return -1;
  }
  
  ldout(store->ctx(), 10) << "new_ofs = "<<new_ofs<<" new_end="<<new_end<<dendl;

  ofs = new_ofs;
  end = new_end;

  
  ret = read_op.iterate_archive(ofs, end, cb);
  if(ret < 0){
    return -1;
  }
  return 0;
}

//
int RGWDelArchiveOp::execute(){

  int64_t new_ofs, new_end;
  RGWArchiveOp_CB* cb = new RGWArchiveOp_CB(this);
  if (NULL == cb)
  {
    ldout(store->ctx(), 0) << "allocate RGWArchiveOp_CB failed"<< dendl;
    return -1;
  }
  this->client_cb = (void*)cb;
  cb->set_op_type(2);
  map<string, bufferlist>::iterator attr_iter;  

  new_ofs = 0;
  new_end = -1;

  RGWRados::Object op_target(store, bucket_info, *static_cast<RGWObjectCtx *>(obj_ctx), obj);
  RGWRados::Object::Read read_op(&op_target,1);

  read_op.params.attrs = &attrs;
  read_op.params.lastmod = &lastmod;
  read_op.params.read_size = &total_len;
  read_op.params.obj_size = &obj_size;

  int ret = read_op.prepare(&new_ofs,&new_end);
  if(ret < 0)
  {
    ldout(store->ctx(), 0) << "op prepare failed: "<< ret << dendl;
    return -1;
  }
  
  ldout(store->ctx(), 10) << "new_ofs = "<<new_ofs<<" new_end="<<new_end<<dendl;

  ofs = new_ofs;
  end = new_end;

  
  ret = read_op.iterate_archive(ofs, end, cb);
  if(ret < 0){
    return -1;
  }
  return 0;
}


int RGWArchiveOp::get_complete_data_cb(list<bufferlist>& lbl,uint64_t size){
  
  ldout(store->ctx(), 10) << "=== read complete ===" << size <<dendl; 
  task->handle_complete_task(lbl,size,item);
  if(obj_ctx!=NULL)
    delete obj_ctx;
  return 0;
}




