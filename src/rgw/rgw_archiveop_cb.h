#ifndef _CEPH_RGW_ARCHIVEOP_CB_
#define _CEPH_RGW_ARCHIVEOP_CB_


#include "rgw_rados.h"
#include "rgw_archive_op.h"
class RGWArchiveOp_CB : public RGWGetDataCB
{
public:
  RGWArchiveOp *op;
public:
	RGWArchiveOp_CB() {}
  RGWArchiveOp_CB(RGWArchiveOp *_op) : op(_op) {}
  virtual ~RGWArchiveOp_CB() {}
  int handle_data(bufferlist& bl, off_t bl_ofs, off_t bl_len); 
  int handle_complete_data(list<bufferlist>& lbl, uint64_t size);

};

#endif
