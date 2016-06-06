#include "rgw_archiveop_cb.h"


 int RGWArchiveOp_CB::handle_data(bufferlist& bl, off_t bl_ofs, off_t bl_len) {

    return op->get_data_cb(bl, bl_ofs, bl_len);
 }
 
  int RGWArchiveOp_CB::handle_complete_data(list<bufferlist>& lbl, uint64_t size){
    int r = op->get_complete_data_cb(lbl,size);
    op->client_cb = NULL;
    delete op;

    delete this;

    return r;
  } 


