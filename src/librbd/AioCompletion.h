// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#ifndef CEPH_LIBRBD_AIOCOMPLETION_H
#define CEPH_LIBRBD_AIOCOMPLETION_H

#include "common/Cond.h"
#include "common/Mutex.h"
#include "common/ceph_context.h"
#include "common/perf_counters.h"
#include "include/Context.h"
#include "include/utime.h"
#include "include/rbd/librbd.hpp"

#include "librbd/AsyncOperation.h"
#include "librbd/ImageCtx.h"
#include "librbd/internal.h"

#include "osdc/Striper.h"

namespace librbd {

  class AioRead;

  typedef enum {
    AIO_TYPE_READ = 0,
    AIO_TYPE_WRITE,
    AIO_TYPE_DISCARD,
    AIO_TYPE_FLUSH,
    AIO_TYPE_NONE,
  } aio_type_t;

  /**
   * AioCompletion is the overall completion for a single
   * rbd I/O request. It may be composed of many AioRequests,
   * which each go to a single object.
   *
   * The retrying of individual requests is handled at a lower level,
   * so all AioCompletion cares about is the count of outstanding
   * requests. Note that this starts at 1 to prevent the reference
   * count from reaching 0 while more requests are being added. When
   * all requests have been added, finish_adding_requests() releases
   * this initial reference.
   */
  struct AioCompletion {
    Mutex lock;
    Cond cond;
    bool done;
    ssize_t rval;
    callback_t complete_cb;
    void *complete_arg;
    rbd_completion_t rbd_comp;
    int pending_count;   ///< number of requests
    uint32_t blockers;
    int ref;
    bool released;
    ImageCtx *ictx;
    utime_t start_time;
    aio_type_t aio_type;

    Striper::StripedReadResult destriper;
    bufferlist *read_bl;
    char *read_buf;
    size_t read_buf_len;

    AsyncOperation async_op;

    xlist<AioCompletion*>::item m_xlist_item;
    bool event_notify;

    AioCompletion() : lock("AioCompletion::lock", true, false),
		      done(false), rval(0), complete_cb(NULL),
		      complete_arg(NULL), rbd_comp(NULL),
		      pending_count(0), blockers(1),
		      ref(1), released(false), ictx(NULL),
		      aio_type(AIO_TYPE_NONE),
		      read_bl(NULL), read_buf(NULL), read_buf_len(0),
                      m_xlist_item(this), event_notify(false) {
    }
    ~AioCompletion() {
    }

    void reset() {
      Mutex::Locker l(lock);
      if (aio_type == AIO_TYPE_NONE) {
        assert(!released && pending_count == 0 && blockers == 1 && !done);
        return ;
      }
      assert(!released && pending_count == 0 && blockers == 0 && done);
      done = false;
      rval = 0;
      ictx = NULL;
      aio_type = AIO_TYPE_NONE;
      destriper.reset();
      read_bl = NULL;
      read_buf = NULL;
      read_buf_len = 0;
      event_notify = false;
      async_op.reset();
      blockers = 1;
    }

    int wait_for_complete();

    void add_request() {
      lock.Lock();
      pending_count++;
      lock.Unlock();
      get();
    }

    void finalize(CephContext *cct, ssize_t rval);

    void finish_adding_requests(CephContext *cct);

    void init_time(ImageCtx *i, aio_type_t t) {
      if (ictx == NULL) {
        ictx = i;
        aio_type = t;
        start_time = ceph_clock_now(ictx->cct);

	async_op.start_op(*ictx);
      }
    }

    void fail(CephContext *cct, int r);

    void complete(CephContext *cct);

    void set_complete_cb(void *cb_arg, callback_t cb) {
      complete_cb = cb;
      complete_arg = cb_arg;
    }

    void complete_request(CephContext *cct, ssize_t r);

    bool is_complete();

    ssize_t get_return_value();

    void get() {
      lock.Lock();
      assert(ref > 0);
      ref++;
      lock.Unlock();
    }
    void release() {
      lock.Lock();
      assert(!released);
      released = true;
      put_unlock();
    }
    void put() {
      lock.Lock();
      put_unlock();
    }
    void put_unlock() {
      assert(ref > 0);
      int n = --ref;
      lock.Unlock();
      if (!n) {
        if (ictx && event_notify) {
          ictx->completed_reqs_lock.Lock();
          m_xlist_item.remove_myself();
          ictx->completed_reqs_lock.Unlock();
        }
        delete this;
      }
    }

    void block() {
      Mutex::Locker l(lock);
      ++blockers;
    }
    void unblock(CephContext *cct) {
      Mutex::Locker l(lock);
      assert(blockers > 0);
      --blockers;
      if (pending_count == 0 && blockers == 0) {
        finalize(cct, rval);
        complete(cct);
      }
    }

    void set_event_notify(bool s) {
      Mutex::Locker l(lock);
      event_notify = s;
    }

    void *get_arg() {
      return complete_arg;
    }
  };

  class C_AioRead : public Context {
  public:
    C_AioRead(CephContext *cct, AioCompletion *completion)
      : m_cct(cct), m_completion(completion), m_req(NULL)
    { }
    virtual ~C_AioRead() {}
    virtual void finish(int r);
    void set_req(AioRead *req) {
      m_req = req;
    }
  private:
    CephContext *m_cct;
    AioCompletion *m_completion;
    AioRead *m_req;
  };

  class C_AioWrite : public Context {
  public:
    C_AioWrite(CephContext *cct, AioCompletion *completion)
      : m_cct(cct), m_completion(completion) {}
    virtual ~C_AioWrite() {}
    virtual void finish(int r) {
      m_completion->complete_request(m_cct, r);
    }
  private:
    CephContext *m_cct;
    AioCompletion *m_completion;
  };

  class C_CacheRead : public Context {
  public:
    explicit C_CacheRead(ImageCtx *ictx, AioRead *req)
      : m_image_ctx(*ictx), m_req(req), m_enqueued(false) {}
    virtual void complete(int r);
  protected:
    virtual void finish(int r);
  private:
    ImageCtx &m_image_ctx;
    AioRead *m_req;
    bool m_enqueued;
  };
}

#endif
