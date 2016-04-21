// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */

#ifndef CEPH_XJOURNALINGOBJECTSTORE_H
#define CEPH_XJOURNALINGOBJECTSTORE_H

#include "os/ObjectStore.h"
#include "os/Journal.h"
#include "XJournal.h"
#include "common/RWLock.h"

class XJournalingObjectStore : public ObjectStore {
protected:
  XJournal *journal;
  Finisher finisher;


  class SubmitManager {
    Mutex lock;
    uint64_t op_seq;
    uint64_t op_submitted;
  public:
    SubmitManager() :
      lock("JOS::SubmitManager::lock", false, true, false, g_ceph_context),
      op_seq(0), op_submitted(0)
    {}
    uint64_t op_submit_start();
    void op_submit_finish(uint64_t op);
    void set_op_seq(uint64_t seq) {
      Mutex::Locker l(lock);
      op_submitted = op_seq = seq;
    }
    uint64_t get_op_seq() {
      return op_seq;
    }
  } submit_manager;

  class ApplyManager {
    XJournal *&journal;
    Finisher &finisher;

    Mutex apply_lock;
    int open_ops;
    uint64_t max_applied_seq;

    Mutex com_lock;
    map<version_t, vector<Context*> > commit_waiters;
    uint64_t committing_seq, committed_seq;
    set<uint64_t> unapply_seq;
    bool replaying;

  public:
    ApplyManager(XJournal *&j, Finisher &f) :
      journal(j), finisher(f),
      apply_lock("JOS::ApplyManager::apply_lock", false, true, false, g_ceph_context),
      open_ops(0),
      max_applied_seq(0),
      com_lock("JOS::ApplyManager::com_lock", false, true, false, g_ceph_context),
      committing_seq(0), committed_seq(0),
      replaying(false) {}
    void reset() {
      assert(open_ops == 0);
      max_applied_seq = 0;
      committing_seq = 0;
      committed_seq = 0;
    }
    void add_waiter(uint64_t, Context*);
    void op_apply_register(uint64_t op);
    uint64_t op_apply_start(uint64_t op);
    void op_apply_finish(uint64_t op);
    bool commit_start();
    void commit_started();
    void commit_finish();
    bool is_committing() {
      Mutex::Locker l(com_lock);
      return committing_seq != committed_seq;
    }
    uint64_t get_committed_seq() {
      Mutex::Locker l(com_lock);
      return committed_seq;
    }
    uint64_t get_committing_seq() {
      Mutex::Locker l(com_lock);
      return committing_seq;
    }
    void init_seq(uint64_t fs_op_seq) {
      {
	Mutex::Locker l(com_lock);
	committed_seq = fs_op_seq;
	committing_seq = fs_op_seq;
      }
      {
	Mutex::Locker l(apply_lock);
	max_applied_seq = fs_op_seq;
      }
    }
    void set_replaying(bool b) {
      replaying = b;
    }
  } apply_manager;

  bool replaying;

protected:
  void journal_start();
  void journal_stop();
  void journal_write_close();
  int journal_replay(uint64_t fs_op_seq);

  int get_ack_txns_seq(uint64_t fs_op_seq, list<uint64_t> &acks);

  void _op_journal_transactions(bufferlist& tls, uint32_t orig_len, uint64_t op,
				Context *onjournal, TrackedOpRef osd_op);

  virtual int do_transactions(list<ObjectStore::Transaction*>& tls, uint64_t op_seq) = 0;
  virtual void reset_object_size() = 0;

public:
  bool is_committing() {
    return apply_manager.is_committing();
  }
  uint64_t get_committed_seq() {
    return apply_manager.get_committed_seq();
  }

public:
  XJournalingObjectStore(const std::string& path)
    : ObjectStore(path),
      journal(NULL),
      finisher(g_ceph_context),
      apply_manager(journal, finisher),
      replaying(false) {}

  ~XJournalingObjectStore() {
  }
};

#endif
