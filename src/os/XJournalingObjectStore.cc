// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-

#include "XJournalingObjectStore.h"

#include "common/errno.h"
#include "common/debug.h"

#define dout_subsys ceph_subsys_xjournal
#undef dout_prefix
#define dout_prefix *_dout << "xjournal "



void XJournalingObjectStore::journal_start()
{
  dout(10) << "journal_start" << dendl;
  finisher.start();
}
 
void XJournalingObjectStore::journal_stop() 
{
  dout(10) << "journal_stop" << dendl;
  finisher.stop();
}

// A journal_replay() makes journal writeable, this closes that out.
void XJournalingObjectStore::journal_write_close()
{
  if (journal) {
    journal->close();
    delete journal;
    journal = 0;
  }
  apply_manager.reset();
}

int XJournalingObjectStore::get_ack_txns_seq(uint64_t fs_op_seq, list<uint64_t> &acks)
{
  dout(10) << "get_ack_txns_seq fs op_seq " << fs_op_seq << dendl;

  uint64_t op_seq = fs_op_seq;
  apply_manager.init_seq(fs_op_seq);

  assert (journal);

  int err = journal->open(op_seq);
  if (err < 0) {
    dout(3) << "get_ack_txns_seq open failed with "
	    << cpp_strerror(err) << dendl;
    delete journal;
    journal = 0;
    return err;
  }

  replaying = true;

  while (1) {
    bufferlist bl;
    uint64_t seq = op_seq + 1;
    bool is_ack = false;
    if (!journal->read_entry(bl, seq, 0, &is_ack)) {
      dout(3) << "get_ack_txns_seq: end of journal, done." << dendl;
      break;
    }

    if (seq <= op_seq) {
      dout(3) << "get_ack_txns_seq: skipping old op seq " << seq << " <= " << op_seq << dendl;
      continue;
    }
    assert(op_seq == seq-1);
   
    if (is_ack)  {
      dout(3) << "get_ack_txns_seq: " << seq << dendl;
      list<uint64_t> l;
      bufferlist::iterator p = bl.begin();
      ::decode(l, p);
      ostringstream oss;
      for (list<uint64_t>::iterator it = l.begin(); it != l.end(); ++it) {
        oss << *it << " ";
      }
      dout(3) << "get_ack_txns_seq: " << oss.str() << dendl;
      acks.splice(acks.end(), l);
    }

    op_seq = seq;

    dout(3) << "get_ack_txns_seq: op_seq now " << op_seq << dendl;
  }

  return 0;
}

int XJournalingObjectStore::journal_replay(uint64_t fs_op_seq)
{
  dout(10) << "journal_replay fs op_seq " << fs_op_seq << dendl;

  if (g_conf->journal_replay_from) {
    dout(0) << "journal_replay forcing replay from " << g_conf->journal_replay_from
	    << " instead of " << fs_op_seq << dendl;
    // the previous op is the last one committed
    fs_op_seq = g_conf->journal_replay_from - 1;
  }

  list<uint64_t> acks;
  int err  = get_ack_txns_seq(fs_op_seq, acks);
  if (err < 0) {
    return err;
  }

  uint64_t op_seq = fs_op_seq;
  apply_manager.init_seq(fs_op_seq);

  assert (journal);

  err = journal->open(op_seq);
  if (err < 0) {
    dout(3) << "journal_replay open failed with " 
	    << cpp_strerror(err) << dendl;
    delete journal;
    journal = 0;
    return err;
  }

  int count = 0;
  while (1) {
    bufferlist bl;
    uint64_t seq = op_seq + 1;
    bool is_ack = false;
    if (!journal->read_entry(bl, seq, 0, &is_ack)) {
      dout(3) << "journal_replay: end of journal, done." << dendl;
      break;
    }

    if (seq <= op_seq) {
      dout(3) << "journal_replay: skipping old op seq " << seq << " <= " << op_seq << dendl;
      continue;
    }
    assert(op_seq == seq-1);
    
    if (!is_ack &&
        std::find(acks.begin(), acks.end(), seq) != acks.end()) {
      dout(3) << "journal_replay: applying op seq " << seq << dendl;
      bufferlist::iterator p = bl.begin();
      list<Transaction*> tls;
      while (!p.end()) {
        Transaction *t = new Transaction(p);
        tls.push_back(t);
      }

      apply_manager.op_apply_start(seq);
      int r = do_transactions(tls, seq);
      apply_manager.op_apply_finish(seq);

      while (!tls.empty()) {
        delete tls.front();
        tls.pop_front();
      }

      dout(3) << "journal_replay: r = " << r << ", op_seq now " << op_seq << dendl;
    } else {
      dout(3) << "journal_replay: skip op seq " << seq << " ack item " << is_ack << dendl;
    }

    op_seq = seq;
  }

  replaying = false;

  submit_manager.set_op_seq(op_seq);

  // done reading, make writeable.
  err = journal->make_writeable();
  if (err < 0)
    return err;

  return count;
}


// ------------------------------------

uint64_t XJournalingObjectStore::ApplyManager::op_apply_start(uint64_t op)
{
  Mutex::Locker l(apply_lock);
  while (blocked) {
    // note: this only happens during journal replay
    dout(10) << "op_apply_start blocked, waiting" << dendl;
    blocked_cond.Wait(apply_lock);
  }
  dout(10) << "op_apply_start " << op << " open_ops " << open_ops << " -> " << (open_ops+1) << dendl;
  assert(!blocked);
  assert(op > committed_seq);
  open_ops++;
  return op;
}

void XJournalingObjectStore::ApplyManager::op_apply_finish(uint64_t op)
{
  Mutex::Locker l(apply_lock);
  dout(10) << "op_apply_finish " << op << " open_ops " << open_ops
	   << " -> " << (open_ops-1)
	   << ", max_applied_seq " << max_applied_seq << " -> " << MAX(op, max_applied_seq)
	   << dendl;
  --open_ops;
  assert(open_ops >= 0);

  // signal a blocked commit_start (only needed during journal replay)
  if (blocked) {
    blocked_cond.Signal();
  }

  // there can be multiple applies in flight; track the max value we
  // note.  note that we can't _read_ this value and learn anything
  // meaningful unless/until we've quiesced all in-flight applies.
  if (op > max_applied_seq)
    max_applied_seq = op;
}

uint64_t XJournalingObjectStore::SubmitManager::op_submit_start()
{
  lock.Lock();
  uint64_t op = ++op_seq;
  dout(10) << "op_submit_start " << op << dendl;
  return op;
}

void XJournalingObjectStore::SubmitManager::op_submit_finish(uint64_t op)
{
  dout(10) << "op_submit_finish " << op << dendl;
  if (op != op_submitted + 1) {
    dout(0) << "op_submit_finish " << op << " expected " << (op_submitted + 1)
	    << ", OUT OF ORDER" << dendl;
    assert(0 == "out of order op_submit_finish");
  }
  op_submitted = op;
  lock.Unlock();
}


// ------------------------------------------

void XJournalingObjectStore::ApplyManager::add_waiter(uint64_t op, Context *c)
{
  Mutex::Locker l(com_lock);
  assert(c);
  commit_waiters[op].push_back(c);
}

bool XJournalingObjectStore::ApplyManager::commit_start()
{
  bool ret = false;

  uint64_t _committing_seq = 0;
  {
    Mutex::Locker l(apply_lock);
    dout(10) << "commit_start max_applied_seq " << max_applied_seq
	     << ", open_ops " << open_ops
	     << dendl;
    blocked = true;
    while (open_ops > 0) {
      dout(10) << "commit_start waiting for " << open_ops << " open ops to drain" << dendl;
      blocked_cond.Wait(apply_lock);
    }
    assert(open_ops == 0);
    dout(10) << "commit_start blocked, all open_ops have completed" << dendl;
    {
      Mutex::Locker l(com_lock);
      if (max_applied_seq == committed_seq) {
	dout(10) << "commit_start nothing to do" << dendl;
	blocked = false;
	assert(commit_waiters.empty());
	goto out;
      }

      _committing_seq = committing_seq = max_applied_seq;

      dout(10) << "commit_start committing " << committing_seq
	       << ", still blocked" << dendl;
    }
  }
  ret = true;

 out:
  journal->commit_start(_committing_seq);  // tell the journal too
  return ret;
}

void XJournalingObjectStore::ApplyManager::commit_started()
{
  Mutex::Locker l(apply_lock);
  // allow new ops. (underlying fs should now be committing all prior ops)
  dout(10) << "commit_started committing " << committing_seq << ", unblocking" << dendl;
  blocked = false;
  blocked_cond.Signal();
}

void XJournalingObjectStore::ApplyManager::commit_finish()
{
  Mutex::Locker l(com_lock);
  dout(10) << "commit_finish thru " << committing_seq << dendl;
  
  assert (journal);
  journal->committed_thru(committing_seq);

  committed_seq = committing_seq;
  
  map<version_t, vector<Context*> >::iterator p = commit_waiters.begin();
  while (p != commit_waiters.end() &&
    p->first <= committing_seq) {
    finisher.queue(p->second);
    commit_waiters.erase(p++);
  }
}

void XJournalingObjectStore::_op_journal_transactions(
  bufferlist& tbl, uint32_t orig_len, uint64_t op,
  Context *onjournal, TrackedOpRef osd_op)
{
  if (osd_op.get())
    dout(10) << "op_journal_transactions " << op << " reqid_t "
             << (static_cast<OpRequest *>(osd_op.get()))->get_reqid() << dendl;
  else
    dout(10) << "op_journal_transactions " << op  << dendl;

  if (journal && journal->is_writeable()) {
    journal->submit_entry(op, tbl, orig_len, onjournal, osd_op);
  } else {
    assert(0 == "Unexpected IO PATH");
  }
}

