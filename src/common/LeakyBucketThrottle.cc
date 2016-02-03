// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2015 XSky <haomai@xsky.com>
 *
 * Author: Haomai Wang <haomai@xsky.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "common/Clock.h"
#include "LeakyBucketThrottle.h"

#define dout_subsys ceph_subsys_throttle
#undef dout_prefix
#define dout_prefix *_dout << "LeakyBucketThrottle::"

LeakyBucketThrottle::LeakyBucketThrottle(CephContext *c, uint64_t op_size)
  : cct(c), op_size(op_size), lock("LeakyBucketThrottle::lock"),
    timer(c, lock, true), enable(false), mode(THROTTLE_MODE_NONE),
    client_bw_threshold(0), client_iops_threshold(0),
    avg_is_max(false), avg_reset(false)
{
  timer_cb[0] = timer_cb[1] = NULL;
  timer_wait[0] = timer_wait[1] = false;
  timer.init();
}

LeakyBucketThrottle::~LeakyBucketThrottle()
{
  {
    Mutex::Locker l(lock);
    timer.shutdown();
  }
  delete timer_cb[0];
  delete timer_cb[1];
}

/* Add timers to event loop */
void LeakyBucketThrottle::attach_context(Context *reader, Context *writer)
{
  delete timer_cb[0];
  delete timer_cb[1];
  timer_cb[0] = reader;
  timer_cb[1] = writer;
}

/* Does any throttling must be done
 *
 * @ret: true if throttling must be done else false
 */
bool LeakyBucketThrottle::throttle_enabling()
{
  if (mode == THROTTLE_MODE_NONE) {
    return false;
  } else if (mode == THROTTLE_MODE_STATIC) {
    for (map<BucketType, LeakyBucket>::const_iterator it = buckets.begin();
         it != buckets.end(); ++it) {
      if (it->second.avg > 0) {
        return true;
      }
    }
  } else if (mode == THROTTLE_MODE_DYNAMIC) {
    // enalbe the throttle only when client threshold, lower limit and upper
    // limit are all set.
    if (client_bw_threshold > 0) {
      if (buckets.count(THROTTLE_BPS_TOTAL) &&
	  buckets[THROTTLE_BPS_TOTAL].min > 0 &&
	  buckets[THROTTLE_BPS_TOTAL].max > 0)
	return true;
    }
    if (client_iops_threshold > 0) {
      if (buckets.count(THROTTLE_OPS_TOTAL) &&
	  buckets[THROTTLE_OPS_TOTAL].min > 0 &&
	  buckets[THROTTLE_OPS_TOTAL].max > 0)
	return true;
    }
  }

  return false;
}

/* return true if any two throttling parameters conflicts
 *
 * @ret: true if any conflict detected else false
 */
static bool throttle_conflicting(map<BucketType, LeakyBucket> &buckets)
{
    bool bps_flag = false, ops_flag = false;
    bool bps_max_flag = false, ops_max_flag = false;

    if (buckets.count(THROTTLE_BPS_TOTAL)) {
      if (buckets.count(THROTTLE_BPS_READ)) {
	bps_flag |= (buckets[THROTTLE_BPS_TOTAL].avg &&
	             buckets[THROTTLE_BPS_READ].avg);
	bps_max_flag |= buckets[THROTTLE_BPS_TOTAL].max &&
	                buckets[THROTTLE_BPS_READ].max;
      }
      if (buckets.count(THROTTLE_BPS_WRITE)) {
	bps_flag |= buckets[THROTTLE_BPS_TOTAL].avg &&
	            buckets[THROTTLE_BPS_WRITE].avg;
	bps_max_flag |= buckets[THROTTLE_BPS_TOTAL].max &&
	                buckets[THROTTLE_BPS_WRITE].max;
      }
    }

    if (buckets.count(THROTTLE_OPS_TOTAL)) {
      if (buckets.count(THROTTLE_OPS_READ)) {
	ops_flag |= buckets[THROTTLE_OPS_TOTAL].avg &&
	            buckets[THROTTLE_OPS_READ].avg;
	ops_max_flag |= buckets[THROTTLE_OPS_TOTAL].max &&
	                buckets[THROTTLE_OPS_READ].max;
      }
      if (buckets.count(THROTTLE_OPS_WRITE)) {
	ops_flag |= buckets[THROTTLE_OPS_TOTAL].avg &&
	            buckets[THROTTLE_OPS_WRITE].avg;
	ops_max_flag |= buckets[THROTTLE_OPS_TOTAL].max &&
	                buckets[THROTTLE_OPS_WRITE].max;
      }
    }

    return bps_flag || ops_flag || bps_max_flag || ops_max_flag;
}

/* Used to configure the throttle
 *
 * @type: the throttle type we are working on
 * @avg: the config to set
 * @max: the config to set
 * @ret: true if any conflict detected else false
 */
bool LeakyBucketThrottle::config(BucketType type, double avg, double max)
{
  if (avg < 0 || max < 0)
    return false;
  Mutex::Locker l(lock);
  map<BucketType, LeakyBucket> local(buckets);
  if (avg)
    local[type].avg = local[type].min = avg;
  if (max)
    local[type].max = max;
  if (throttle_conflicting(local)) {
    ldout(cct, 0) << "Leaky bucket throttle configs conflict, aborting" << dendl;
    return false;
  }

  buckets[type].min = local[type].min;
  buckets[type].avg = local[type].avg;
  // Ensure max value isn't zero if avg not zero
  buckets[type].max = MAX(local[type].avg, local[type].max);
  enable = throttle_enabling();
  ldout(cct, 20) << "Leaky bucket throttle is " << (enable ? "enabled" : "NOT enabled") << dendl;
  avg_is_max = false;
  avg_reset = false;
  return true;
}

/* Set the throttling mode
 *
 * @_mode: the throttle mode to set. 0 - none, 1 - static, 2 - dynamic
 */
bool LeakyBucketThrottle::config_mode(int _mode)
{
  Mutex::Locker l(lock);

  if (_mode == 0) {
    mode = THROTTLE_MODE_NONE;
  } else if (_mode == 1) {
    mode = THROTTLE_MODE_STATIC;
  } else if (_mode == 2) {
    mode = THROTTLE_MODE_DYNAMIC;
  } else {
    ldout(cct, 0) << __func__ << " invalid mode " << _mode << ", aborting" << dendl;
    return false;
  }
  enable = throttle_enabling();
  ldout(cct, 20) << "Leaky bucket throttle is " << (enable ? "enabled" : "NOT enabled") << dendl;
  return true;
}

bool LeakyBucketThrottle::config_client_threshold(int64_t bw_threshold,
                                                  int64_t iops_threshold)
{
  Mutex::Locker l(lock);
  if (bw_threshold)
    client_bw_threshold = bw_threshold;
  if (iops_threshold)
    client_iops_threshold = iops_threshold;
  enable = throttle_enabling();
  ldout(cct, 20) << "Leaky bucket throttle is " << (enable ? "enabled" : "NOT enabled") << dendl;
  return true;
}

/* Schedule the read or write timer if needed
 *
 * NOTE: this function is not unit tested due to it's usage of timer_mod
 *
 * @is_write: the type of operation (read/write)
 * @ret:      true if the timer has been scheduled else false
 */
bool LeakyBucketThrottle::schedule_timer(bool is_write, bool release_timer_wait)
{
  if (release_timer_wait)
    timer_wait[is_write] = false;
  else
    lock.Lock();

  /* leak proportionally to the time elapsed */
  throttle_do_leak();

  /* compute the wait time if any */
  double wait = throttle_compute_wait_for(is_write);

  /* if the code must wait compute when the next timer should fire */
  if (!wait) {
    if (!release_timer_wait)
      lock.Unlock();
    return false;
  }

  /* request throttled and timer not pending -> arm timer */
  if (!timer_wait[is_write]) {
    assert(timer_cb[is_write]);
    timer.add_event_after(wait, timer_cb[is_write]);
    timer_wait[is_write] = true;
  }
  if (!release_timer_wait)
    lock.Unlock();
  return true;
}

/*
 * NOTE: no read/write differentiation version of schedule_timer
 *
 * @ret:      true if the timer has been scheduled else false
 */
bool LeakyBucketThrottle::schedule_timer(bool release_timer_wait)
{
  if (release_timer_wait)
    timer_wait[0] = false;
  else
    lock.Lock();

  /* leak proportionally to the time elapsed */
  throttle_do_leak();

  /* compute the wait time if any */
  double wait = throttle_compute_wait_for();

  /* if the code must wait compute when the next timer should fire */
  if (!wait) {
    if (!release_timer_wait)
      lock.Unlock();
    return false;
  }

  /* request throttled and timer not pending -> arm timer */
  ldout(cct, 20) << __func__ << " timer_wait is " << timer_wait[0]
                 << " add event after " << wait << " seconds" << dendl;
  if (!timer_wait[0]) {
    assert(timer_cb[0]);
    timer.add_event_after(wait, timer_cb[0]);
    timer_wait[0] = true;
  }
  if (!release_timer_wait)
    lock.Unlock();
  return true;
}

/* do the accounting for this operation
 *
 * @is_write: the type of operation (read/write)
 * @size:     the size of the operation
 */
void LeakyBucketThrottle::account(bool is_write, uint64_t size, bool lock_hold)
{
  if (!lock_hold)
    lock.Lock();
  double units = 1.0;

  /* if op_size is defined and smaller than size we compute unit count */
  if (op_size && size > op_size)
    units = (double) size / op_size;

  if (buckets.count(THROTTLE_BPS_TOTAL))
    buckets[THROTTLE_BPS_TOTAL].level += size;
  if (buckets.count(THROTTLE_OPS_TOTAL))
    buckets[THROTTLE_OPS_TOTAL].level += units;

  if (is_write) {
    if (buckets.count(THROTTLE_BPS_WRITE))
      buckets[THROTTLE_BPS_WRITE].level += size;
    if (buckets.count(THROTTLE_OPS_WRITE))
      buckets[THROTTLE_OPS_WRITE].level += units;
  } else {
    if (buckets.count(THROTTLE_BPS_READ))
      buckets[THROTTLE_BPS_READ].level += size;
    if (buckets.count(THROTTLE_OPS_READ))
      buckets[THROTTLE_OPS_READ].level += units;
  }
  if (!lock_hold)
    lock.Unlock();
}

/*
 * NOTE: no read/write differentiation verison of account
 *
 * @size:     the size of the operation
 */
void LeakyBucketThrottle::account(uint64_t size, bool lock_hold)
{
  if (!lock_hold)
    lock.Lock();
  double units = 1.0;

  /* if op_size is defined and smaller than size we compute unit count */
  if (op_size && size > op_size)
    units = (double) size / op_size;

  if (buckets.count(THROTTLE_BPS_TOTAL))
    buckets[THROTTLE_BPS_TOTAL].level += size;
  if (buckets.count(THROTTLE_OPS_TOTAL))
    buckets[THROTTLE_OPS_TOTAL].level += units;

  if (!lock_hold)
    lock.Unlock();
}

/* This function make a bucket leak
 *
 * @bkt:   the bucket to make leak
 * @delta_ns: the time delta
 */
void LeakyBucketThrottle::throttle_leak_bucket(LeakyBucket *bkt, uint64_t delta_ns)
{
  /* compute how much to leak */
  double leak = (bkt->avg * (double) delta_ns) / NANOSECONDS_PER_SECOND;
  /* make the bucket leak */
  bkt->level = MAX(bkt->level - leak, 0);
}

/* Calculate the time delta since last leak and make proportionals leaks
 *
 * @now:      the current timestamp in ns
 */
void LeakyBucketThrottle::throttle_do_leak()
{
  utime_t delta, now = ceph_clock_now(cct);
  /* compute the time elapsed since the last leak */
  if (now > previous_leak)
    delta = now - previous_leak;

  previous_leak = now;

  if (delta.is_zero()) {
    return ;
  }

  /* make each bucket leak */
  for (map<BucketType, LeakyBucket>::iterator it = buckets.begin();
       it != buckets.end(); ++it)
    throttle_leak_bucket(&it->second, delta.to_nsec());
}

/* This function compute the wait time in ns that a leaky bucket should trigger
 *
 * @bkt: the leaky bucket we operate on
 * @ret: the resulting wait time in seconds or 0 if the operation can go through
 */
double LeakyBucketThrottle::throttle_compute_wait(LeakyBucket *bkt, bool allow_burst)
{
  if (!bkt->avg)
    return 0;

  /* the number of extra units blocking the io */
  double extra = 0;
  if (mode == THROTTLE_MODE_STATIC && allow_burst)
    extra = bkt->level - bkt->max;
  else
    extra = bkt->level - bkt->avg;

  if (extra <= 0)
    return 0;

  return extra / bkt->avg;
}

/* This function compute the time that must be waited while this IO
 *
 * @is_write:   true if the current IO is a write, false if it's a read
 * @ret:        time to wait(seconds)
 */
double LeakyBucketThrottle::throttle_compute_wait_for(bool is_write)
{
  BucketType to_check[2][4] = { {THROTTLE_BPS_TOTAL,
                                 THROTTLE_OPS_TOTAL,
                                 THROTTLE_BPS_READ,
                                 THROTTLE_OPS_READ},
                                {THROTTLE_BPS_TOTAL,
                                 THROTTLE_OPS_TOTAL,
                                 THROTTLE_BPS_WRITE,
                                 THROTTLE_OPS_WRITE}, };
  double wait = 0, max_wait = 0;

  for (int i = 0; i < 4; i++) {
    BucketType index = to_check[is_write][i];
    if (buckets.count(index))
      wait = throttle_compute_wait(&buckets[index], true);

    if (wait > max_wait)
      max_wait = wait;
  }

  return max_wait;
}

/* no read/write differentiation version of throttle_compute_wait_for
 *
 * @ret:        time to wait(seconds)
 */
double LeakyBucketThrottle::throttle_compute_wait_for()
{
  BucketType to_check[2] = {THROTTLE_BPS_TOTAL, THROTTLE_OPS_TOTAL};
  double wait = 0, max_wait = 0;

  for (int i = 0; i < 2; i++) {
    BucketType index = to_check[i];
    if (buckets.count(index))
      wait = throttle_compute_wait(&buckets[index], false);

    if (wait > max_wait)
      max_wait = wait;
  }

  return max_wait;
}

/* adjust the level in the bucket
 *
 * @add_size:           the size to add
 * @substract_size:     the size to substract 
 */
void LeakyBucketThrottle::adjust(BucketType type, uint64_t add_size,
                                 uint64_t substract_size)
{
  assert(type == THROTTLE_BPS_TOTAL || type == THROTTLE_OPS_TOTAL);
  Mutex::Locker l(lock);
  if (buckets.count(type)) {
    buckets[type].level += add_size;
    buckets[type].level -= substract_size;
  }
}

/*
 * increase an unit on the bucket average for the dynamic mode
 */
void LeakyBucketThrottle::increase_bucket_average()
{
  Mutex::Locker l(lock);
  if (avg_is_max)
    return;

  avg_reset = false;
  uint32_t num_max = 0;
  for (map<BucketType, LeakyBucket>::iterator it = buckets.begin();
       it != buckets.end(); ++it) {
    if (it->first == THROTTLE_BPS_TOTAL) {
      double new_avg = it->second.avg + THROTTLE_BPS_INCREASE_UNIT;
      if (new_avg < it->second.max) {
        it->second.avg = new_avg;
      } else {
        it->second.avg = it->second.max;
	num_max++;
      }
      ldout(cct, 10) << __func__ << " increase throttle bw to " << it->second.avg << dendl;
    } else if (it->first == THROTTLE_OPS_TOTAL) {
      double new_avg = it->second.avg + THROTTLE_OPS_INCREASE_UNIT;
      if (new_avg <= it->second.max) {
        it->second.avg = new_avg;
      } else {
        it->second.avg = it->second.max;
	num_max++;
      }
      ldout(cct, 10) << __func__ << " increase throttle iops to " << it->second.avg << dendl;
    }
  }
  if (num_max == buckets.size())
    avg_is_max = true;

  return;
}

/*
 * reset the bucket average for the dynamic mode
 */
void LeakyBucketThrottle::reset_bucket_average()
{
  Mutex::Locker l(lock);
  if (avg_reset)
    return;

  avg_is_max = false;
  for (map<BucketType, LeakyBucket>::iterator it = buckets.begin();
       it != buckets.end(); ++it) {
    it->second.avg = it->second.min;
  }
  avg_reset = true;
  ldout(cct, 10) << __func__ << " reset throttle bw/iops to min" << dendl;
  return;
}
