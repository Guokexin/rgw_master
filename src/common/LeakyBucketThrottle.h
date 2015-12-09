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

#ifndef CEPH_LEAKYBUCKETTHROTTLE_H
#define CEPH_LEAKYBUCKETTHROTTLE_H

#include "include/utime.h"
#include "common/Timer.h"
#include "common/Mutex.h"

#define NANOSECONDS_PER_SECOND  1000000000.0

enum BucketType {
  THROTTLE_BPS_TOTAL,
  THROTTLE_BPS_READ,
  THROTTLE_BPS_WRITE,
  THROTTLE_OPS_TOTAL,
  THROTTLE_OPS_READ,
  THROTTLE_OPS_WRITE,
  BUCKETS_COUNT,
};

enum ThrottleMode {
  THROTTLE_MODE_NONE,
  THROTTLE_MODE_STATIC,
  THROTTLE_MODE_DYNAMIC,
  THROTTLE_MODE_MAX,
};

// For MODE_STATIC, avg is the average goal in units per second, max is the
// max burst in units, min is not used.
// For MODE_DYNAMIC, min is the lower limit, avg is the current limit, max is
// the upper limit. avg falls in [min, max].
struct LeakyBucket {
  double  min;              /* min goal in units per second */
  double  avg;              /* average goal in units per second */
  double  max;              /* leaky bucket max burst in units */
  double  level;            /* bucket level in units */
  LeakyBucket(): avg(0), max(0), level(0) {}
};

class LeakyBucketThrottle {
  /*
   * The max parameter of the leaky bucket throttling algorithm can be used to
   * allow the guest to do bursts.
   * The max value is a pool of I/O that the guest can use without being throttled
   * at all. Throttling is triggered once this pool is empty.
   */

  CephContext *cct;
  /* The following structure is used to configure a ThrottleState
   * It contains a bit of state: the bucket field of the LeakyBucket structure.
   * However it allows to keep the code clean and the bucket field is reset to
   * zero at the right time.
   */
  map<BucketType, LeakyBucket> buckets; /* leaky buckets */
  uint64_t op_size;         /* size of an operation in bytes */
  utime_t previous_leak;    /* timestamp of the last leak done */
  Mutex lock;
  SafeTimer timer;          /* timers used to do the throttling */
  /* 0 for read, 1 for write. 0 is used if no read/write differentiation */
  Context *timer_cb[2];
  bool timer_wait[2];
  bool enable;
  ThrottleMode mode;
  int64_t client_bw_threshold;
  int64_t client_iops_threshold;
  bool avg_is_max;
  bool avg_reset;

  static const int THROTTLE_BPS_INCREASE_UNIT = 1048576; // 1M
  static const int THROTTLE_OPS_INCREASE_UNIT = 1;

  void throttle_do_leak();
  double throttle_compute_wait_for(bool is_write);
  double throttle_compute_wait_for();

 public:
  LeakyBucketThrottle(CephContext *c, uint64_t op_size);
  ~LeakyBucketThrottle();

  static void throttle_leak_bucket(LeakyBucket *bkt, uint64_t delta_ns);
  double throttle_compute_wait(LeakyBucket *bkt, bool allow_burst);
  void attach_context(Context *reader, Context *writer=NULL);
  bool throttle_enabling();

  /* configuration */
  void set_op_size(uint64_t s) { op_size = s; }
  bool enabled() const { return enable; }
  bool config(BucketType type, double avg, double max);
  bool config_mode(int mode);
  bool config_client_threshold(int64_t bw_threshold, int64_t iops_threshold);
  void get_config(map<BucketType, LeakyBucket> &_buckets) { _buckets = buckets; }
  ThrottleMode get_mode() { return mode; }

  /* usage */
  bool schedule_timer(bool is_write, bool release_timer_wait);
  void account(bool is_write, uint64_t size, bool lock_hold=false);
  void adjust(BucketType type, uint64_t add_size, uint64_t substract_size);
  void increase_bucket_average();
  void reset_bucket_average();
  // no read/write differentiation version
  bool schedule_timer(bool release_timer_wait);
  void account(uint64_t size, bool lock_hold=false);
};

#endif
