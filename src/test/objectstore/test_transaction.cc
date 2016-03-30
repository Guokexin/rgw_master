// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2016 Casey Bodley <cbodley@redhat.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "os/ObjectStore.h"
#include <gtest/gtest.h>
#include "common/Clock.h"
#include "include/utime.h"


ObjectStore::Transaction* generate_transaction()
{
  ObjectStore::Transaction* a = new ObjectStore::Transaction();
  a->set_use_tbl(false);
  a->nop();

  coll_t cid;
  object_t obj("test_name");
  snapid_t snap(0);
  hobject_t hoid(obj, "key", snap, 0, 0, "nspace");
  ghobject_t oid(hoid);

  coll_t acid;
  object_t aobj("another_test_name");
  snapid_t asnap(0);
  hobject_t ahoid(obj, "another_key", snap, 0, 0, "another_nspace");
  ghobject_t aoid(hoid);
  std::set<string> keys;
  keys.insert("any_1");
  keys.insert("any_2");
  keys.insert("any_3");

  bufferlist bl;
  bl.append_zero(4096);
  map<string, bufferlist> insertkeys;
  insertkeys.insert(make_pair("xxx", bl));

  a->write(cid, oid, 1, 4096, bl, 0);

  a->omap_setkeys(acid, aoid, insertkeys);

  a->omap_rmkeys(cid, aoid, keys);

  a->touch(acid, oid);

  return a;
}

TEST(Transaction, GetNumBytes)
{
  ObjectStore::Transaction* a = new ObjectStore::Transaction();
  a->set_use_tbl(false);
  a->nop();
  ASSERT_TRUE(a->get_encoded_bytes() == a->get_encoded_bytes_test());

  coll_t cid;
  object_t obj("test_name");
  snapid_t snap(0);
  hobject_t hoid(obj, "key", snap, 0, 0, "nspace");
  ghobject_t oid(hoid);

  coll_t acid;
  object_t aobj("another_test_name");
  snapid_t asnap(0);
  hobject_t ahoid(obj, "another_key", snap, 0, 0, "another_nspace");
  ghobject_t aoid(hoid);
  std::set<string> keys;
  keys.insert("any_1");
  keys.insert("any_2");
  keys.insert("any_3");

  bufferlist bl;
  bl.append_zero(4096);

  a->write(cid, oid, 1, 4096, bl, 0);
  ASSERT_TRUE(a->get_encoded_bytes() == a->get_encoded_bytes_test());

  map<string, bufferlist> insertkeys;
  insertkeys.insert(make_pair("xxx", bl));
  a->omap_setkeys(acid, aoid, insertkeys);
  ASSERT_TRUE(a->get_encoded_bytes() == a->get_encoded_bytes_test());

  a->omap_rmkeys(cid, aoid, keys);
  ASSERT_TRUE(a->get_encoded_bytes() == a->get_encoded_bytes_test());

  a->touch(acid, oid);
  ASSERT_TRUE(a->get_encoded_bytes() == a->get_encoded_bytes_test());

  delete a;
}

void bench_num_bytes(bool legacy)
{
  const int max = 2500000;
  ObjectStore::Transaction* a = generate_transaction();

  if (legacy) {
    cout << "get_encoded_bytes_test: ";
  } else {
    cout << "get_encoded_bytes: ";
  }

  utime_t start = ceph_clock_now(NULL);
  if (legacy) {
    for (int i = 0; i < max; ++i) {
      a->get_encoded_bytes_test();
    }
  } else {
    for (int i = 0; i < max; ++i) {
      a->get_encoded_bytes();
    }
  }

  utime_t end = ceph_clock_now(NULL);
  cout << max << " encodes in " << (end - start) << std::endl;

  delete a;
}

TEST(Transaction, GetNumBytesBenchLegacy)
{
   bench_num_bytes(true);
}

TEST(Transaction, GetNumBytesBenchCurrent)
{
   bench_num_bytes(false);
}
