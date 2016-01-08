// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "include/types.h"
#include "include/atomic.h"
#include "common/Initialize.h"
#include "Messenger.h"

#include "msg/simple/SimpleMessenger.h"
#include "msg/async/AsyncMessenger.h"
#ifdef HAVE_XIO
#include "msg/xio/XioMessenger.h"
#endif

static atomic_t cfd(0);

Messenger *Messenger::create(CephContext *cct, const string &type,
			     entity_name_t name, string lname,
			     uint64_t nonce)
{
  if (!cfd.read() && cct->_conf->ms_enable_dma_latency) {
    int32_t latency = 0;
    int fd = open("/dev/cpu_dma_latency", O_WRONLY);
    if (fd < 0) {
      assert(0 == "open /dev/cpu_dma_latency %m - need root permissions");
    }
    cfd.set(fd);
    if (write(fd, &latency, sizeof(latency)) != sizeof(latency)) {
      assert(0 == "write to /dev/cpu_dma_latency %m - need root permissions");
      close(fd);
      cfd.set(0);
    }
  }

  int r = -1;
  if (type == "random")
    r = rand() % 2; // random does not include xio
  if (r == 0 || type == "simple")
    return new SimpleMessenger(cct, name, lname, nonce);
  else if ((r == 1 || type == "async") &&
	   cct->check_experimental_feature_enabled("ms-type-async"))
    return new AsyncMessenger(cct, name, lname, nonce);
#ifdef HAVE_XIO
  else if ((type == "xio") &&
	   cct->check_experimental_feature_enabled("ms-type-xio"))
    return new XioMessenger(cct, name, lname, nonce);
#endif
  lderr(cct) << "unrecognized ms_type '" << type << "'" << dendl;
  return NULL;
}

/*
 * Pre-calculate desired software CRC settings.  CRC computation may
 * be disabled by default for some transports (e.g., those with strong
 * hardware checksum support).
 */
int Messenger::get_default_crc_flags(md_config_t * conf)
{
  int r = 0;
  if (conf->ms_crc_data)
    r |= MSG_CRC_DATA;
  if (conf->ms_crc_header)
    r |= MSG_CRC_HEADER;
  return r;
}
