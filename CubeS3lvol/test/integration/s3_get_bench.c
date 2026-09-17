/* Copyright (c) 2026 Tencent Inc.
 * SPDX-License-Identifier: Apache-2.0 */
/*
 * Measure concurrent whole-object COS GET bandwidth through s3_get_range(),
 * the same CRT client dest restore uses.
 *
 * Default: 256 concurrent 1 MiB GETs of distinct keys, matching
 * S3_WHOLE_GET_MAX_INFLIGHT and the dest chunk size.
 *
 * Credentials from the environment only.
 *
 * Usage:
 *   export AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=...
 *   ./s3_get_bench --endpoint cos.ap-nanjing.myqcloud.com --bucket <name> \
 *                  [--region ap-nanjing] [--inflight 256] [--size-mib 1] \
 *                  [--rounds 3] [--threads 8]
 */

#include "spdk/stdinc.h"
#include "spdk/log.h"

#include "s3lvol/s3_client.h"
#include "s3lvol/s3_spawner.h"

#include <getopt.h>
#include <inttypes.h>

#define DEFAULT_INFLIGHT  256
#define DEFAULT_SIZE_MIB  1
#define DEFAULT_ROUNDS    3
#define DEFAULT_THREADS   8
#define WAIT_TIMEOUT_SEC  180

struct wave {
	pthread_mutex_t mutex;
	pthread_cond_t  cond;
	uint32_t        remaining;
	uint32_t        errors;
	uint64_t        bytes;
};

struct slot {
	struct wave    *wave;
	char            key[256];
	void           *buf;
	uint64_t        bytes;
	int             status;
	struct timespec done;
};

static void
wave_init(struct wave *w, uint32_t remaining)
{
	pthread_mutex_init(&w->mutex, NULL);
	pthread_cond_init(&w->cond, NULL);
	w->remaining = remaining;
	w->errors = 0;
	w->bytes = 0;
}

static void
wave_fini(struct wave *w)
{
	pthread_mutex_destroy(&w->mutex);
	pthread_cond_destroy(&w->cond);
}

static int
wave_wait(struct wave *w)
{
	struct timespec deadline;
	int rc = 0;

	clock_gettime(CLOCK_REALTIME, &deadline);
	deadline.tv_sec += WAIT_TIMEOUT_SEC;

	pthread_mutex_lock(&w->mutex);
	while (w->remaining > 0 && rc == 0) {
		rc = pthread_cond_timedwait(&w->cond, &w->mutex, &deadline);
	}
	pthread_mutex_unlock(&w->mutex);
	return rc == 0 ? 0 : -ETIMEDOUT;
}

static void
put_done(void *cb_arg, int status)
{
	struct slot *s = cb_arg;
	struct wave *w = s->wave;

	s->status = status;
	pthread_mutex_lock(&w->mutex);
	if (status != 0) {
		w->errors++;
	}
	if (--w->remaining == 0) {
		pthread_cond_signal(&w->cond);
	}
	pthread_mutex_unlock(&w->mutex);
}

static void
get_done(void *cb_arg, uint64_t bytes_read, int status)
{
	struct slot *s = cb_arg;
	struct wave *w = s->wave;

	clock_gettime(CLOCK_MONOTONIC, &s->done);
	s->status = status;
	s->bytes = bytes_read;
	pthread_mutex_lock(&w->mutex);
	if (status != 0) {
		w->errors++;
	} else {
		w->bytes += bytes_read;
	}
	if (--w->remaining == 0) {
		pthread_cond_signal(&w->cond);
	}
	pthread_mutex_unlock(&w->mutex);
}

static void
delete_done(void *cb_arg, int status)
{
	struct completion {
		pthread_mutex_t mutex;
		pthread_cond_t cond;
		bool done;
		int status;
	} *c = cb_arg;

	(void)status;
	pthread_mutex_lock(&c->mutex);
	c->done = true;
	c->status = status;
	pthread_cond_signal(&c->cond);
	pthread_mutex_unlock(&c->mutex);
}

static uint64_t
ns_since(const struct timespec *start, const struct timespec *end)
{
	return (uint64_t)(end->tv_sec - start->tv_sec) * 1000000000ULL +
	       (uint64_t)(end->tv_nsec - start->tv_nsec);
}

static int
cmp_u64(const void *a, const void *b)
{
	uint64_t x = *(const uint64_t *)a;
	uint64_t y = *(const uint64_t *)b;

	return (x > y) - (x < y);
}

static void
usage(const char *argv0)
{
	fprintf(stderr,
		"Usage: %s --endpoint HOST --bucket NAME [options]\n"
		"  --region     COS region (default ap-nanjing)\n"
		"  --prefix     object prefix (default s3lvol-getbench/)\n"
		"  --inflight   concurrent GETs (default %d)\n"
		"  --size-mib   object size (default %d)\n"
		"  --rounds     GET waves after PUT (default %d; first is warmup)\n"
		"  --threads    CRT event-loop threads (default %d)\n"
		"  --path-style\n"
		"  --no-tls\n"
		"  --keep       leave objects in the bucket\n",
		argv0, DEFAULT_INFLIGHT, DEFAULT_SIZE_MIB, DEFAULT_ROUNDS,
		DEFAULT_THREADS);
}

int
main(int argc, char **argv)
{
	static const struct option long_opts[] = {
		{ "endpoint",   required_argument, NULL, 'e' },
		{ "bucket",     required_argument, NULL, 'b' },
		{ "region",     required_argument, NULL, 'r' },
		{ "prefix",     required_argument, NULL, 'p' },
		{ "inflight",   required_argument, NULL, 'n' },
		{ "size-mib",   required_argument, NULL, 's' },
		{ "rounds",     required_argument, NULL, 'R' },
		{ "threads",    required_argument, NULL, 't' },
		{ "path-style", no_argument,       NULL, 'P' },
		{ "no-tls",     no_argument,       NULL, 'T' },
		{ "keep",       no_argument,       NULL, 'k' },
		{ "help",       no_argument,       NULL, 'h' },
		{ 0, 0, 0, 0 },
	};
	const char *endpoint = NULL;
	const char *bucket = NULL;
	const char *region = "ap-nanjing";
	const char *prefix = "s3lvol-getbench/";
	uint32_t inflight = DEFAULT_INFLIGHT;
	uint32_t size_mib = DEFAULT_SIZE_MIB;
	uint32_t rounds = DEFAULT_ROUNDS;
	uint32_t threads = DEFAULT_THREADS;
	bool path_style = false;
	bool verify_tls = true;
	bool keep = false;
	uint32_t i, round;
	size_t obj_size;
	struct slot *slots = NULL;
	uint64_t *lat_ns = NULL;
	const char **keys = NULL;
	cpu_set_t allowed;
	struct s3_target target;
	struct s3_client *client = NULL;
	struct wave wave;
	struct timespec t0, t1;
	int opt, rc = 1;

	while ((opt = getopt_long(argc, argv, "e:b:r:p:n:s:R:t:PTkh",
				  long_opts, NULL)) != -1) {
		switch (opt) {
		case 'e': endpoint = optarg; break;
		case 'b': bucket = optarg; break;
		case 'r': region = optarg; break;
		case 'p': prefix = optarg; break;
		case 'n': inflight = (uint32_t)strtoul(optarg, NULL, 10); break;
		case 's': size_mib = (uint32_t)strtoul(optarg, NULL, 10); break;
		case 'R': rounds = (uint32_t)strtoul(optarg, NULL, 10); break;
		case 't': threads = (uint32_t)strtoul(optarg, NULL, 10); break;
		case 'P': path_style = true; break;
		case 'T': verify_tls = false; break;
		case 'k': keep = true; break;
		case 'h': usage(argv[0]); return 0;
		default: usage(argv[0]); return 1;
		}
	}

	if (!endpoint || !bucket || inflight == 0 || size_mib == 0 ||
	    rounds == 0 || threads == 0) {
		usage(argv[0]);
		return 1;
	}
	if (!getenv("AWS_ACCESS_KEY_ID") || !getenv("AWS_SECRET_ACCESS_KEY")) {
		fprintf(stderr, "AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY must be set\n");
		return 1;
	}

	obj_size = (size_t)size_mib << 20;
	slots = calloc(inflight, sizeof(*slots));
	lat_ns = calloc(inflight, sizeof(*lat_ns));
	keys = calloc(inflight, sizeof(*keys));
	if (!slots || !lat_ns || !keys) {
		fprintf(stderr, "allocation failed\n");
		goto out;
	}

	for (i = 0; i < inflight; i++) {
		uint8_t *buf = malloc(obj_size);
		uint32_t b;

		if (!buf) {
			fprintf(stderr, "buffer allocation failed at %u\n", i);
			goto out;
		}
		for (b = 0; b < obj_size / sizeof(uint32_t); b++) {
			((uint32_t *)buf)[b] = (i + 1) * 0x9e3779b9u + b;
		}
		slots[i].buf = buf;
		snprintf(slots[i].key, sizeof(slots[i].key),
			 "%sobj-%d-%u.bin", prefix, (int)getpid(), i);
		keys[i] = slots[i].key;
	}

	spdk_log_set_print_level(SPDK_LOG_NOTICE);
	spdk_log_open(NULL);

	CPU_ZERO(&allowed);
	if (sched_getaffinity(0, sizeof(allowed), &allowed) != 0) {
		fprintf(stderr, "sched_getaffinity: %s\n", strerror(errno));
		goto out_log;
	}
	if (s3_spawner_start(&allowed) != 0) {
		fprintf(stderr, "s3_spawner_start failed\n");
		goto out_log;
	}
	if (s3_crt_global_init(threads) != 0) {
		fprintf(stderr, "s3_crt_global_init failed\n");
		goto out_spawner;
	}

	memset(&target, 0, sizeof(target));
	target.endpoint = (char *)endpoint;
	target.region = (char *)region;
	target.bucket = (char *)bucket;
	target.auth_mode = S3_AUTH_ENV;
	target.use_path_style = path_style;
	target.verify_tls = verify_tls;
	if (s3_client_get_or_create(&target, &client) != 0 || !client) {
		fprintf(stderr, "s3_client_get_or_create failed\n");
		goto out_crt;
	}

	printf("=== COS concurrent GET bench ===\n");
	printf("endpoint   : %s\n", endpoint);
	printf("bucket     : %s\n", bucket);
	printf("region     : %s\n", region);
	printf("objects    : %u x %u MiB (distinct keys)\n", inflight, size_mib);
	printf("inflight   : %u (S3_WHOLE_GET_MAX_INFLIGHT=%d)\n",
	       inflight, S3_WHOLE_GET_MAX_INFLIGHT);
	printf("crt threads: %u  tls=%s\n", threads, verify_tls ? "on" : "off");
	printf("rounds     : %u (first is warmup)\n\n", rounds);

	printf("-- PUT %u objects --\n", inflight);
	wave_init(&wave, inflight);
	clock_gettime(CLOCK_MONOTONIC, &t0);
	for (i = 0; i < inflight; i++) {
		struct iovec iov = {
			.iov_base = slots[i].buf,
			.iov_len = obj_size,
		};

		slots[i].wave = &wave;
		rc = s3_put(client, slots[i].key, &iov, 1, false, put_done,
			    &slots[i]);
		if (rc != 0) {
			put_done(&slots[i], rc);
		}
	}
	rc = wave_wait(&wave);
	clock_gettime(CLOCK_MONOTONIC, &t1);
	{
		double sec = ns_since(&t0, &t1) / 1e9;
		double mib = (double)inflight * size_mib;

		printf("PUT done in %.3fs  errors=%u  %.2f MiB/s  (%.2f Gbit/s)\n\n",
		       sec, wave.errors, mib / sec, (mib * 8.0) / (sec * 1024.0));
	}
	if (rc != 0 || wave.errors != 0) {
		fprintf(stderr, "PUT wave failed (timeout=%s errors=%u); not measuring GETs\n",
			rc != 0 ? "yes" : "no", wave.errors);
		wave_fini(&wave);
		goto out_cleanup;
	}
	wave_fini(&wave);

	for (round = 0; round < rounds; round++) {
		uint32_t ok = 0;
		uint64_t p50, p99, pmax;

		wave_init(&wave, inflight);
		clock_gettime(CLOCK_MONOTONIC, &t0);
		for (i = 0; i < inflight; i++) {
			slots[i].wave = &wave;
			slots[i].status = 0;
			slots[i].bytes = 0;
			rc = s3_get_range(client, slots[i].key, 0, obj_size,
					  slots[i].buf, get_done, &slots[i]);
			if (rc != 0) {
				get_done(&slots[i], 0, rc);
			}
		}
		rc = wave_wait(&wave);
		clock_gettime(CLOCK_MONOTONIC, &t1);

		for (i = 0; i < inflight; i++) {
			lat_ns[i] = ns_since(&t0, &slots[i].done);
			if (slots[i].status == 0 && slots[i].bytes == obj_size) {
				ok++;
			}
		}
		qsort(lat_ns, inflight, sizeof(*lat_ns), cmp_u64);
		p50 = lat_ns[inflight / 2];
		p99 = lat_ns[(inflight * 99) / 100];
		pmax = lat_ns[inflight - 1];

		{
			double sec = ns_since(&t0, &t1) / 1e9;
			double mib = wave.bytes / (1024.0 * 1024.0);
			double gib_s = (mib / 1024.0) / sec;
			double gbit_s = (mib * 8.0) / (sec * 1024.0);

			printf("GET round %u%s: %u/%u in %.3fs  %.2f MiB  "
			       "%.3f GiB/s  %.2f Gbit/s\n",
			       round + 1, round == 0 ? " (warmup)" : "        ",
			       ok, inflight, sec, mib, gib_s, gbit_s);
			printf("             latency p50=%.1f ms  p99=%.1f ms  "
			       "max=%.1f ms  errors=%u%s\n",
			       p50 / 1e6, p99 / 1e6, pmax / 1e6, wave.errors,
			       rc != 0 ? "  TIMEOUT" : "");
		}
		wave_fini(&wave);
	}

	rc = 0;

out_cleanup:
	if (!keep) {
		struct {
			pthread_mutex_t mutex;
			pthread_cond_t cond;
			bool done;
			int status;
		} del;
		struct timespec deadline;

		pthread_mutex_init(&del.mutex, NULL);
		pthread_cond_init(&del.cond, NULL);
		del.done = false;
		del.status = 0;
		printf("\n-- DELETE %u objects --\n", inflight);
		if (s3_delete_batch(client, keys, inflight, delete_done, &del) == 0) {
			clock_gettime(CLOCK_REALTIME, &deadline);
			deadline.tv_sec += WAIT_TIMEOUT_SEC;
			pthread_mutex_lock(&del.mutex);
			while (!del.done) {
				if (pthread_cond_timedwait(&del.cond, &del.mutex,
							   &deadline) != 0) {
					break;
				}
			}
			pthread_mutex_unlock(&del.mutex);
			printf("DELETE status=%d\n", del.status);
		}
		pthread_mutex_destroy(&del.mutex);
		pthread_cond_destroy(&del.cond);
	}

	s3_client_put(client);
out_crt:
	s3_crt_global_fini();
out_spawner:
	s3_spawner_stop();
out_log:
	spdk_log_close();
out:
	if (slots) {
		for (i = 0; i < inflight; i++) {
			free(slots[i].buf);
		}
	}
	free(slots);
	free(lat_ns);
	free(keys);
	return rc;
}
