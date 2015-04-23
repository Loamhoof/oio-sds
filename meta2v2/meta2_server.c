/*
OpenIO SDS meta2v2
Copyright (C) 2014 Worldine, original work as part of Redcurrant
Copyright (C) 2015 OpenIO, modified as part of OpenIO Software Defined Storage

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU Affero General Public License as
published by the Free Software Foundation, either version 3 of the
License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

#ifndef G_LOG_DOMAIN
# define G_LOG_DOMAIN "grid.meta2.server"
#endif

#include <stdlib.h>
#include <stddef.h>
#include <errno.h>
#include <string.h>
#include <sys/time.h>
#include <sys/resource.h>

#include <metautils/lib/metautils.h>
#include <cluster/lib/gridcluster.h>
#include <server/network_server.h>
#include <server/transport_gridd.h>
#include <server/grid_daemon.h>
#include <resolver/hc_resolver.h>
#include <sqliterepo/sqliterepo.h>
#include <sqliterepo/replication_dispatcher.h>
#include <sqliterepo/upgrade.h>
#include <meta2v2/meta2_gridd_dispatcher.h>
#include <meta2v2/meta2_backend.h>
#include <meta2v2/meta2_backend_internals.h>
#include <meta2v2/meta2_events.h>
#include <sqlx/sqlx_service.h>
#include <sqlx/sqlx_service_extras.h>

#include <zmq.h>

static GAsyncQueue *q_notify = NULL;
static GThread *th_notify = NULL;

static struct meta2_backend_s *m2 = NULL;
static void *zmq_ctx = NULL;

static void
_notification_manage (void *zpush, gchar *tmp)
{
	int rc, err;
	if (zpush) {
		rc = zmq_send (zpush, tmp, strlen(tmp), 0);
		if (rc < 0) {
			err = zmq_errno ();
			GRID_WARN("Notification failed : container state %s: (%d) %s",
					tmp, err, zmq_strerror(err));
		} else {
			GRID_DEBUG("Notified container state %s", tmp);
		}
	} else {
		GRID_INFO("Container usage [%s]", tmp);
	}
	g_free (tmp);
}

static gpointer
_func_notify_account (gpointer p)
{
	void *zpush = NULL;

	if (zmq_ctx) {
		gchar *url =  gridcluster_get_accountagent (PSRV(p)->ns_name);
		if (url) {
			int rc, err;
			zpush = zmq_socket (zmq_ctx, ZMQ_PUSH);
			if (!zpush) {
				err = zmq_errno ();
				GRID_WARN("ZMQ socket creation error : (%d) %s",
						err, zmq_strerror(err));
			} else {
				if (0 > (rc = zmq_connect (zpush, url))) {
					err = zmq_errno ();
					GRID_WARN("ZMQ connection error (account-agent) : (%d) %s",
							err, zmq_strerror (err));
					zmq_close (zpush);
					zpush = NULL;
				}

				int opt = 1000;
				zmq_setsockopt (zpush, ZMQ_LINGER, &opt, sizeof(opt));
			}
			g_free (url);
		}
	}

	gchar *tmp;
	while (grid_main_is_running()) {
		tmp = (gchar*) g_async_queue_timeout_pop (q_notify, 1000000L);
		if (!tmp)
			continue;
		_notification_manage (zpush, tmp);
	}
	while (NULL != (tmp = g_async_queue_try_pop (q_notify)))
		_notification_manage (zpush, tmp);

	if (zpush) {
		zmq_close (zpush);
		zpush = NULL;
	}

	return p;
}

static void
_task_reconfigure_m2(gpointer p)
{
	meta2_backend_configure_nsinfo(m2, &(PSRV(p)->nsinfo));
}

static gchar **
filter_services(struct sqlx_service_s *ss,
		gchar **s, gint64 seq, const gchar *type)
{
	gboolean matched = FALSE;
	GPtrArray *tmp = g_ptr_array_new();
	(void) seq;
	for (; *s ;s++) {
		struct meta1_service_url_s *u = meta1_unpack_url(*s);
		if (0 == strcmp(type, u->srvtype)) {
			if (!g_ascii_strcasecmp(u->host, ss->url->str))
				matched = TRUE;
			else
				g_ptr_array_add(tmp, g_strdup(u->host));
		}
		meta1_service_url_clean(u);
	}

	gchar **out = (gchar**)metautils_gpa_to_array (tmp, TRUE);
	if (matched)
		return out;
	g_strfreev (out);
	return NULL;
}

static gchar **
filter_services_and_clean(struct sqlx_service_s *ss,
		gchar **src, gint64 seq, const gchar *type)
{
	if (!src)
		return NULL;
	gchar **result = filter_services(ss, src, seq, type);
	g_strfreev(src);
	return result;
}

static struct hc_url_s *
_init_hc_url(struct sqlx_service_s *ss, struct sqlx_name_s *n, gint64 *pseq)
{
	gint64 seq = 1;
	struct hc_url_s *u = hc_url_empty();

	hc_url_set(u, HCURL_NS, ss->ns_name);

	const gchar *sep = strchr(n->base, '@');
	if (!sep)
		sep = n->base;
	else {
		seq = g_ascii_strtoll(n->base, NULL, 10);
		++ sep;
	}
	if (!hc_url_set(u, HCURL_HEXID, sep)) {
		hc_url_clean(u);
		return NULL;
	}
	*pseq = seq;
	return u;
}

static GError *
_get_peers(struct sqlx_service_s *ss, struct sqlx_name_s *n,
		gboolean nocache, gchar ***result)
{
	EXTRA_ASSERT(ss != NULL);
	EXTRA_ASSERT(result != NULL);
	SQLXNAME_CHECK(n);

	if (!g_str_has_prefix(n->type, NAME_SRVTYPE_META2))
		return NEWERROR(CODE_BAD_REQUEST, "Invalid type name: '%s'", n->type);

	gint retries = 1;
	gint64 seq = 1;
	struct hc_url_s *u = NULL;
	gchar **peers = NULL;
	GError *err = NULL;

	u = _init_hc_url(ss, n, &seq);
	if (!u)
		return NEWERROR(CODE_BAD_REQUEST, "Invalid base name [%s]", n->base);

retry:
	if (nocache) {
		hc_decache_reference_service(ss->resolver, u, n->type);
	}
	peers = NULL;
	err = hc_resolve_reference_service(ss->resolver, u, n->type, &peers);

	if (NULL != err) {
		g_prefix_error(&err, "Peer resolution error: ");
		hc_url_clean(u);
		return err;
	}

	gchar **out = filter_services_and_clean(ss, peers, seq, n->type);

	if (!out) {
		if (retries-- > 0) {
			nocache = TRUE;
			goto retry;
		}
		err = NEWERROR(CODE_CONTAINER_NOTFOUND, "Base not managed");
	}

	if (err) {
		if (out)
			g_strfreev (out);
		*result = NULL;
	} else {
		*result = out;
	}

	hc_url_clean(u);
	return err;
}

static void
meta2_on_close(struct sqlx_sqlite3_s *sq3, gboolean deleted, gpointer cb_data)
{
	gint64 seq = 1;
	EXTRA_ASSERT(sq3 != NULL);

	if (!deleted)
		return;

	struct hc_url_s *u = _init_hc_url(PSRV(cb_data), sqlx_name_mutable_to_const(&sq3->name), &seq);
	if (!u) {
		GRID_WARN("Invalid base name [%s]", sq3->name.base);
		return;
	}
	hc_decache_reference_service(PSRV(cb_data)->resolver, u, NAME_SRVTYPE_META2);
	hc_url_clean(u);
	u = NULL;
}

static gboolean
_post_config(struct sqlx_service_s *ss)
{
	GError *err = NULL;

	err = sqlx_service_extras_init(ss);
	if (err != NULL) {
		GRID_WARN("%s", err->message);
		g_clear_error(&err);
		return FALSE;
	}

	// prepare a meta2 backend
	err = meta2_backend_init(&m2, ss->repository, ss->ns_name, ss->extras->lb,
			ss->resolver, ss->extras->evt_repo);
	if (err) {
		GRID_WARN("META2 backend init failure: (%d) %s", err->code, err->message);
		g_clear_error(&err);
		return FALSE;
	}

	/* Make deleted bases exit the cache */
	sqlx_repository_configure_close_callback(ss->repository, meta2_on_close, ss);

	// Register meta2 requests handlers
	transport_gridd_dispatcher_add_requests(ss->dispatcher,
			meta2_gridd_get_v1_requests(), m2);
	transport_gridd_dispatcher_add_requests(ss->dispatcher,
			meta2_gridd_get_v2_requests(), m2);

	// Register few meta2 tasks
	grid_task_queue_register(ss->gtq_reload, 5,
			(GDestroyNotify)sqlx_task_reload_event_config, NULL, ss);
	grid_task_queue_register(ss->gtq_reload, 5,
			_task_reconfigure_m2, NULL, ss);
	grid_task_queue_register(ss->gtq_reload, 10,
			(GDestroyNotify)sqlx_task_reload_lb, NULL, ss);

	q_notify = g_async_queue_new();
	th_notify = g_thread_try_new("notifier", _func_notify_account, ss, &err);
	if (!th_notify) {
		GRID_WARN("META2 notifier thread start failure: (%d) %s",
				gerror_get_code(err), gerror_get_message(err));
		g_clear_error(&err);
		return FALSE;
	}
	m2->q_notify = q_notify;

	return TRUE;
}

int
main(int argc, char **argv)
{
	struct sqlx_service_config_s cfg = {
		NAME_SRVTYPE_META2, "m2v2", "el/" NAME_SRVTYPE_META2, 2, 2, schema,
		_get_peers, _post_config, NULL
	};

	zmq_ctx = zmq_ctx_new ();
	int rc = sqlite_service_main (argc, argv, &cfg);
	if (m2)
		meta2_backend_clean (m2);
	if (th_notify)
		g_thread_join (th_notify);
	if (q_notify)
		g_async_queue_unref (q_notify);
	if (zmq_ctx) {
		zmq_ctx_term (zmq_ctx);
		zmq_ctx = NULL;
	}
	return rc;
}

