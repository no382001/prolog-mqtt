/*
 * mqtt_bridge.c -- TCP bridge between Prolog and MQTT brokers
 *
 * accepts one TCP connection from Prolog and multiplexes N MQTT connections
 * over it using integer connection IDs.
 *
 * commands (Prolog -> bridge, tab-separated fields, newline-terminated):
 *   connect\t<id>\t<host>\t<port>\t<clientid>\t<keepalive>[\t<tls>\t<ca_cert>\t<client_cert>\t<client_key>]
 *   log\t<0|1>
 *     tls: 0=plain, 1=TLS (verify server cert), 2=TLS (skip server cert verify)
 *     ca_cert, client_cert, client_key: file paths or empty string
 *   publish\t<id>\t<topic>\t<payload>\t<qos>\t<retain>
 *   subscribe\t<id>\t<topic>\t<qos>
 *   unsubscribe\t<id>\t<topic>
 *   disconnect\t<id>
 *
 * events (bridge -> Prolog, one Prolog term per line):
 *   connected(Id, 0).
 *   disconnected(Id, Cause).
 *   message(Id, Topic, Payload, QoS, Retain).
 *   published(Id, Token).
 *   subscribed(Id, Token).
 *   unsubscribed(Id, Token).
 *   error(Id, Description).
 */

#include "MQTTAsync.h"
#include <arpa/inet.h>
#include <netinet/in.h>
#include <pthread.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

#define MAX_CONNS 16
#define DEFAULT_PORT 7883
#define LINE_BUF 65536

#define nil NULL
#define size(a) (sizeof(a) / sizeof((a)[0]))

typedef struct {
  int id;
  MQTTAsync client;
  int used;
} conn_t;

static conn_t g_conns[MAX_CONNS];
static pthread_mutex_t g_conns_mu = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t g_write_mu = PTHREAD_MUTEX_INITIALIZER;
static int g_log = 1;

#define LOG(...)                                                               \
  do {                                                                         \
    if (g_log)                                                                 \
      fprintf(stderr, __VA_ARGS__);                                            \
  } while (0)
static FILE *g_out = nil;

static void send_event(const char *fmt, ...);

static int conn_index(int id) {
  for (int i = 0; i < MAX_CONNS; i++)
    if (g_conns[i].used && g_conns[i].id == id)
      return i;
  return -1;
}

static int conn_alloc(int id) {
  for (int i = 0; i < MAX_CONNS; i++)
    if (!g_conns[i].used) {
      g_conns[i].id = id;
      g_conns[i].used = 1;
      return i;
    }
  return -1;
}

static void conn_free_idx(int idx) {
  if (g_conns[idx].client)
    MQTTAsync_destroy(&g_conns[idx].client);
  memset(&g_conns[idx], 0, sizeof(g_conns[idx]));
}

/* resolve callback context to connection id (-1 if slot gone) */
static int ctx_id(void *ctx) {
  int idx = (int)(intptr_t)ctx;
  pthread_mutex_lock(&g_conns_mu);
  int id = g_conns[idx].used ? g_conns[idx].id : -1;
  pthread_mutex_unlock(&g_conns_mu);
  return id;
}

/* free slot from callback context; returns id, or -1 if already gone */
static int ctx_free(void *ctx) {
  int idx = (int)(intptr_t)ctx;
  pthread_mutex_lock(&g_conns_mu);
  if (!g_conns[idx].used) {
    pthread_mutex_unlock(&g_conns_mu);
    return -1;
  }
  int id = g_conns[idx].id;
  conn_free_idx(idx);
  pthread_mutex_unlock(&g_conns_mu);
  return id;
}

/* look up a live connection by id; returns slot index and sets *out_client.
 * on failure sends error event and returns -1. */
static int lookup_conn(int id, MQTTAsync *out_client) {
  pthread_mutex_lock(&g_conns_mu);
  int idx = conn_index(id);
  if (idx < 0) {
    pthread_mutex_unlock(&g_conns_mu);
    send_event("error(%d,'not connected').\n", id);
    return -1;
  }
  *out_client = g_conns[idx].client;
  pthread_mutex_unlock(&g_conns_mu);
  return idx;
}

static void send_event(const char *fmt, ...) {
  va_list ap;
  pthread_mutex_lock(&g_write_mu);
  if (g_out) {
    va_start(ap, fmt);
    vfprintf(g_out, fmt, ap);
    va_end(ap);
    fflush(g_out);
  }
  pthread_mutex_unlock(&g_write_mu);
}

/* write a single-quoted Prolog atom, escaping ' and \ */
static void fwrite_atom(FILE *f, const char *s, int len) {
  fputc('\'', f);
  for (int i = 0; i < len; i++) {
    if (s[i] == '\'' || s[i] == '\\')
      fputc('\\', f);
    fputc((unsigned char)s[i], f);
  }
  fputc('\'', f);
}

static void send_message(int id, char *topic, int topicLen, void *payload,
                         int payloadLen, int qos, int retained) {
  pthread_mutex_lock(&g_write_mu);
  if (g_out) {
    fprintf(g_out, "message(%d,", id);
    fwrite_atom(g_out, topic, topicLen > 0 ? topicLen : (int)strlen(topic));
    fputc(',', g_out);
    fwrite_atom(g_out, (char *)payload, payloadLen);
    fprintf(g_out, ",%d,%d).\n", qos, retained);
    fflush(g_out);
  }
  pthread_mutex_unlock(&g_write_mu);
}

static int messageArrived(void *ctx, char *topic, int topicLen,
                          MQTTAsync_message *msg) {
  int id = ctx_id(ctx);
  if (id >= 0)
    send_message(id, topic, topicLen, msg->payload, msg->payloadlen, msg->qos,
                 msg->retained);
  MQTTAsync_freeMessage(&msg);
  MQTTAsync_free(topic);
  return 1;
}

static void connectionLost(void *ctx, char *cause) {
  int id = ctx_free(ctx);
  if (id >= 0)
    send_event("disconnected(%d,'%s').\n", id, cause ? cause : "");
}

static void onDeliveryComplete(void *ctx, MQTTAsync_token token) {
  int id = ctx_id(ctx);
  if (id >= 0)
    send_event("published(%d,%d).\n", id, (int)token);
}

static void onConnect(void *ctx, MQTTAsync_successData *resp) {
  (void)resp;
  int id = ctx_id(ctx);
  if (id >= 0)
    send_event("connected(%d,0).\n", id);
}

static void onConnectFailure(void *ctx, MQTTAsync_failureData *resp) {
  int id = ctx_free(ctx);
  if (id >= 0)
    send_event("error(%d,'connect failed rc=%d').\n", id,
               resp ? resp->code : -1);
}

static void onDisconnect(void *ctx, MQTTAsync_successData *resp) {
  (void)resp;
  int id = ctx_free(ctx);
  if (id >= 0)
    send_event("disconnected(%d,'clean').\n", id);
}

static void onSubscribe(void *ctx, MQTTAsync_successData *resp) {
  int id = ctx_id(ctx);
  if (id >= 0)
    send_event("subscribed(%d,%d).\n", id, resp ? (int)resp->token : 0);
}

static void onSubscribeFailure(void *ctx, MQTTAsync_failureData *resp) {
  int id = ctx_id(ctx);
  if (id >= 0)
    send_event("error(%d,'subscribe failed rc=%d').\n", id,
               resp ? resp->code : -1);
}

static void onUnsubscribe(void *ctx, MQTTAsync_successData *resp) {
  int id = ctx_id(ctx);
  if (id >= 0)
    send_event("unsubscribed(%d,%d).\n", id, resp ? (int)resp->token : 0);
}

static void cmd_connect(int id, const char *host, int port,
                        const char *clientid, int keepalive, int tls,
                        const char *ca_cert, const char *client_cert,
                        const char *client_key) {
  pthread_mutex_lock(&g_conns_mu);
  if (conn_index(id) >= 0) {
    pthread_mutex_unlock(&g_conns_mu);
    send_event("error(%d,'already connected').\n", id);
    return;
  }
  int idx = conn_alloc(id);
  if (idx < 0) {
    pthread_mutex_unlock(&g_conns_mu);
    send_event("error(%d,'too many connections').\n", id);
    return;
  }

  char uri[256];
  snprintf(uri, sizeof(uri), "%s://%s:%d", tls ? "ssl" : "tcp", host, port);

  MQTTAsync_create(&g_conns[idx].client, uri, clientid,
                   MQTTCLIENT_PERSISTENCE_NONE, nil);
  MQTTAsync_setCallbacks(g_conns[idx].client, (void *)(intptr_t)idx,
                         connectionLost, messageArrived, onDeliveryComplete);

  MQTTAsync_connectOptions opts = MQTTAsync_connectOptions_initializer;
  opts.keepAliveInterval = keepalive;
  opts.cleansession = 1;
  opts.onSuccess = onConnect;
  opts.onFailure = onConnectFailure;
  opts.context = (void *)(intptr_t)idx;

  MQTTAsync_SSLOptions ssl = MQTTAsync_SSLOptions_initializer;
  if (tls) {
    ssl.enableServerCertAuth = (tls == 1) ? 1 : 0;
    ssl.verify = (tls == 1) ? 1 : 0;
    if (ca_cert && *ca_cert)
      ssl.trustStore = ca_cert;
    if (client_cert && *client_cert)
      ssl.keyStore = client_cert;
    if (client_key && *client_key)
      ssl.privateKey = client_key;
    opts.ssl = &ssl;
  }

  pthread_mutex_unlock(&g_conns_mu);
  MQTTAsync_connect(g_conns[idx].client, &opts);
}

static void cmd_log(int on) { g_log = on ? 1 : 0; }

static void cmd_disconnect(int id) {
  MQTTAsync client;
  int idx = lookup_conn(id, &client);
  if (idx < 0)
    return;

  MQTTAsync_disconnectOptions opts = MQTTAsync_disconnectOptions_initializer;
  opts.onSuccess = onDisconnect;
  opts.context = (void *)(intptr_t)idx;
  MQTTAsync_disconnect(client, &opts);
}

static void cmd_publish(int id, const char *topic, const char *payload, int qos,
                        int retain) {
  MQTTAsync client;
  int idx = lookup_conn(id, &client);
  if (idx < 0)
    return;

  MQTTAsync_responseOptions ropts = MQTTAsync_responseOptions_initializer;
  ropts.context = (void *)(intptr_t)idx;

  MQTTAsync_message msg = MQTTAsync_message_initializer;
  msg.payload = (void *)payload;
  msg.payloadlen = (int)strlen(payload);
  msg.qos = qos;
  msg.retained = retain;

  MQTTAsync_sendMessage(client, topic, &msg, &ropts);
}

static void cmd_subscribe(int id, const char *topic, int qos) {
  MQTTAsync client;
  int idx = lookup_conn(id, &client);
  if (idx < 0)
    return;

  MQTTAsync_responseOptions ropts = MQTTAsync_responseOptions_initializer;
  ropts.onSuccess = onSubscribe;
  ropts.onFailure = onSubscribeFailure;
  ropts.context = (void *)(intptr_t)idx;
  MQTTAsync_subscribe(client, topic, qos, &ropts);
}

static void cmd_unsubscribe(int id, const char *topic) {
  MQTTAsync client;
  int idx = lookup_conn(id, &client);
  if (idx < 0)
    return;

  MQTTAsync_responseOptions ropts = MQTTAsync_responseOptions_initializer;
  ropts.onSuccess = onUnsubscribe;
  ropts.context = (void *)(intptr_t)idx;
  MQTTAsync_unsubscribe(client, topic, &ropts);
}

typedef void (*cmd_fn_t)(char **a);

typedef struct {
  const char *name;
  int min_args; // a[min_args-1] must be non-null
  cmd_fn_t fn;
} cmd_entry_t;

static void dispatch_connect(char **a) {
  cmd_connect(atoi(a[0]), a[1], atoi(a[2]), a[3], atoi(a[4]),
              a[5] ? atoi(a[5]) : 0, a[6] ? a[6] : "", a[7] ? a[7] : "",
              a[8] ? a[8] : "");
}
static void dispatch_publish(char **a) {
  cmd_publish(atoi(a[0]), a[1], a[2], atoi(a[3]), atoi(a[4]));
}
static void dispatch_subscribe(char **a) {
  cmd_subscribe(atoi(a[0]), a[1], atoi(a[2]));
}
static void dispatch_unsubscribe(char **a) {
  cmd_unsubscribe(atoi(a[0]), a[1]);
}
static void dispatch_disconnect(char **a) { cmd_disconnect(atoi(a[0])); }
static void dispatch_log(char **a) { cmd_log(atoi(a[0])); }

static const cmd_entry_t cmd_table[] = {
    {"connect", 5, dispatch_connect},
    {"publish", 5, dispatch_publish},
    {"subscribe", 3, dispatch_subscribe},
    {"unsubscribe", 2, dispatch_unsubscribe},
    {"disconnect", 1, dispatch_disconnect},
    {"log", 1, dispatch_log},
};

static void dispatch_line(char *line) {
  char *cmd = strtok(line, "\t");
  if (!cmd)
    return;
  char *a[9] = {0};
  for (int i = 0; i < 9; i++)
    a[i] = strtok(nil, "\t");

  for (size_t i = 0; i < size(cmd_table); i++) {
    if (!strcmp(cmd, cmd_table[i].name)) {
      if (a[cmd_table[i].min_args - 1])
        cmd_table[i].fn(a);
      return;
    }
  }
}

int main(int argc, char **argv) {
  int port = DEFAULT_PORT;
  if (argc > 1)
    port = atoi(argv[1]);

  int srv = socket(AF_INET, SOCK_STREAM, 0);
  int one = 1;
  setsockopt(srv, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));

  struct sockaddr_in addr = {0};
  addr.sin_family = AF_INET;
  addr.sin_port = htons(port);
  addr.sin_addr.s_addr = INADDR_ANY;

  if (bind(srv, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
    perror("bind");
    return 1;
  }
  listen(srv, 1);
  LOG("mqtt_bridge: listening on :%d\n", port);

  for (;;) {
    int fd = accept(srv, nil, nil);
    if (fd < 0)
      continue;
    LOG("mqtt_bridge: prolog client connected\n");

    g_out = fdopen(dup(fd), "w");
    FILE *in = fdopen(fd, "r");

    char line[LINE_BUF];
    while (fgets(line, sizeof(line), in)) {
      int len = strlen(line);
      while (len > 0 && (line[len - 1] == '\n' || line[len - 1] == '\r'))
        line[--len] = '\0';
      if (len > 0)
        dispatch_line(line);
    }

    LOG("mqtt_bridge: prolog client disconnected\n");
    fclose(in);

    pthread_mutex_lock(&g_write_mu);
    if (g_out) {
      fclose(g_out);
      g_out = nil;
    }
    pthread_mutex_unlock(&g_write_mu);

    pthread_mutex_lock(&g_conns_mu);
    for (int i = 0; i < MAX_CONNS; i++)
      if (g_conns[i].used)
        conn_free_idx(i);
    pthread_mutex_unlock(&g_conns_mu);
  }

  close(srv);
  return 0;
}
