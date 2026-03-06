/*
 * mqtt_bridge.c -- TCP bridge between Prolog and MQTT brokers
 *
 * accepts one TCP connection from Prolog and multiplexes N MQTT connections
 * over it using integer connection IDs.
 *
 * commands (Prolog -> bridge, tab-separated fields, newline-terminated):
 *   connect\t<id>\t<host>\t<port>\t<clientid>\t<keepalive>
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

typedef struct {
  int id;
  MQTTAsync client;
  int used;
} conn_t;

static conn_t g_conns[MAX_CONNS];
static pthread_mutex_t g_conns_mu = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t g_write_mu = PTHREAD_MUTEX_INITIALIZER;
static FILE *g_out = NULL;

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
  int idx = (int)(intptr_t)ctx;
  pthread_mutex_lock(&g_conns_mu);
  int id = g_conns[idx].used ? g_conns[idx].id : -1;
  pthread_mutex_unlock(&g_conns_mu);

  if (id >= 0)
    send_message(id, topic, topicLen, msg->payload, msg->payloadlen, msg->qos,
                 msg->retained);

  MQTTAsync_freeMessage(&msg);
  MQTTAsync_free(topic);
  return 1;
}

static void connectionLost(void *ctx, char *cause) {
  int idx = (int)(intptr_t)ctx;
  pthread_mutex_lock(&g_conns_mu);
  if (!g_conns[idx].used) {
    pthread_mutex_unlock(&g_conns_mu);
    return;
  }
  int id = g_conns[idx].id;
  conn_free_idx(idx);
  pthread_mutex_unlock(&g_conns_mu);
  send_event("disconnected(%d,'%s').\n", id, cause ? cause : "");
}

static void onDeliveryComplete(void *ctx, MQTTAsync_token token) {
  int idx = (int)(intptr_t)ctx;
  pthread_mutex_lock(&g_conns_mu);
  int id = g_conns[idx].used ? g_conns[idx].id : -1;
  pthread_mutex_unlock(&g_conns_mu);
  if (id >= 0)
    send_event("published(%d,%d).\n", id, (int)token);
}

static void onConnect(void *ctx, MQTTAsync_successData *resp) {
  (void)resp;
  int idx = (int)(intptr_t)ctx;
  pthread_mutex_lock(&g_conns_mu);
  int id = g_conns[idx].used ? g_conns[idx].id : -1;
  pthread_mutex_unlock(&g_conns_mu);
  if (id >= 0)
    send_event("connected(%d,0).\n", id);
}

static void onConnectFailure(void *ctx, MQTTAsync_failureData *resp) {
  int idx = (int)(intptr_t)ctx;
  pthread_mutex_lock(&g_conns_mu);
  if (!g_conns[idx].used) {
    pthread_mutex_unlock(&g_conns_mu);
    return;
  }
  int id = g_conns[idx].id;
  conn_free_idx(idx);
  pthread_mutex_unlock(&g_conns_mu);
  send_event("error(%d,'connect failed rc=%d').\n", id, resp ? resp->code : -1);
}

static void onDisconnect(void *ctx, MQTTAsync_successData *resp) {
  (void)resp;
  int idx = (int)(intptr_t)ctx;
  pthread_mutex_lock(&g_conns_mu);
  if (!g_conns[idx].used) {
    pthread_mutex_unlock(&g_conns_mu);
    return;
  }
  int id = g_conns[idx].id;
  conn_free_idx(idx);
  pthread_mutex_unlock(&g_conns_mu);
  send_event("disconnected(%d,'clean').\n", id);
}

static void onSubscribe(void *ctx, MQTTAsync_successData *resp) {
  int idx = (int)(intptr_t)ctx;
  pthread_mutex_lock(&g_conns_mu);
  int id = g_conns[idx].used ? g_conns[idx].id : -1;
  pthread_mutex_unlock(&g_conns_mu);
  if (id >= 0)
    send_event("subscribed(%d,%d).\n", id, resp ? (int)resp->token : 0);
}

static void onSubscribeFailure(void *ctx, MQTTAsync_failureData *resp) {
  int idx = (int)(intptr_t)ctx;
  pthread_mutex_lock(&g_conns_mu);
  int id = g_conns[idx].used ? g_conns[idx].id : -1;
  pthread_mutex_unlock(&g_conns_mu);
  if (id >= 0)
    send_event("error(%d,'subscribe failed rc=%d').\n", id,
               resp ? resp->code : -1);
}

static void onUnsubscribe(void *ctx, MQTTAsync_successData *resp) {
  int idx = (int)(intptr_t)ctx;
  pthread_mutex_lock(&g_conns_mu);
  int id = g_conns[idx].used ? g_conns[idx].id : -1;
  pthread_mutex_unlock(&g_conns_mu);
  if (id >= 0)
    send_event("unsubscribed(%d,%d).\n", id, resp ? (int)resp->token : 0);
}

static void cmd_connect(int id, const char *host, int port,
                        const char *clientid, int keepalive) {
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
  snprintf(uri, sizeof(uri), "tcp://%s:%d", host, port);

  MQTTAsync_create(&g_conns[idx].client, uri, clientid,
                   MQTTCLIENT_PERSISTENCE_NONE, NULL);
  MQTTAsync_setCallbacks(g_conns[idx].client, (void *)(intptr_t)idx,
                         connectionLost, messageArrived, onDeliveryComplete);

  MQTTAsync_connectOptions opts = MQTTAsync_connectOptions_initializer;
  opts.keepAliveInterval = keepalive;
  opts.cleansession = 1;
  opts.onSuccess = onConnect;
  opts.onFailure = onConnectFailure;
  opts.context = (void *)(intptr_t)idx;

  pthread_mutex_unlock(&g_conns_mu);
  MQTTAsync_connect(g_conns[idx].client, &opts);
}

static void cmd_disconnect(int id) {
  pthread_mutex_lock(&g_conns_mu);
  int idx = conn_index(id);
  if (idx < 0) {
    pthread_mutex_unlock(&g_conns_mu);
    send_event("error(%d,'not connected').\n", id);
    return;
  }
  MQTTAsync client = g_conns[idx].client;
  pthread_mutex_unlock(&g_conns_mu);

  MQTTAsync_disconnectOptions opts = MQTTAsync_disconnectOptions_initializer;
  opts.onSuccess = onDisconnect;
  opts.context = (void *)(intptr_t)idx;
  MQTTAsync_disconnect(client, &opts);
}

static void cmd_publish(int id, const char *topic, const char *payload, int qos,
                        int retain) {
  pthread_mutex_lock(&g_conns_mu);
  int idx = conn_index(id);
  if (idx < 0) {
    pthread_mutex_unlock(&g_conns_mu);
    send_event("error(%d,'not connected').\n", id);
    return;
  }
  MQTTAsync client = g_conns[idx].client;
  pthread_mutex_unlock(&g_conns_mu);

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
  pthread_mutex_lock(&g_conns_mu);
  int idx = conn_index(id);
  if (idx < 0) {
    pthread_mutex_unlock(&g_conns_mu);
    send_event("error(%d,'not connected').\n", id);
    return;
  }
  MQTTAsync client = g_conns[idx].client;
  pthread_mutex_unlock(&g_conns_mu);

  MQTTAsync_responseOptions ropts = MQTTAsync_responseOptions_initializer;
  ropts.onSuccess = onSubscribe;
  ropts.onFailure = onSubscribeFailure;
  ropts.context = (void *)(intptr_t)idx;
  MQTTAsync_subscribe(client, topic, qos, &ropts);
}

static void cmd_unsubscribe(int id, const char *topic) {
  pthread_mutex_lock(&g_conns_mu);
  int idx = conn_index(id);
  if (idx < 0) {
    pthread_mutex_unlock(&g_conns_mu);
    send_event("error(%d,'not connected').\n", id);
    return;
  }
  MQTTAsync client = g_conns[idx].client;
  pthread_mutex_unlock(&g_conns_mu);

  MQTTAsync_responseOptions ropts = MQTTAsync_responseOptions_initializer;
  ropts.onSuccess = onUnsubscribe;
  ropts.context = (void *)(intptr_t)idx;
  MQTTAsync_unsubscribe(client, topic, &ropts);
}

static void dispatch_line(char *line) {
  char *cmd = strtok(line, "\t");
  if (!cmd)
    return;
  char *a[5] = {0};
  for (int i = 0; i < 5; i++)
    a[i] = strtok(NULL, "\t");

  if (!strcmp(cmd, "connect") && a[4])
    cmd_connect(atoi(a[0]), a[1], atoi(a[2]), a[3], atoi(a[4]));
  else if (!strcmp(cmd, "publish") && a[4])
    cmd_publish(atoi(a[0]), a[1], a[2], atoi(a[3]), atoi(a[4]));
  else if (!strcmp(cmd, "subscribe") && a[2])
    cmd_subscribe(atoi(a[0]), a[1], atoi(a[2]));
  else if (!strcmp(cmd, "unsubscribe") && a[1])
    cmd_unsubscribe(atoi(a[0]), a[1]);
  else if (!strcmp(cmd, "disconnect") && a[0])
    cmd_disconnect(atoi(a[0]));
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
  fprintf(stderr, "mqtt_bridge: listening on :%d\n", port);

  for (;;) {
    int fd = accept(srv, NULL, NULL);
    if (fd < 0)
      continue;
    fprintf(stderr, "mqtt_bridge: prolog client connected\n");

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

    fprintf(stderr, "mqtt_bridge: prolog client disconnected\n");
    fclose(in);

    pthread_mutex_lock(&g_write_mu);
    if (g_out) {
      fclose(g_out);
      g_out = NULL;
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
