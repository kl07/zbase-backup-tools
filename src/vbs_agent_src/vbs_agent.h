#ifndef VBS_AGENT_H
#define VBS_AGENT_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <pthread.h>
#include <assert.h>

#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <string.h>
#include <sys/types.h>
#include <time.h>


typedef struct {

    char *hostname;       // hostname of vbs server
    int port;             // port of vbs server

} vbs_config_t;

int start_vbs_config(vbs_config_t *config);
char *get_current_config();

#endif

