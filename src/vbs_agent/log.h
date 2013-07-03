/*

/*
 *   Copyright 2013 Zynga Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
 * moxi logging API
 * mtaneja@zynga.com
 */

#ifndef _LOG_H_
#define _LOG_H_

#include <errno.h>
#include <string.h>

/*
 * define the log levels
 */
#define MOXI_LOG_CRIT    1
#define MOXI_LOG_ERR     5
#define MOXI_LOG_INFO    10
#define MOXI_LOG_DEBUG   15

#define ERRORLOG_STDERR        0x1
#define ERRORLOG_FILE          0x2
#define ERRORLOG_SYSLOG        0x4

struct moxi_log {
    int fd;             /* log fd */
    int log_level;      /* logging level. default 5 */
    int log_mode;       /* syslog, log file, stderr */
    char *log_ident;    /* syslog identifier */
    char *log_file;     /* if log file is specified */
    int use_syslog;     /* set if syslog is being used */
    time_t base_ts;     /* base timestamp */
    time_t last_generated_debug_ts;
};

typedef struct moxi_log moxi_log;

int log_error_open(moxi_log *);
int log_error_close(moxi_log *);
int log_error_write(moxi_log *, const char *filename, unsigned int line, const char *fmt, ...);
int log_error_cycle(moxi_log *);

#ifndef MAIN_CHECK
extern moxi_log *ml;
#define moxi_log_write(...) log_error_write(ml, __FILE__, __LINE__, __VA_ARGS__)
#else
#define moxi_log_write(...) fprintf(stderr, __VA_ARGS__)
#endif

#undef perror
#define perror(str) log_error_write(ml, __FILE__, __LINE__, str, ": %s", strerror(errno));

#endif
