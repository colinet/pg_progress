#include <stdio.h>
#include <stdlib.h>
#include <libunwind.h>
#include <string.h>
#include <libgen.h>

#include "postgres.h"
#include "back_trace.h"

static char *prog = "main";
char *src_dir = "/srv/pg/";

static int get_file_and_line(unw_word_t addr, char *file, size_t flen, int *line);


int get_file_and_line(unw_word_t addr, char *file, size_t flen, int *line)
{
	static char buf[256];
	FILE* f;

	// prepare command to be executed
	// our program need to be passed after the -e parameter
	sprintf(buf, "/usr/bin/addr2line -C -e /srv/pg-9.6/bin/postgres -f -i %lx", addr);
	f = popen(buf, "r");
	if (f == NULL) {
		perror(buf);
		return 0;
	}
	
	// get function name
	fgets(buf, 256, f);

	// get file and line
	fgets(buf, 256, f);

	if (buf[0] != '?') {
	 	char *p = buf;

		// file name is until ':'
		while (*p != ':') {
			p++;
	 	}

		*p++ = 0;
															
		// after file name follows line number
		strcpy(file , buf);
		sscanf(p,"%d", line);
	} else {
		strcpy(file,"unkown");
		*line = 0;
	}

	pclose(f);
	return 0;
}

void back_trace(void)
{
	char name[256];
	unw_cursor_t cursor;
	unw_context_t uc;
	unw_word_t ip, sp, offp;
	char* buf;
	unsigned short used;
	char* src_file;
	unsigned short offset;

	buf = malloc(1024);
	if (buf == NULL) {
		elog(DEBUG4, "out of memory)");
		return;
	}

	unw_getcontext(&uc);
	unw_init_local(&cursor, &uc);

	// Do not print the base source dir in the stack trace
	offset = sizeof(src_dir);
	used = sprintf(buf, "=== thread back trace\n");

	while (unw_step(&cursor) > 0) {
		char file[256];
		int line = 0;

		name[0] = '\0';
		unw_get_proc_name(&cursor, name, 256, &offp);

		unw_get_reg(&cursor, UNW_REG_IP, &ip);
		unw_get_reg(&cursor, UNW_REG_SP, &sp);
		get_file_and_line((long)ip, file, 256, &line);

		// Add function call to stack
		used += sprintf(buf + used, "%20s:%d %s() at ip=%lx/sp=%lx\n",
			file + offset, line, name, (long) ip, (long) sp);

		// Skip rest if main is reached in the stack.
		if (strcmp(name, prog) == 0) {
			used += sprintf(buf + used, "=== "); 	// No new line needed
			break;
		}

	}	

	elog(LOG, buf);
}
