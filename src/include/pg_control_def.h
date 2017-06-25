#ifndef PG_CONTROL_DEF_H
#define PG_CONTROL_DEF_H

#define KB                      1024
#define MB                      (1024 * 1024)
#define GB                      (1024 * 1024 * 1024)
#define TB                      (1024 * 1024 * 1024 * 1024)

#define REL_BLCK_SIZE_MIN       1024            /* in bytes */
#define REL_BLCK_SIZE_DEF       8192            /* in bytes */
#define REL_BLCK_SIZE_MAX       32768           /* in bytes */

#define REL_FILE_SIZE_MIN       GB              /* in bytes */
#define REL_FILE_SIZE_DEF       GB              /* in bytes */
#define REL_FILE_SIZE_MAX       2 * GB		/* in bytes */

/* Below are based on above 2 series of block size and segment size */
#define REL_FILE_BLCK_MIN	32768		/* in blocks */
#define REL_FILE_BLCK_DEF	131072		/* in blocks */
#define REL_FILE_BLCK_MAX	4194304		/* in blocks */


#define WAL_BLCK_SIZE_MIN       1024            /* in bytes */
#define WAL_BLCK_SIZE_DEF       8192            /* in bytes */
#define WAL_BLCK_SIZE_MAX       65536           /* in bytes */

#define WAL_FILE_SIZE_MIN       MB              /* in bytes */
#define WAL_FILE_SIZE_DEF       (16 * MB)       /* in bytes */
#define WAL_FILE_SIZE_MAX       GB       	/* in bytes */

/* Below are based on above 2 series of block size and segment size */
#define WAL_FILE_BLCK_MIN	16		/* in blocks */
#define WAL_FILE_BLCK_DEF	2048		/* in blocks */
#define WAL_FILE_BLCK_MAX	1048576		/* in blocks */

#endif
