/*-------------------------------------------------------------------------
 *
 * progress.h
 *	  Progress of query: PROGRESS
 *
 * Portions Copyright (c) 1996-2011, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/commands/progress.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef PROGRESS_H
#define PROGRESS_H

/*
 * This is arbitratry defined
 * TODO: Add a guc variable to enable dynamic definition
 */
#define PROGRESS_AREA_SIZE	(4096 * 128)

/*
 * Track when a progress report has been requested 
 */
extern volatile bool progress_requested;

/*
 * global parameters in local backend memory
 */
extern StringInfo progress_str;
extern ReportState* progress_state;

/*
 * Init and Fini functions
 */
extern size_t ProgressShmemSize(void);
extern void ProgressShmemInit(void);

/* 
 * external functions
 */
extern void ProgressSendRequest(int pid, int verbose, char* buf);
extern void HandleProgressSignal(void);
extern void HandleProgressRequest(void);

#endif /* PROGRESS_H */
