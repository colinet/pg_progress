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
 * Functions to setup share memory data structures
 */
extern size_t ProgressShmemSize(void);
extern void ProgressShmemInit(void);

/*
 * Report only SQL querries which have been running longer than this value.
 */
extern int progress_time_threshold;

/*
 * Has a progress report been requested
 */
extern volatile bool progress_requested;

/* 
 * Functions to deal with signals sent by the monitoring 
 * backend to the monitored backend
 */
extern void HandleProgressSignal(void);
extern void HandleProgressRequest(void);

extern HeapScanDesc CreateIndexHeapScan;

extern PlannedStmt* MyUtilityPlannedStmt;
extern char* MyUtilityQueryString;

#endif /* PROGRESS_H */
