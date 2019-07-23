/*-------------------------------------------------------------------------
 *
 * smode.h
 *	  POSTGRES scan mode definitions.
 *
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/smode.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef SMODE_H
#define SMODE_H

typedef enum ScanMode
{
	ScanModeAll = 0,
	ScanModeMin = 1,
	ScanModeMax = 2,
	ScanModeMinMax = 3
} ScanMode;

#define ScanModeIncludesMin(mode) \
	((bool) (ScanModeMin & (mode)))

#define ScanModeIncludesMax(mode) \
	((bool) (ScanModeMax & (mode)))

#define ScanModeReverse(mode) \
	((bool) (ScanModeMin == (mode) ? ScanModeMax : ScanModeMin))

#endif							/* SMODE_H */
