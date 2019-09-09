/*-------------------------------------------------------------------------
 *
 * nbtskip.c
 *	  Search code related to skip scan for postgres btrees.
 *
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/nbtree/nbtskip.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/nbtree.h"
#include "access/relscan.h"
#include "catalog/catalog.h"
#include "miscadmin.h"
#include "utils/guc.h"
#include "storage/predicate.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"

static inline void _bt_update_scankey_with_tuple(BTScanInsert scankeys,
											Relation indexRel, IndexTuple itup, int numattrs);
static inline bool _bt_scankey_within_page(IndexScanDesc scan, BTScanInsert key,
										Buffer buf, ScanDirection dir);
static bool _bt_readnextpage_one(IndexScanDesc scan, BlockNumber blkno, ScanDirection dir);
static bool _bt_steppage_one(IndexScanDesc scan, ScanDirection dir);
static bool _bt_readpage_one(IndexScanDesc scan, ScanDirection dir, OffsetNumber offnum);
static inline int32 _bt_compare_until(Relation rel, BTScanInsert key, IndexTuple itup, int prefix);
static inline void _bt_copy_scankey(BTScanInsert to, BTScanInsert from, int numattrs);

/* just a debug function that prints a scankey. will be removed for final patch */
static inline void _print_skey(IndexScanDesc scan, BTScanInsert scanKey)
{
	Oid			typOutput;
	bool		varlenatype;
	char	   *val;
	int i;
	Relation rel = scan->indexRelation;

	for (i = 0; i < scanKey->keysz; i++)
	{
		ScanKey cur = &scanKey->scankeys[i];
		if (!IsCatalogRelation(rel))
		{
			if (!(cur->sk_flags & SK_ISNULL))
			{
				if (cur->sk_subtype != InvalidOid)
					getTypeOutputInfo(cur->sk_subtype,
									  &typOutput, &varlenatype);
				else
					getTypeOutputInfo(rel->rd_opcintype[i],
									  &typOutput, &varlenatype);
				val = OidOutputFunctionCall(typOutput, cur->sk_argument);
				if (val)
				{
					elog(DEBUG1, "%s sk attr %d val: %s (%s, %s)",
						 RelationGetRelationName(rel), i, val,
						 (cur->sk_flags & SK_BT_NULLS_FIRST) != 0 ? "NULLS FIRST" : "NULLS LAST",
						 (cur->sk_flags & SK_BT_DESC) != 0 ? "DESC" : "ASC");
					pfree(val);
				}
			}
			else
			{
				elog(DEBUG1, "%s sk attr %d val: NULL (%s, %s)",
					 RelationGetRelationName(rel), i,
					 (cur->sk_flags & SK_BT_NULLS_FIRST) != 0 ? "NULLS FIRST" : "NULLS LAST",
					 (cur->sk_flags & SK_BT_DESC) != 0 ? "DESC" : "ASC");
			}
		}
	}
}

/*
 * Compare a scankey with a given tuple but only the first prefix columns
 * This function returns 0 if the first 'prefix' columns are equal
 * -1 if key < itup for the first prefix columns
 * 1 if key > itup for the first prefix columns
 */
int32
_bt_compare_until(Relation rel,
			BTScanInsert key,
			IndexTuple itup,
			int prefix)
{
	TupleDesc	itupdesc = RelationGetDescr(rel);
	ScanKey		scankey;
	int			ncmpkey;

	Assert(key->keysz <= IndexRelationGetNumberOfKeyAttributes(rel));

	ncmpkey = Min(prefix, key->keysz);
	scankey = key->scankeys;
	for (int i = 1; i <= ncmpkey; i++)
	{
		Datum		datum;
		bool		isNull;
		int32		result;

		datum = index_getattr(itup, scankey->sk_attno, itupdesc, &isNull);

		/* see comments about NULLs handling in btbuild */
		if (scankey->sk_flags & SK_ISNULL)	/* key is NULL */
		{
			if (isNull)
				result = 0;		/* NULL "=" NULL */
			else if (scankey->sk_flags & SK_BT_NULLS_FIRST)
				result = -1;	/* NULL "<" NOT_NULL */
			else
				result = 1;		/* NULL ">" NOT_NULL */
		}
		else if (isNull)		/* key is NOT_NULL and item is NULL */
		{
			if (scankey->sk_flags & SK_BT_NULLS_FIRST)
				result = 1;		/* NOT_NULL ">" NULL */
			else
				result = -1;	/* NOT_NULL "<" NULL */
		}
		else
		{
			/*
			 * The sk_func needs to be passed the index value as left arg and
			 * the sk_argument as right arg (they might be of different
			 * types).  Since it is convenient for callers to think of
			 * _bt_compare as comparing the scankey to the index item, we have
			 * to flip the sign of the comparison result.  (Unless it's a DESC
			 * column, in which case we *don't* flip the sign.)
			 */
			result = DatumGetInt32(FunctionCall2Coll(&scankey->sk_func,
													 scankey->sk_collation,
													 datum,
													 scankey->sk_argument));

			if (!(scankey->sk_flags & SK_BT_DESC))
				INVERT_COMPARE_RESULT(result);
		}

		/* if the keys are unequal, return the difference */
		if (result != 0)
			return result;

		scankey++;
	}
	return 0;
}


/*
 * Create initial scankeys for skipping and stores them in the skipData
 * structure
 */
void _bt_skip_create_scankeys(Relation rel, BTScanOpaque so)
{
	int keysCount;
	BTSkip skip = so->skipData;
	StrategyNumber stratTotal;
	ScanKey		keyPointers[INDEX_MAX_KEYS];
	bool goback;
	/* we need to create both forward and backward keys because the scan direction
	 * may change at any moment in scans with a cursor.
	 * we could technically delay creation of the second until first use as an optimization
	 * but that is not implemented yet.
	 */
	keysCount = _bt_choose_scan_keys(so->keyData, so->numberOfKeys, ForwardScanDirection, keyPointers, skip->fwdNotNullKeys, &stratTotal, skip->prefix);
	_bt_create_insertion_scan_key(rel, ForwardScanDirection, keyPointers, keysCount, &skip->fwdScanKey, &stratTotal, &goback);

	keysCount = _bt_choose_scan_keys(so->keyData, so->numberOfKeys, BackwardScanDirection, keyPointers, skip->bwdNotNullKeys, &stratTotal, skip->prefix);
	_bt_create_insertion_scan_key(rel, BackwardScanDirection, keyPointers, keysCount, &skip->bwdScanKey, &stratTotal, &goback);

	skip->skipScanKey.heapkeyspace = _bt_heapkeyspace(rel);
	skip->skipScanKey.anynullkeys = false; /* unused */
	skip->skipScanKey.nextkey = false;
	skip->skipScanKey.pivotsearch = false;
	skip->skipScanKey.scantid = NULL;
	skip->skipScanKey.keysz = 0;

	/* setup scankey for the current tuple as well. it's not necessarily that
	 * we will use the data from the current tuple already,
	 * but we need the rest of the data structure to be set up correctly
	 * for when we use it to create skip->skipScanKey keys later
	 */
	_bt_mkscankey(rel, NULL, &skip->currentTupleKey);

	/* determine whether extra quals are available after skipping to a prefix
	 * for example: select * from tbl where c=1000, and an index on (a,b,c)
	 * if we are skipping over prefix=2 (a,b)
	 * after we skip to the first item of a certain prefix, it turns out
	 * we can now use the c=1000 condition to directly locate the element
	 * we need. extraConditionsPossible will be true if it's possible to
	 * use extra quals after skipping to the next prefix.
	 */
	skip->extraConditionsPossibleFwd = _bt_has_extra_quals_after_skip(
				so->skipData, ForwardScanDirection, keysCount);
	skip->extraConditionsPossibleBwd = _bt_has_extra_quals_after_skip(
				so->skipData, BackwardScanDirection, keysCount);
}

/*
 * _bt_scankey_within_page() -- check if the provided scankey could be found
 * 								within a page, specified by the buffer.
 */
static inline bool
_bt_scankey_within_page(IndexScanDesc scan, BTScanInsert key,
						Buffer buf, ScanDirection dir)
{
	OffsetNumber low, high, compare_offset;
	Page page = BufferGetPage(buf);
	BTPageOpaque opaque = (BTPageOpaque) PageGetSpecialPointer(page);
	int			ans;

	low = P_FIRSTDATAKEY(opaque);
	high = PageGetMaxOffsetNumber(page);
	compare_offset = ScanDirectionIsForward(dir) ? high : low;

	ans = _bt_compare(scan->indexRelation,
					   key, page, compare_offset);
	if (ScanDirectionIsForward(dir))
	{
		if (key->nextkey)
		{
			return ans == -1; // sk < last tuple
		}
		else
		{
			return ans <= 0; // sk <= last tuple
		}
	}
	else
	{
		if (key->nextkey)
		{
			return ans >= 0; // first tuple <= sk
		}
		else
		{
			return ans == 1; // first tuple < sk
		}
	}
}

/* in: pinned and locked, out: pinned and locked */
static inline void _bt_skip_find(IndexScanDesc scan, BTScanInsert scanKey, ScanDirection dir)
{
	BTScanOpaque 	so = (BTScanOpaque) scan->opaque;
	OffsetNumber offnum;
	BTStack stack;
	Buffer buf;
	bool goback;
	Page		page;
	BTPageOpaque opaque;
	OffsetNumber minoff;
	Relation rel = scan->indexRelation;

	_bt_set_bsearch_flags(scanKey->scankeys[scanKey->keysz - 1].sk_strategy, dir, &scanKey->nextkey, &goback);

	if ((DEBUG1 >= log_min_messages || DEBUG1 >= client_min_messages) && !IsCatalogRelation(rel))
	{
		if (DEBUG1 >= log_min_messages || DEBUG1 >= client_min_messages)
			print_itup(BufferGetBlockNumber(so->currPos.buf), _bt_get_current_tuple(scan), NULL, rel,
						"before btree search");

		elog(DEBUG1, "%s searching tree with %d keys, nextkey=%d, goback=%d",
			 RelationGetRelationName(rel), scanKey->keysz, scanKey->nextkey,
			 goback);

		_print_skey(scan, scanKey);
	}

	/* Check if the next unique key can be found within the current page */
	if (BTScanPosIsValid(so->currPos) &&
		_bt_scankey_within_page(scan, scanKey, so->currPos.buf, so->skipData->overallDir))
	{
		//LockBuffer(so->currPos.buf, BT_READ);
		offnum = _bt_binsrch(scan->indexRelation, scanKey, so->currPos.buf);
	}
	else
	{
		if (BTScanPosIsValid(so->currPos))
		{
			LockBuffer(so->currPos.buf, BUFFER_LOCK_UNLOCK);
			ReleaseBuffer(so->currPos.buf);
			so->currPos.buf = InvalidBuffer;
		}

		/*
		 * We haven't found scan key within the current page, so let's scan from
		 * the root. Use _bt_search and _bt_binsrch to get the buffer and offset
		 * number
		 */
		stack = _bt_search(scan->indexRelation, scanKey,
						   &buf, BT_READ, scan->xs_snapshot);
		_bt_freestack(stack);
		so->currPos.buf = buf;
		offnum = _bt_binsrch(scan->indexRelation, scanKey, buf);
	}

	page = BufferGetPage(so->currPos.buf);
	opaque = (BTPageOpaque) PageGetSpecialPointer(page);

	if (goback)
	{
		offnum = OffsetNumberPrev(offnum);
		minoff = P_FIRSTDATAKEY(opaque);
		if (offnum < minoff)
		{
			LockBuffer(so->currPos.buf, BUFFER_LOCK_UNLOCK);
			if (!_bt_step_back_page(scan))
				return;
			offnum = so->skipData->indexOffset;
		}
	}
	else if (offnum > PageGetMaxOffsetNumber(page))
	{
		BlockNumber next = opaque->btpo_next;
		LockBuffer(so->currPos.buf, BUFFER_LOCK_UNLOCK);
		if (!_bt_step_forward_page(scan, next))
			return;
		offnum = so->skipData->indexOffset;
	}

	/* Lock the page for SERIALIZABLE transactions */
	PredicateLockPage(scan->indexRelation, BufferGetBlockNumber(so->currPos.buf),
					  scan->xs_snapshot);

	/* We know in which direction to look */
	_bt_initialize_more_data(so, dir);

	so->skipData->indexOffset = offnum;
	so->currPos.currPage = BufferGetBlockNumber(so->currPos.buf);

	if (DEBUG1 >= log_min_messages || DEBUG1 >= client_min_messages)
		print_itup(BufferGetBlockNumber(so->currPos.buf), _bt_get_current_tuple(scan), NULL, rel,
					"after btree search");
}

/* in: possibly pinned, but unlocked, out: pinned and locked */
bool _bt_step_forward_page(IndexScanDesc scan, BlockNumber next)
{
	BTScanOpaque so = (BTScanOpaque) scan->opaque;
	Relation rel = scan->indexRelation;
	BlockNumber blkno = next;
	Page page;
	BTPageOpaque opaque;

	Assert(BTScanPosIsValid(so->currPos));

	/* Before leaving current page, deal with any killed items */
	if (so->numKilled > 0)
		_bt_killitems(scan);

	/*
	 * Before we modify currPos, make a copy of the page data if there was a
	 * mark position that needs it.
	 */
	if (so->markItemIndex >= 0)
	{
		/* bump pin on current buffer for assignment to mark buffer */
		if (BTScanPosIsPinned(so->currPos))
			IncrBufferRefCount(so->currPos.buf);
		memcpy(&so->markPos, &so->currPos,
			   offsetof(BTScanPosData, items[1]) +
			   so->currPos.lastItem * sizeof(BTScanPosItem));
		if (so->markTuples)
			memcpy(so->markTuples, so->currTuples,
				   so->currPos.nextTupleOffset);
		so->markPos.itemIndex = so->markItemIndex;
		so->markItemIndex = -1;
	}

	/* Remember we left a page with data */
	so->currPos.moreLeft = true;

	/* release the previous buffer, if pinned */
	BTScanPosUnpinIfPinned(so->currPos);

	{
		for (;;)
		{
			/*
			 * if we're at end of scan, give up and mark parallel scan as
			 * done, so that all the workers can finish their scan
			 */
			if (blkno == P_NONE)
			{
				_bt_parallel_done(scan);
				BTScanPosInvalidate(so->currPos);
				return false;
			}

			/* check for interrupts while we're not holding any buffer lock */
			CHECK_FOR_INTERRUPTS();
			/* step right one page */
			so->currPos.buf = _bt_getbuf(rel, blkno, BT_READ);
			page = BufferGetPage(so->currPos.buf);
			TestForOldSnapshot(scan->xs_snapshot, rel, page);
			opaque = (BTPageOpaque) PageGetSpecialPointer(page);
			/* check for deleted page */
			if (!P_IGNORE(opaque))
			{
				PredicateLockPage(rel, blkno, scan->xs_snapshot);
				so->skipData->indexOffset = P_FIRSTDATAKEY(opaque);
				//so->skipData->indexOffset = PageGetMaxOffsetNumber(page);
				break;
			}

			blkno = opaque->btpo_next;
			_bt_relbuf(rel, so->currPos.buf);
		}
	}

	return true;
}

/* in: possibly pinned, but unlocked, out: pinned and locked */
bool _bt_step_back_page(IndexScanDesc scan)
{
	BTScanOpaque so = (BTScanOpaque) scan->opaque;

	Assert(BTScanPosIsValid(so->currPos));

	/* Before leaving current page, deal with any killed items */
	if (so->numKilled > 0)
		_bt_killitems(scan);

	/*
	 * Before we modify currPos, make a copy of the page data if there was a
	 * mark position that needs it.
	 */
	if (so->markItemIndex >= 0)
	{
		/* bump pin on current buffer for assignment to mark buffer */
		if (BTScanPosIsPinned(so->currPos))
			IncrBufferRefCount(so->currPos.buf);
		memcpy(&so->markPos, &so->currPos,
			   offsetof(BTScanPosData, items[1]) +
			   so->currPos.lastItem * sizeof(BTScanPosItem));
		if (so->markTuples)
			memcpy(so->markTuples, so->currTuples,
				   so->currPos.nextTupleOffset);
		so->markPos.itemIndex = so->markItemIndex;
		so->markItemIndex = -1;
	}

	/* Remember we left a page with data */
	so->currPos.moreRight = true;

	/* Not parallel, so just use our own notion of the current page */

	{
		Relation	rel;
		Page		page;
		BTPageOpaque opaque;

		rel = scan->indexRelation;

		if (BTScanPosIsPinned(so->currPos))
			LockBuffer(so->currPos.buf, BT_READ);
		else
			so->currPos.buf = _bt_getbuf(rel, so->currPos.currPage, BT_READ);

		for (;;)
		{
			/* Step to next physical page */
			so->currPos.buf = _bt_walk_left(rel, so->currPos.buf,
											scan->xs_snapshot);

			/* if we're physically at end of index, return failure */
			if (so->currPos.buf == InvalidBuffer)
			{
				BTScanPosInvalidate(so->currPos);
				return false;
			}

			/*
			 * Okay, we managed to move left to a non-deleted page. Done if
			 * it's not half-dead and contains matching tuples. Else loop back
			 * and do it all again.
			 */
			page = BufferGetPage(so->currPos.buf);
			TestForOldSnapshot(scan->xs_snapshot, rel, page);
			opaque = (BTPageOpaque) PageGetSpecialPointer(page);
			if (!P_IGNORE(opaque))
			{
				PredicateLockPage(rel, BufferGetBlockNumber(so->currPos.buf), scan->xs_snapshot);
				so->skipData->indexOffset = PageGetMaxOffsetNumber(page);
				break;
			}
		}
	}

	return true;
}

bool _bt_skip_find_next(IndexScanDesc scan)
{
	BTScanOpaque 	so = (BTScanOpaque) scan->opaque;
	BTSkip skip = so->skipData;
	bool regularMode = _bt_skip_is_regular_mode(skip->overallDir, skip->curDir);
	if ((regularMode || skip->compareResult.prefixCmpResult == 0) && skip->compareResult.equal)
	{
		if (skip->compareResult.prefixCmpResult != 0)
		{
			_bt_update_scankey_for_prefix_skip(scan, scan->indexRelation, true, _bt_get_current_tuple(scan));
			skip->compareResult.prefixCmpResult = 0;
			skip->compareResult.prefixSkip = false;
			skip->compareResult.prefixSkipIndex = skip->prefix;
		}
		_bt_readpage_one(scan, so->skipData->curDir, so->skipData->indexOffset);
		return true;
	}
	while (_bt_skip_is_valid(so))
	{
		bool found;
		_bt_skip_until_match(scan);

		found = _bt_readpage_one(scan, skip->curDir, skip->indexOffset);
		if (found)
		{
			return true;
		}
		if (regularMode && !_bt_skip_is_always_valid(so))
		{
			return false;
		}
		if (!skip->compareResult.prefixSkip)
		{
			LockBuffer(so->currPos.buf, BUFFER_LOCK_UNLOCK);
			found = _bt_steppage_one(scan, skip->overallDir);
			if (found)
			{
				return true;
			}
			if (regularMode && !_bt_skip_is_always_valid(so))
			{
				return false;
			}
		}

		if (regularMode && _bt_skip_is_always_valid(so))
		{
			_bt_skip_update_scankey_for_extra_skip(scan, scan->indexRelation, skip->curDir, false, _bt_get_current_tuple(scan));
			_bt_compare_current_item(scan, _bt_get_current_tuple(scan), IndexRelationGetNumberOfAttributes(scan->indexRelation), skip->curDir);
			if (skip->compareResult.fullKeySkip)
			{
				return false;
			}
		}
	}
	return false;
}

void _bt_skip_until_match(IndexScanDesc scan)
{
	BTScanOpaque 	so = (BTScanOpaque) scan->opaque;
	BTSkip skip = so->skipData;
	while (_bt_skip_is_valid(so) && skip->compareResult.prefixSkip)
	{
		_bt_skip_once(scan, NULL);
	}
}

void _bt_compare_current_item(IndexScanDesc scan, IndexTuple tuple, int tupnatts, ScanDirection dir)
{
	BTScanOpaque 	so = (BTScanOpaque) scan->opaque;
	BTSkip skip = so->skipData;

	if (_bt_skip_is_always_valid(so))
	{
		bool continuescan = true;

		skip->compareResult.equal = _bt_checkkeys(scan, tuple, tupnatts, dir, &continuescan, &skip->compareResult.prefixSkipIndex);
		skip->compareResult.fullKeySkip = !continuescan;
		/* prefix can be smaller than scankey due to extra quals being added
		 * therefore we need to compare both. @todo this can be optimized into one function call */
		skip->compareResult.prefixCmpResult = _bt_compare_until(scan->indexRelation, &skip->skipScanKey, tuple, skip->prefix);
		skip->compareResult.skCmpResult = _bt_compare_until(scan->indexRelation, &skip->skipScanKey, tuple, skip->skipScanKey.keysz);
		if (skip->compareResult.prefixSkipIndex == -1)
		{
			skip->compareResult.prefixSkipIndex = skip->prefix;
			skip->compareResult.prefixSkip = skip->compareResult.prefixCmpResult != 0;
		}
		else
		{
			int newskip = -1;
			_bt_checkkeys_threeway(scan, tuple, tupnatts, dir, &continuescan, &newskip);
			if (newskip != -1)
			{
				skip->compareResult.prefixSkip = true;
				skip->compareResult.prefixSkipIndex = newskip;
			}
			else
			{
				skip->compareResult.prefixSkip = skip->compareResult.prefixCmpResult != 0;
				skip->compareResult.prefixSkipIndex = skip->prefix;
			}
		}

		if (DEBUG1 >= log_min_messages || DEBUG1 >= client_min_messages)
		{
			print_itup(BufferGetBlockNumber(so->currPos.buf), tuple, NULL, scan->indexRelation,
						"compare item");
			_print_skey(scan, &skip->skipScanKey);
			elog(DEBUG1, "result: eq: %d fkskip: %d pfxskip: %d prefixcmpres: %d prefixskipidx: %d", skip->compareResult.equal, skip->compareResult.fullKeySkip,
				 skip->compareResult.prefixSkip, skip->compareResult.prefixCmpResult, skip->compareResult.prefixSkipIndex);
		}
	}
	else if (!_bt_skip_is_regular_mode(skip->overallDir, dir))
	{
		skip->compareResult.fullKeySkip = false;
		skip->compareResult.equal = false;
		skip->compareResult.prefixCmpResult = -2;
		skip->compareResult.prefixSkip = true;
		skip->compareResult.prefixSkipIndex = skip->prefix;
		skip->compareResult.skCmpResult = -2;
	}
	else
	{
		skip->compareResult.fullKeySkip = true;
	}
}

void _bt_skip_once(IndexScanDesc scan, IndexTuple initialSkip)
{
	BTScanOpaque 	so = (BTScanOpaque) scan->opaque;
	BTSkip skip = so->skipData;
	bool regularMode = _bt_skip_is_regular_mode(skip->overallDir, skip->curDir);
	bool doskip = skip->compareResult.prefixCmpResult == 0 || !regularMode;
	while (doskip)
	{
		if (initialSkip != NULL)
		{
			_bt_update_scankey_for_prefix_skip(scan, scan->indexRelation, regularMode, initialSkip);
			initialSkip = NULL;
		}
		else
		{
			_bt_update_scankey_for_prefix_skip(scan, scan->indexRelation, regularMode, NULL);
		}

		_bt_skip_find(scan, &skip->skipScanKey, skip->overallDir);

		if (_bt_skip_is_always_valid(so))
		{
			_bt_skip_update_scankey_for_extra_skip(scan, scan->indexRelation, skip->overallDir, true, _bt_get_current_tuple(scan));
			_bt_compare_current_item(scan, _bt_get_current_tuple(scan), IndexRelationGetNumberOfAttributes(scan->indexRelation), skip->overallDir);
			if (skip->compareResult.fullKeySkip)
			{
				return;
			}
			if (skip->compareResult.equal && regularMode)
			{
				return;
			}
			doskip = skip->compareResult.prefixSkip && ((ScanDirectionIsForward(skip->overallDir) && skip->compareResult.skCmpResult == 1)
														|| (ScanDirectionIsBackward(skip->overallDir) && skip->compareResult.skCmpResult == -1)) && regularMode;
		}
		else
		{
			skip->compareResult.fullKeySkip = true;
			return;
		}
	}
	_bt_skip_extra_conditions(scan, NULL);
}

void _bt_skip_extra_conditions(IndexScanDesc scan, IndexTuple initialSkip)
{
	BTScanOpaque 	so = (BTScanOpaque) scan->opaque;
	BTSkip skip = so->skipData;
	bool regularMode = _bt_skip_is_regular_mode(skip->overallDir, skip->curDir);
	bool extraPossible = ScanDirectionIsForward(skip->curDir) ? skip->extraConditionsPossibleFwd : skip->extraConditionsPossibleBwd;
	if (_bt_skip_is_always_valid(so) && (extraPossible || !regularMode))
	{
		do
		{
			if (initialSkip)
			{
				_bt_skip_update_scankey_for_extra_skip(scan, scan->indexRelation, skip->curDir, false, initialSkip);
				initialSkip = NULL;
			}
			else
			{
				_bt_skip_update_scankey_for_extra_skip(scan, scan->indexRelation, skip->curDir, false, _bt_get_current_tuple(scan));
			}
			_bt_skip_find(scan, &skip->skipScanKey, skip->curDir);
			_bt_compare_current_item(scan, _bt_get_current_tuple(scan), IndexRelationGetNumberOfAttributes(scan->indexRelation), skip->curDir);
			if (skip->compareResult.fullKeySkip && regularMode)
			{
				return;
			}
		} while (regularMode && skip->compareResult.prefixCmpResult != 0 && !skip->compareResult.equal);
	}
	else if (_bt_skip_is_always_valid(so) && skip->compareResult.prefixCmpResult != 0)
	{
		_bt_update_scankey_for_prefix_skip(scan, scan->indexRelation, true, initialSkip ? initialSkip : _bt_get_current_tuple(scan));
		_bt_compare_current_item(scan, _bt_get_current_tuple(scan), IndexRelationGetNumberOfAttributes(scan->indexRelation), skip->curDir);
		if (skip->compareResult.fullKeySkip)
		{
			return;
		}
	}
}

static inline int _bt_compare_one(ScanKey scankey, Datum datum2, bool isNull2)
{
	int32		result;
	Datum datum1 = scankey->sk_argument;
	bool isNull1 = scankey->sk_flags & SK_ISNULL;
	/* see comments about NULLs handling in btbuild */
	if (isNull1)	/* key is NULL */
	{
		if (isNull2)
			result = 0;		/* NULL "=" NULL */
		else if (scankey->sk_flags & SK_BT_NULLS_FIRST)
			result = -1;	/* NULL "<" NOT_NULL */
		else
			result = 1;		/* NULL ">" NOT_NULL */
	}
	else if (isNull2)		/* key is NOT_NULL and item is NULL */
	{
		if (scankey->sk_flags & SK_BT_NULLS_FIRST)
			result = 1;		/* NOT_NULL ">" NULL */
		else
			result = -1;	/* NOT_NULL "<" NULL */
	}
	else
	{
		/*
		 * The sk_func needs to be passed the index value as left arg and
		 * the sk_argument as right arg (they might be of different
		 * types).  Since it is convenient for callers to think of
		 * _bt_compare as comparing the scankey to the index item, we have
		 * to flip the sign of the comparison result.  (Unless it's a DESC
		 * column, in which case we *don't* flip the sign.)
		 */
		result = DatumGetInt32(FunctionCall2Coll(&scankey->sk_func,
												 scankey->sk_collation,
												 datum2,
												 datum1));

		if (!(scankey->sk_flags & SK_BT_DESC))
			INVERT_COMPARE_RESULT(result);
	}
	return result;
}

/*
 * set up new values for the existing scankeys
 * based on the current index tuple
 */
static inline void
_bt_update_scankey_with_tuple(BTScanInsert insertKey, Relation indexRel, IndexTuple itup, int numattrs)
{
	TupleDesc		itupdesc;
	int				i;
	ScanKey			scankeys = insertKey->scankeys;

	insertKey->keysz = numattrs;
	itupdesc = RelationGetDescr(indexRel);
	for (i = 0; i < numattrs; i++)
	{
		Datum datum;
		bool null;
		int flags;

		datum = index_getattr(itup, i + 1, itupdesc, &null);
		flags = (null ? SK_ISNULL : 0) |
				(indexRel->rd_indoption[i] << SK_BT_INDOPTION_SHIFT);
		scankeys[i].sk_flags = flags;
		scankeys[i].sk_argument = datum;
	}
}

/* copy the elements important to a skip from one insertion sk to another */
void _bt_copy_scankey(BTScanInsert to, BTScanInsert from, int numattrs)
{
	memcpy(to->scankeys, from->scankeys, sizeof(ScanKeyData) * (unsigned long)numattrs);
	to->nextkey = from->nextkey;
	to->keysz = numattrs;
}

/*
 * Updates the existing scankey for skipping to the next prefix
 * alwaysUsePrefix determines how many attrs the scankey will have
 * when true, it will always have skip->prefix number of attributes,
 * otherwise, the value can be less, which will be determined by the comparison
 * result with the current tuple.
 * for example, a SELECT * FROM tbl WHERE b<2, index (a,b,c) and when skipping with prefix size=2
 * if we encounter the tuple (1,3,1) - this does not match the qual b<2. however, we also know that
 * it is not useful to skip to any next qual with prefix=2 (eg. (1,4)), because that will definitely not
 * match either. However, we do want to skip to eg. (2,0). Therefore, we skip over prefix=1 in this case.
 *
 * the provided itup may be null. this happens when we don't want to use the current tuple to update
 * the scankey, but instead want to use the existing skipScanKey to fill currentTupleKey. this accounts
 * for some edge cases.
 */
void _bt_update_scankey_for_prefix_skip(IndexScanDesc scan, Relation indexRel,
										bool alwaysUsePrefix, IndexTuple itup)
{
	BTScanOpaque so = (BTScanOpaque) scan->opaque;
	BTSkip skip = so->skipData;
	/* we use skip->prefix is alwaysUsePrefix is set or if skip->prefix is smaller than whatever the
	 * comparison result provided, such that we never skip more than skip->prefix
	 */
	int numattrs = (alwaysUsePrefix || skip->prefix <= skip->compareResult.prefixSkipIndex) ?
				skip->prefix :
				skip->compareResult.prefixSkipIndex;

	if (itup != NULL)
	{
		_bt_update_scankey_with_tuple(&skip->currentTupleKey, indexRel, itup, numattrs);
		_bt_copy_scankey(&skip->skipScanKey, &skip->currentTupleKey, numattrs);
	}
	else
	{
		skip->skipScanKey.keysz = numattrs;
		_bt_copy_scankey(&skip->currentTupleKey, &skip->skipScanKey, numattrs);
	}
	/* update strategy for last attribute as we will use this to determine the rest of the
	 * rest of the flags (goback) when doing the actual tree search
	 */
	skip->currentTupleKey.scankeys[numattrs - 1].sk_strategy =
			skip->skipScanKey.scankeys[numattrs - 1].sk_strategy =
			ScanDirectionIsForward(skip->overallDir) ? BTGreaterStrategyNumber : BTLessStrategyNumber;
}

/* update the scankey for skipping the 'extra' conditions, opportunities
 * that arise when we have just skipped to a new prefix and can try to skip
 * within the prefix to the right tuple by using extra quals when available
 *
 * @todo as an optimization it should be possible to optimize calls to this function
 * and to _bt_update_scankey_for_prefix_skip to some more specific functions that
 * will need to do less copying of data.
 */
void _bt_skip_update_scankey_for_extra_skip(IndexScanDesc scan, Relation indexRel, ScanDirection curDir, bool prioritizeEqual, IndexTuple itup)
{
	BTScanOpaque 	so = (BTScanOpaque) scan->opaque;
	BTSkip skip = so->skipData;
	BTScanInsert toCopy;
	int i, left;

	/* first make sure that currentTupleKey is correct at all times */
	_bt_update_scankey_for_prefix_skip(scan, indexRel, true, itup);
	/* then do the actual work to setup skipScanKey - distinguish between work that depends on overallDir
	 * (those attributes between attribute number 1 and 'prefix' inclusive)
	 * and work that depends on curDir
	 * (those attributes between attribute number 'prefix' + 1 and fwdScanKey.keysz inclusive)
	 */
	if (ScanDirectionIsForward(skip->overallDir))
	{
		/*
		 * if overallDir is Forward, we need to choose between fwdScanKey or
		 * currentTupleKey. we need to choose the most restrictive one -
		 * in most cases this means choosing eg. a>5 over a=2 when scanning forward,
		 * unless prioritizeEqual is set. this is done for certain special cases
		 */
		for (i = 0; i < skip->prefix; i++)
		{
			ScanKey scankey = &skip->fwdScanKey.scankeys[i];
			ScanKey scankeyItem = &skip->currentTupleKey.scankeys[i];
			if (scankey->sk_attno != 0 && (_bt_compare_one(scankey, scankeyItem->sk_argument, scankeyItem->sk_flags & SK_ISNULL) > 0
										   || (prioritizeEqual && scankey->sk_strategy == BTEqualStrategyNumber)))
			{
				memcpy(skip->skipScanKey.scankeys + i, scankey, sizeof(ScanKeyData));
			}
			else
			{
				memcpy(skip->skipScanKey.scankeys + i, scankeyItem, sizeof(ScanKeyData));
			}
			/* for now choose equal here - it could actually be improved a bit @todo by choosing the strategy
			 * from the scankeys, but it doesn't matter a lot
			 */
			skip->skipScanKey.scankeys[i].sk_strategy = BTEqualStrategyNumber;
		}
	}
	else
	{
		/* similar for backward but in opposite direction */
		for (i = 0; i < skip->prefix; i++)
		{
			ScanKey scankey = &skip->bwdScanKey.scankeys[i];
			ScanKey scankeyItem = &skip->currentTupleKey.scankeys[i];
			if (scankey->sk_attno != 0 && (_bt_compare_one(scankey, scankeyItem->sk_argument, scankeyItem->sk_flags & SK_ISNULL) < 0
										   || (prioritizeEqual && scankey->sk_strategy == BTEqualStrategyNumber)))
			{
				memcpy(skip->skipScanKey.scankeys + i, scankey, sizeof(ScanKeyData));
			}
			else
			{
				memcpy(skip->skipScanKey.scankeys + i, scankeyItem, sizeof(ScanKeyData));
			}
			skip->skipScanKey.scankeys[i].sk_strategy = BTEqualStrategyNumber;
		}
	}

	/*
	 * the remaining keys are the quals after the prefix
	 */
	if (ScanDirectionIsForward(curDir))
		toCopy = &skip->fwdScanKey;
	else
		toCopy = &skip->bwdScanKey;

	left = toCopy->keysz - skip->prefix;
	if (left > 0)
	{
		memcpy(skip->skipScanKey.scankeys + i, toCopy->scankeys + i, sizeof(ScanKeyData) * (unsigned long)left);
	}
	skip->skipScanKey.keysz = toCopy->keysz;
}

/* alias of BTScanPosIsValid */
bool _bt_skip_is_always_valid(BTScanOpaque so)
{
	return BTScanPosIsValid(so->currPos);
}

/*
 * returns whether we're at the end of a scan.
 * the scan position can be invalid even though we still
 * should continue the scan. this happens for example when
 * we're scanning forwards in Max mode. when looking at the first
 * prefix, we traverse the items within the prefix from max to min.
 * if none of them match, we actually run off the start of the index,
 * meaning none of the tuples within this prefix match. the scan pos becomes
 * invalid, however, we do need to look further to the next prefix.
 * therefore, this function still returns true in this particular case.
 */
bool _bt_skip_is_valid(BTScanOpaque so)
{
	return BTScanPosIsValid(so->currPos) ||
			(!_bt_skip_is_regular_mode(so->skipData->overallDir,
				so->skipData->curDir) && !so->skipData->compareResult.fullKeySkip);
}

/* returns whether or not we can use extra quals in the scankey after skipping to a prefix */
bool _bt_has_extra_quals_after_skip(BTSkip skip, ScanDirection dir, int keysCountFirst)
{
	if (ScanDirectionIsForward(dir))
	{
		return skip->fwdScanKey.keysz > keysCountFirst && skip->fwdScanKey.keysz > skip->prefix;
	}
	else
	{
		return skip->bwdScanKey.keysz > keysCountFirst && skip->bwdScanKey.keysz > skip->prefix;
	}
}

/* update the current direction based on the current mode
 * we could probably get rid of this and use curMode directly in code @todo investigate
 */
void _bt_skip_update_curdir(BTSkip skip)
{
	if (skip->curMode == ScanModeMin)
		skip->curDir = ForwardScanDirection;
	else if (skip->curMode == ScanModeMax)
		skip->curDir = BackwardScanDirection;
	else
		elog(ERROR, "should not be reached, invalid ScanMode current %d",
			 skip->curMode);
}

/* regulare mode is defined as the case when overall direction of the scan
 * is equal to the passed scan direction to compare with.
 * this is the case when looking for Min value in a forward scan, or
 * Max value in a backward scan
 */
bool _bt_skip_is_regular_mode(ScanDirection overallDir, ScanDirection dirToCompare)
{
	return overallDir == dirToCompare;
}

/* reverse the current scan mode
 * changes Max to Min and Min to Max if overallMode is MinMax
 * otherwise has no effect
 */
void _bt_skip_reverse_curmode(BTSkip skip)
{
	if (skip->overallMode == ScanModeMinMax)
	{
		skip->curMode = ScanModeReverse(skip->curMode);
	}
	else
	{
		skip->curMode = skip->overallMode;
	}
}

void _bt_skip_init_curmode(ScanDirection dir, BTSkip skip)
{
	if (skip->overallMode == ScanModeMinMax)
	{
		skip->curMode = ScanDirectionIsForward(dir) ? ScanModeMin : ScanModeMax;
	}
	else
	{
		skip->curMode = skip->overallMode;
	}
}

/* @todo put into macro? */
bool _bt_skip_enabled(BTScanOpaque so)
{
	return so->skipData != NULL;
}

IndexTuple _bt_get_current_tuple(IndexScanDesc scan)
{
	Page		page;
	BTScanOpaque so = (BTScanOpaque) scan->opaque;
	ItemId		iid;
	IndexTuple	itup;

	if (_bt_skip_is_always_valid(so))
	{
		page = BufferGetPage(so->currPos.buf);

		iid = PageGetItemId(page, so->skipData->indexOffset);
		itup = (IndexTuple) PageGetItem(page, iid);

		return itup;
	}
	return NULL;
}

static bool
_bt_readpage_one(IndexScanDesc scan, ScanDirection dir, OffsetNumber offnum)
{
	BTScanOpaque so = (BTScanOpaque) scan->opaque;
	Page		page;
	BTPageOpaque opaque;
	OffsetNumber minoff;
	OffsetNumber maxoff;
	int			itemIndex;
	int			indnatts;

	/*
	 * We must have the buffer pinned and locked, but the usual macro can't be
	 * used here; this function is what makes it good for currPos.
	 */
	if (!_bt_skip_is_always_valid(so))
		return false;

	Assert(BufferIsValid(so->currPos.buf));

	page = BufferGetPage(so->currPos.buf);
	opaque = (BTPageOpaque) PageGetSpecialPointer(page);

	/* allow next page be processed by parallel worker */
	if (scan->parallel_scan)
	{
		if (ScanDirectionIsForward(dir))
			_bt_parallel_release(scan, opaque->btpo_next);
		else
			_bt_parallel_release(scan, BufferGetBlockNumber(so->currPos.buf));
	}

	indnatts = IndexRelationGetNumberOfAttributes(scan->indexRelation);
	minoff = P_FIRSTDATAKEY(opaque);
	maxoff = PageGetMaxOffsetNumber(page);

	/*
	 * We note the buffer's block number so that we can release the pin later.
	 * This allows us to re-read the buffer if it is needed again for hinting.
	 */
	so->currPos.currPage = BufferGetBlockNumber(so->currPos.buf);

	/*
	 * We save the LSN of the page as we read it, so that we know whether it
	 * safe to apply LP_DEAD hints to the page later.  This allows us to drop
	 * the pin for MVCC scans, which allows vacuum to avoid blocking.
	 */
	so->currPos.lsn = BufferGetLSNAtomic(so->currPos.buf);

	/*
	 * we must save the page's right-link while scanning it; this tells us
	 * where to step right to after we're done with these items.  There is no
	 * corresponding need for the left-link, since splits always go right.
	 */
	so->currPos.nextPage = opaque->btpo_next;

	/* initialize tuple workspace to empty */
	so->currPos.nextTupleOffset = 0;

	/*
	 * Now that the current page has been made consistent, the macro should be
	 * good.
	 */
	Assert(BTScanPosIsPinned(so->currPos));

	if (ScanDirectionIsForward(dir))
	{
		/* load items[] in ascending order */
		itemIndex = 0;

		offnum = Max(offnum, minoff);
		so->skipData->indexOffset = InvalidOffsetNumber;

		while (offnum <= maxoff)
		{
			ItemId		iid = PageGetItemId(page, offnum);
			IndexTuple	itup;

			/*
			 * If the scan specifies not to return killed tuples, then we
			 * treat a killed tuple as not passing the qual
			 */
			if (scan->ignore_killed_tuples && ItemIdIsDead(iid))
			{
				offnum = OffsetNumberNext(offnum);
				continue;
			}

			itup = (IndexTuple) PageGetItem(page, iid);

			_bt_compare_current_item(scan, itup, indnatts, dir);
			if (so->skipData->compareResult.equal)
			{
				/* tuple passes all scan key conditions, so remember it */
				_bt_saveitem(so, itemIndex, offnum, itup);
				itemIndex++;
				//so->skipData->indexOffset = offnum;
				//break;
			}
			so->skipData->indexOffset = offnum;
			/* When !continuescan, there can't be any more matches, so stop */
			if (so->skipData->compareResult.fullKeySkip || so->skipData->compareResult.prefixSkip)
				break;

			offnum = OffsetNumberNext(offnum);
		}

		Assert(itemIndex <= MaxIndexTuplesPerPage);
		so->currPos.firstItem = 0;
		so->currPos.lastItem = itemIndex - 1;
		so->currPos.itemIndex = 0;
		if (so->skipData->indexOffset == InvalidOffsetNumber)
			so->skipData->indexOffset = maxoff;
	}
	else
	{
		/* load items[] in descending order */
		itemIndex = MaxIndexTuplesPerPage;

		offnum = Min(offnum, maxoff);
		so->skipData->indexOffset = InvalidOffsetNumber;

		while (offnum >= minoff)
		{
			ItemId		iid = PageGetItemId(page, offnum);
			IndexTuple	itup;
			bool		tuple_alive;

			/*
			 * If the scan specifies not to return killed tuples, then we
			 * treat a killed tuple as not passing the qual.  Most of the
			 * time, it's a win to not bother examining the tuple's index
			 * keys, but just skip to the next tuple (previous, actually,
			 * since we're scanning backwards).  However, if this is the first
			 * tuple on the page, we do check the index keys, to prevent
			 * uselessly advancing to the page to the left.  This is similar
			 * to the high key optimization used by forward scans.
			 */
			if (scan->ignore_killed_tuples && ItemIdIsDead(iid))
			{
				Assert(offnum >= P_FIRSTDATAKEY(opaque));
				if (offnum > P_FIRSTDATAKEY(opaque))
				{
					offnum = OffsetNumberPrev(offnum);
					continue;
				}

				tuple_alive = false;
			}
			else
				tuple_alive = true;

			itup = (IndexTuple) PageGetItem(page, iid);

			_bt_compare_current_item(scan, itup, indnatts, dir);
			if (so->skipData->compareResult.equal && tuple_alive)
			{
				/* tuple passes all scan key conditions, so remember it */
				itemIndex--;
				//so->skipData->indexOffset = offnum;
				_bt_saveitem(so, itemIndex, offnum, itup);
				break;
			}
			so->skipData->indexOffset = offnum;
			if (so->skipData->compareResult.fullKeySkip || so->skipData->compareResult.prefixSkip)
				break;

			offnum = OffsetNumberPrev(offnum);
		}

		Assert(itemIndex >= 0);
		so->currPos.firstItem = itemIndex;
		so->currPos.lastItem = MaxIndexTuplesPerPage - 1;
		so->currPos.itemIndex = MaxIndexTuplesPerPage - 1;
		if (so->skipData->indexOffset == InvalidOffsetNumber)
			so->skipData->indexOffset = maxoff;
	}

	return (so->currPos.firstItem <= so->currPos.lastItem);
}


/*
 *	_bt_steppage() -- Step to next page containing valid data for scan
 *
 * On entry, if so->currPos.buf is valid the buffer is pinned but not locked;
 * if pinned, we'll drop the pin before moving to next page.  The buffer is
 * not locked on entry.
 *
 * For success on a scan using a non-MVCC snapshot we hold a pin, but not a
 * read lock, on that page.  If we do not hold the pin, we set so->currPos.buf
 * to InvalidBuffer.  We return true to indicate success.
 */
static bool
_bt_steppage_one(IndexScanDesc scan, ScanDirection dir)
{
	BTScanOpaque so = (BTScanOpaque) scan->opaque;
	BlockNumber blkno = InvalidBlockNumber;
	bool		status = true;

	Assert(BTScanPosIsValid(so->currPos));

	/* Before leaving current page, deal with any killed items */
	if (so->numKilled > 0)
		_bt_killitems(scan);

	/*
	 * Before we modify currPos, make a copy of the page data if there was a
	 * mark position that needs it.
	 */
	if (so->markItemIndex >= 0)
	{
		/* bump pin on current buffer for assignment to mark buffer */
		if (BTScanPosIsPinned(so->currPos))
			IncrBufferRefCount(so->currPos.buf);
		memcpy(&so->markPos, &so->currPos,
			   offsetof(BTScanPosData, items[1]) +
			   so->currPos.lastItem * sizeof(BTScanPosItem));
		if (so->markTuples)
			memcpy(so->markTuples, so->currTuples,
				   so->currPos.nextTupleOffset);
		so->markPos.itemIndex = so->markItemIndex;
		so->markItemIndex = -1;
	}

	if (ScanDirectionIsForward(dir))
	{
		/* Walk right to the next page with data */
		if (scan->parallel_scan != NULL)
		{
			/*
			 * Seize the scan to get the next block number; if the scan has
			 * ended already, bail out.
			 */
			status = _bt_parallel_seize(scan, &blkno);
			if (!status)
			{
				/* release the previous buffer, if pinned */
				BTScanPosUnpinIfPinned(so->currPos);
				BTScanPosInvalidate(so->currPos);
				return false;
			}
		}
		else
		{
			/* Not parallel, so use the previously-saved nextPage link. */
			blkno = so->currPos.nextPage;
		}

		/* Remember we left a page with data */
		so->currPos.moreLeft = true;

		/* release the previous buffer, if pinned */
		BTScanPosUnpinIfPinned(so->currPos);
	}
	else
	{
		/* Remember we left a page with data */
		so->currPos.moreRight = true;

		if (scan->parallel_scan != NULL)
		{
			/*
			 * Seize the scan to get the current block number; if the scan has
			 * ended already, bail out.
			 */
			status = _bt_parallel_seize(scan, &blkno);
			BTScanPosUnpinIfPinned(so->currPos);
			if (!status)
			{
				BTScanPosInvalidate(so->currPos);
				return false;
			}
		}
		else
		{
			/* Not parallel, so just use our own notion of the current page */
			blkno = so->currPos.currPage;
		}
	}

	if (!_bt_readnextpage_one(scan, blkno, dir))
		return false;

	/* Drop the lock, and maybe the pin, on the current page */
	//_bt_drop_lock_and_maybe_pin(scan, &so->currPos);

	return true;
}

/*
 *	_bt_readnextpage() -- Read next page containing valid data for scan
 *
 * On success exit, so->currPos is updated to contain data from the next
 * interesting page.  Caller is responsible to release lock and pin on
 * buffer on success.  We return true to indicate success.
 *
 * If there are no more matching records in the given direction, we drop all
 * locks and pins, set so->currPos.buf to InvalidBuffer, and return false.
 */
static bool
_bt_readnextpage_one(IndexScanDesc scan, BlockNumber blkno, ScanDirection dir)
{
	BTScanOpaque so = (BTScanOpaque) scan->opaque;
	Relation	rel;
	Page		page;
	BTPageOpaque opaque;
	bool		status = true;

	rel = scan->indexRelation;

	if (ScanDirectionIsForward(dir))
	{
		for (;;)
		{
			/*
			 * if we're at end of scan, give up and mark parallel scan as
			 * done, so that all the workers can finish their scan
			 */
			if (blkno == P_NONE || !so->currPos.moreRight)
			{
				_bt_parallel_done(scan);
				BTScanPosInvalidate(so->currPos);
				return false;
			}
			/* check for interrupts while we're not holding any buffer lock */
			CHECK_FOR_INTERRUPTS();
			/* step right one page */
			so->currPos.buf = _bt_getbuf(rel, blkno, BT_READ);
			page = BufferGetPage(so->currPos.buf);
			TestForOldSnapshot(scan->xs_snapshot, rel, page);
			opaque = (BTPageOpaque) PageGetSpecialPointer(page);
			/* check for deleted page */
			if (!P_IGNORE(opaque))
			{
				PredicateLockPage(rel, blkno, scan->xs_snapshot);
				/* see if there are any matches on this page */
				/* note that this will clear moreRight if we can stop */
				if (_bt_readpage_one(scan, dir, P_FIRSTDATAKEY(opaque)))
					break;
				if (so->skipData->compareResult.prefixSkip)
					return false;
			}
			else if (scan->parallel_scan != NULL)
			{
				/* allow next page be processed by parallel worker */
				_bt_parallel_release(scan, opaque->btpo_next);
			}

			/* nope, keep going */
			if (scan->parallel_scan != NULL)
			{
				_bt_relbuf(rel, so->currPos.buf);
				status = _bt_parallel_seize(scan, &blkno);
				if (!status)
				{
					BTScanPosInvalidate(so->currPos);
					return false;
				}
			}
			else
			{
				blkno = opaque->btpo_next;
				_bt_relbuf(rel, so->currPos.buf);
			}
		}
	}
	else
	{
		/*
		 * Should only happen in parallel cases, when some other backend
		 * advanced the scan.
		 */
		if (so->currPos.currPage != blkno)
		{
			BTScanPosUnpinIfPinned(so->currPos);
			so->currPos.currPage = blkno;
		}

		/*
		 * Walk left to the next page with data.  This is much more complex
		 * than the walk-right case because of the possibility that the page
		 * to our left splits while we are in flight to it, plus the
		 * possibility that the page we were on gets deleted after we leave
		 * it.  See nbtree/README for details.
		 *
		 * It might be possible to rearrange this code to have less overhead
		 * in pinning and locking, but that would require capturing the left
		 * pointer when the page is initially read, and using it here, along
		 * with big changes to _bt_walk_left() and the code below.  It is not
		 * clear whether this would be a win, since if the page immediately to
		 * the left splits after we read this page and before we step left, we
		 * would need to visit more pages than with the current code.
		 *
		 * Note that if we change the code so that we drop the pin for a scan
		 * which uses a non-MVCC snapshot, we will need to modify the code for
		 * walking left, to allow for the possibility that a referenced page
		 * has been deleted.  As long as the buffer is pinned or the snapshot
		 * is MVCC the page cannot move past the half-dead state to fully
		 * deleted.
		 */
		if (BTScanPosIsPinned(so->currPos))
			LockBuffer(so->currPos.buf, BT_READ);
		else
			so->currPos.buf = _bt_getbuf(rel, so->currPos.currPage, BT_READ);

		for (;;)
		{
			/* Done if we know there are no matching keys to the left */
			if (!so->currPos.moreLeft)
			{
				_bt_relbuf(rel, so->currPos.buf);
				_bt_parallel_done(scan);
				BTScanPosInvalidate(so->currPos);
				return false;
			}

			/* Step to next physical page */
			so->currPos.buf = _bt_walk_left(rel, so->currPos.buf,
											scan->xs_snapshot);

			/* if we're physically at end of index, return failure */
			if (so->currPos.buf == InvalidBuffer)
			{
				_bt_parallel_done(scan);
				BTScanPosInvalidate(so->currPos);
				return false;
			}

			/*
			 * Okay, we managed to move left to a non-deleted page. Done if
			 * it's not half-dead and contains matching tuples. Else loop back
			 * and do it all again.
			 */
			page = BufferGetPage(so->currPos.buf);
			TestForOldSnapshot(scan->xs_snapshot, rel, page);
			opaque = (BTPageOpaque) PageGetSpecialPointer(page);
			if (!P_IGNORE(opaque))
			{
				PredicateLockPage(rel, BufferGetBlockNumber(so->currPos.buf), scan->xs_snapshot);
				/* see if there are any matches on this page */
				/* note that this will clear moreLeft if we can stop */
				if (_bt_readpage_one(scan, dir, PageGetMaxOffsetNumber(page)))
					break;
				if (so->skipData->compareResult.prefixSkip)
					return false;
			}
			else if (scan->parallel_scan != NULL)
			{
				/* allow next page be processed by parallel worker */
				_bt_parallel_release(scan, BufferGetBlockNumber(so->currPos.buf));
			}

			/*
			 * For parallel scans, get the last page scanned as it is quite
			 * possible that by the time we try to seize the scan, some other
			 * worker has already advanced the scan to a different page.  We
			 * must continue based on the latest page scanned by any worker.
			 */
			if (scan->parallel_scan != NULL)
			{
				_bt_relbuf(rel, so->currPos.buf);
				status = _bt_parallel_seize(scan, &blkno);
				if (!status)
				{
					BTScanPosInvalidate(so->currPos);
					return false;
				}
				so->currPos.buf = _bt_getbuf(rel, blkno, BT_READ);
			}
		}
	}

	return true;
}
