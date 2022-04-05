/*-------------------------------------------------------------------------
 * param_utils.h
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef PARAM_UTILS_H
#define PARAM_UTILS_H

extern bool GetParamsUsedInQuery(Node *expression, Bitmapset **paramBitmap);
extern void MarkUnreferencedExternParams(Node *expression, ParamListInfo boundParams);

#endif /* PARAM_UTILS_H */
