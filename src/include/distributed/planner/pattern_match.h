//
// Created by Nils Dijk on 20/01/2021.
//

#ifndef CITUS_PATTERN_MATCH_H
#define CITUS_PATTERN_MATCH_H

#include "nodes/plannodes.h"

#define GET_ARG_COUNT(...) INTERNAL_GET_ARG_COUNT_PRIVATE(0, ## __VA_ARGS__, 70, 69, 68, 67, 66, 65, 64, 63, 62, 61, 60, 59, 58, 57, 56, 55, 54, 53, 52, 51, 50, 49, 48, 47, 46, 45, 44, 43, 42, 41, 40, 39, 38, 37, 36, 35, 34, 33, 32, 31, 30, 29, 28, 27, 26, 25, 24, 23, 22, 21, 20, 19, 18, 17, 16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0)
#define INTERNAL_GET_ARG_COUNT_PRIVATE(_0, _1_, _2_, _3_, _4_, _5_, _6_, _7_, _8_, _9_, _10_, _11_, _12_, _13_, _14_, _15_, _16_, _17_, _18_, _19_, _20_, _21_, _22_, _23_, _24_, _25_, _26_, _27_, _28_, _29_, _30_, _31_, _32_, _33_, _34_, _35_, _36, _37, _38, _39, _40, _41, _42, _43, _44, _45, _46, _47, _48, _49, _50, _51, _52, _53, _54, _55, _56, _57, _58, _59, _60, _61, _62, _63, _64, _65, _66, _67, _68, _69, _70, count, ...) count

#define CONCATENATE(arg1, arg2)   CONCATENATE1(arg1, arg2)
#define CONCATENATE1(arg1, arg2)  CONCATENATE2(arg1, arg2)
#define CONCATENATE2(arg1, arg2)  arg1##arg2

#define FOR_EACH_0(what, index, x) ;
#define FOR_EACH_1(what, index, x)      what((index), x); FOR_EACH_0(what, index+1,  __VA_ARGS__);
#define FOR_EACH_2(what, index, x, ...) what((index), x); FOR_EACH_1(what, index+1,  __VA_ARGS__);
#define FOR_EACH_3(what, index, x, ...) what((index), x); FOR_EACH_2(what, index+1,  __VA_ARGS__);
#define FOR_EACH_4(what, index, x, ...) what((index), x); FOR_EACH_3(what, index+1,  __VA_ARGS__);
#define FOR_EACH_5(what, index, x, ...) what((index), x); FOR_EACH_4(what, index+1,  __VA_ARGS__);
#define FOR_EACH_6(what, index, x, ...) what((index), x); FOR_EACH_5(what, index+1,  __VA_ARGS__);
#define FOR_EACH_7(what, index, x, ...) what((index), x); FOR_EACH_6(what, index+1,  __VA_ARGS__);
#define FOR_EACH_8(what, index, x, ...) what((index), x); FOR_EACH_7(what, index+1,  __VA_ARGS__);

#define FOR_EACH_NARG(...) FOR_EACH_NARG_(__VA_ARGS__, FOR_EACH_RSEQ_N())
#define FOR_EACH_NARG_(...) FOR_EACH_ARG_N(__VA_ARGS__)
#define FOR_EACH_ARG_N(_1, _2, _3, _4, _5, _6, _7, _8, N, ...) N
#define FOR_EACH_RSEQ_N() 8, 7, 6, 5, 4, 3, 2, 1, 0

#define FOR_EACH_(N, what, ...) CONCATENATE(FOR_EACH_, N)(what, 0, __VA_ARGS__)
#define FOR_EACH(what, ...) FOR_EACH_(FOR_EACH_NARG(__VA_ARGS__), what, __VA_ARGS__)

#define MatchAny { }
#define MatchFailed { break; }


#define MakeStack(type, stackName, value) \
	type *stackName = (type *) value; \
	List *stackName##Stack = NIL;\
	(void) stackName; \
	(void) stackName##Stack;

#define PushStack(stackName) \
	stackName##Stack = lappend(stackName##Stack, stackName)

#define PeekStack(stackName) \
	((typeof(stackName)) llast(stackName##Stack))

#define PopStack(stackName) \
	stackName = (typeof(stackName)) llast(stackName##Stack); \
	stackName##Stack = list_delete_last(stackName##Stack)

#define VerifyStack(stackName) \
	Assert(list_length(stackName##Stack) == 0)

#define InitializeCapture \
	void *ignoreCaptureValue = NULL; \
	(void) ignoreCaptureValue \

#define NoCapture \
	&ignoreCaptureValue

#define DoCapture(capture, type, toCapture) \
	*(capture) = (type) toCapture

#define SkipReadThrough(capture, matcher) \
{ \
	PushStack(pathToMatch); \
\
    { \
        bool skipped = true; \
        while (skipped) { \
            switch(pathToMatch->type) \
            { \
                case T_MaterialPath: \
                { \
                    pathToMatch = castNode(MaterialPath, pathToMatch)->subpath; \
                    break; \
                } \
\
                default: \
                { \
                    skipped = false; \
                    break; \
                } \
            } \
        } \
	} \
\
	matcher; \
	PopStack(pathToMatch); \
	DoCapture(capture, Path *, pathToMatch); \
}

#define MatchJoin(capture, joinType, conditionMatcher, innerMatcher, outerMatcher) \
{ \
    { \
        bool m = false; \
        switch (pathToMatch->type) \
        { \
            case T_NestPath: \
            case T_MergePath: \
            case T_HashPath: \
            { \
                m = true; \
                break; \
            } \
\
            default: \
            { \
                m = false; \
                break; \
            } \
        } \
\
        if (!m)\
        { \
            MatchFailed; \
        } \
    } \
\
	if (((JoinPath *) pathToMatch)->jointype != joinType) \
	{ \
		MatchFailed; \
	} \
\
	PushStack(pathToMatch); \
\
	pathToMatch = ((JoinPath *) PeekStack(pathToMatch))->innerjoinpath; \
	innerMatcher; \
	pathToMatch = ((JoinPath *) PeekStack(pathToMatch))->outerjoinpath; \
	outerMatcher; \
\
	PopStack(pathToMatch); \
	conditionMatcher; \
	DoCapture(capture, JoinPath *, pathToMatch); \
}

#define MatchGrouping(capture, matcher) \
{                              \
	if (!IsA(pathToMatch, AggPath)) \
	{ \
		MatchFailed; \
	} \
\
	PushStack(pathToMatch); \
\
	pathToMatch = ((AggPath *) pathToMatch)->subpath; \
	matcher;\
\
	PopStack(pathToMatch); \
    DoCapture(capture, AggPath *, pathToMatch); \
}

#define MatchDistributedUnion(capture, matcher) \
{ \
	if (!IsDistributedUnion(pathToMatch, false, NULL))    \
	{ \
		MatchFailed; \
	} \
\
	PushStack(pathToMatch); \
	pathToMatch = ((DistributedUnionPath *) pathToMatch)->worker_path; \
	PopStack(pathToMatch); \
	DoCapture(capture, DistributedUnionPath *, pathToMatch); \
}

#define MatchGeoScan(capture) \
{ \
	if (!IsA(pathToMatch, CustomPath)) \
	{ \
		MatchFailed; \
	} \
\
	if (!IsGeoScanPath(castNode(CustomPath, pathToMatch))) \
	{ \
		MatchFailed; \
	} \
\
	DoCapture(capture, GeoScanPath *, pathToMatch); \
}

#define IfPathMatch(path, matcher) \
bool matched = false; \
do \
{ \
    MakeStack(Path, pathToMatch, path); \
	InitializeCapture; \
\
    matcher; \
\
	VerifyStack(pathToMatch); \
	matched = true; \
	break; \
} \
while (false); \
if (matched)


#define MatchJoinRestrictions(capture, matcher) \
{ \
    Assert(IsA(pathToMatch, NestPath) \
	    || IsA(pathToMatch, MergePath) \
    	|| IsA(pathToMatch, HashPath)); \
\
	bool restrictionMatched = false; \
    RestrictInfo *restrictInfo = NULL; \
	foreach_ptr(restrictInfo, ((JoinPath *) pathToMatch)->joinrestrictinfo) \
	{ \
		do { \
            MakeStack(Expr, clause, restrictInfo->clause);\
\
			matcher; \
\
			restrictionMatched = true; \
			VerifyStack(clause); \
        	DoCapture(capture, RestrictInfo *, restrictInfo); \
		} while(false); \
		if (restrictionMatched) \
		{ \
        	break; \
		} \
	} \
\
	if (!restrictionMatched) \
	{ \
		MatchFailed; \
	} \
}


#define InternalFunctionDispatch(index, matcher) \
{ \
    clause = (Expr *) list_nth(((FuncExpr *) PeekStack(clause))->args, index); \
    matcher; \
}


#define MatchExprNamedFunction(capture, name, ...) \
{ \
	if (!IsA(clause, FuncExpr)) \
	{ \
    	MatchFailed; \
	} \
\
    { \
        FuncExpr *funcexpr = castNode(FuncExpr, clause); \
        if (list_length(funcexpr->args) != GET_ARG_COUNT(__VA_ARGS__)) \
        { \
            MatchFailed; \
        } \
\
		NameData funcexprNameData = GetFunctionNameData(funcexpr->funcid); \
		if (strcmp(NameStr(funcexprNameData), #name) != 0) \
		{ \
			MatchFailed; \
		} \
	} \
\
	PushStack(clause); \
	FOR_EACH(InternalFunctionDispatch, __VA_ARGS__); \
    PopStack(clause); \
	DoCapture(capture, FuncExpr *, clause); \
}


#define InternalOperationDispatch(index, matcher) \
{ \
    clause = (Expr *) list_nth(((OpExpr *) PeekStack(clause))->args, index); \
    matcher; \
}


#define MatchExprNamedOperation(capture, name, ...) \
{ \
	if (!IsA(clause, OpExpr)) \
	{ \
    	MatchFailed; \
	} \
\
    { \
        OpExpr *opexpr = castNode(OpExpr, clause); \
        if (list_length(opexpr->args) != GET_ARG_COUNT(__VA_ARGS__)) \
        { \
            MatchFailed; \
        } \
\
		NameData opexprNameData = GetFunctionNameData(opexpr->opfuncid); \
		if (strcmp(NameStr(opexprNameData), #name) != 0)             \
		{ \
			MatchFailed; \
		} \
	} \
\
	PushStack(clause); \
	FOR_EACH(InternalOperationDispatch, __VA_ARGS__); \
	PopStack(clause); \
    DoCapture(capture, OpExpr *, clause); \
}


#define MatchFieldsInternal(index, check) \
{ \
	if (!(typedClause->check)) \
	{ \
		MatchFailed; \
	} \
}


#define MatchFields(...) \
	FOR_EACH(MatchFieldsInternal, __VA_ARGS__);


#define MatchVar(capture, ...) \
{ \
	if (!IsA(clause, Var)) \
	{ \
    	MatchFailed; \
	} \
	Var *typedClause = castNode(Var, clause); \
	(void) typedClause; \
	__VA_ARGS__; \
	DoCapture(capture, Var *, clause); \
}


#define MatchConst(capture, ...) \
{ \
	if (!IsA(clause, Const)) \
	{ \
    	MatchFailed; \
	} \
	Const *typedClause = castNode(Const, clause); \
	(void) typedClause; \
	__VA_ARGS__; \
	DoCapture(capture, Const *, clause); \
}


#endif //CITUS_PATTERN_MATCH_H
