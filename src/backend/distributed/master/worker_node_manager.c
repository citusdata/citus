/*-------------------------------------------------------------------------
 *
 * worker_node_manager.c
 *	  Routines for reading worker nodes from membership file, and allocating
 *	  candidate nodes for shard placement.
 *
 * Copyright (c) 2012-2016, Citus Data, Inc.
 *
 * $Id$
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "miscadmin.h"

#include "commands/dbcommands.h"
#include "distributed/worker_manager.h"
#include "distributed/multi_client_executor.h"
#include "libpq/hba.h"
#include "libpq/ip.h"
#include "libpq/libpq-be.h"
#include "postmaster/postmaster.h"
#include "storage/fd.h"
#include "storage/ipc.h"
#include "storage/shmem.h"
#include "utils/guc.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"


/* Config variables managed via guc.c */
char *WorkerListFileName;            /* location of pg_worker_list.conf */
int MaxWorkerNodesTracked = 2048;    /* determines worker node hash table size */

static HTAB *WorkerNodesHash = NULL; /* worker node hash in shared memory */
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;


/* Local functions forward declarations */
static char * ClientHostAddress(StringInfo remoteHostStringInfo);
static bool OddNumber(uint32 number);
static WorkerNode * FindRandomNodeNotInList(HTAB *WorkerNodesHash,
											List *currentNodeList);
static bool ListMember(List *currentList, WorkerNode *workerNode);
static Size WorkerNodeShmemSize(void);
static void WorkerNodeShmemAndWorkerListInit(void);
static uint32 WorkerNodeHashCode(const void *key, Size keySize);
static int WorkerNodeCompare(const void *lhsKey, const void *rhsKey, Size keySize);
static List * ParseWorkerNodeFile(const char *workerNodeFilename);
static void ResetWorkerNodesHash(HTAB *WorkerNodesHash);
static bool WorkerNodeResponsive(const char *workerName, uint32 workerPort);


/* ------------------------------------------------------------
 * Worker node selection functions follow
 * ------------------------------------------------------------
 */

/*
 * WorkerGetRandomCandidateNode takes in a list of worker nodes, and then allocates
 * a new worker node. The allocation is performed according to the following
 * policy: if the list is empty, a random node is allocated; if the list has one
 * node (or an odd number of nodes), the new node is allocated on a different
 * rack than the first node; and if the list has two nodes (or an even number of
 * nodes), the new node is allocated on the same rack as the first node, but is
 * different from all the nodes in the list. This node allocation policy ensures
 * that shard locality is maintained within a rack, but no single rack failure
 * can result in data loss.
 *
 * Note that the function returns null if the worker membership list does not
 * contain enough nodes to allocate a new worker node.
 */
WorkerNode *
WorkerGetRandomCandidateNode(List *currentNodeList)
{
	WorkerNode *workerNode = NULL;
	bool wantSameRack = false;
	uint32 tryCount = WORKER_RACK_TRIES;
	uint32 tryIndex = 0;

	/*
	 * We check if the shard has already been placed on all nodes known to us.
	 * This check is rather defensive, and has the drawback of performing a full
	 * scan over the worker node hash for determining the number of live nodes.
	 */
	uint32 currentNodeCount = list_length(currentNodeList);
	uint32 liveNodeCount = WorkerGetLiveNodeCount();
	if (currentNodeCount >= liveNodeCount)
	{
		return NULL;
	}

	/* if current node list is empty, randomly pick one node and return */
	if (currentNodeCount == 0)
	{
		workerNode = FindRandomNodeNotInList(WorkerNodesHash, NIL);
		return workerNode;
	}

	/*
	 * If the current list has an odd number of nodes (1, 3, 5, etc), we want to
	 * place the shard on a different rack than the first node's rack.
	 * Otherwise, we want to place the shard on the same rack as the first node.
	 */
	if (OddNumber(currentNodeCount))
	{
		wantSameRack = false;
	}
	else
	{
		wantSameRack = true;
	}

	/*
	 * We try to find a worker node that fits our rack-aware placement strategy.
	 * If after a predefined number of tries, we still cannot find such a node,
	 * we simply give up and return the last worker node we found.
	 */
	for (tryIndex = 0; tryIndex < tryCount; tryIndex++)
	{
		WorkerNode *firstNode = (WorkerNode *) linitial(currentNodeList);
		char *firstRack = firstNode->workerRack;
		char *workerRack = NULL;
		bool sameRack = false;

		workerNode = FindRandomNodeNotInList(WorkerNodesHash, currentNodeList);
		workerRack = workerNode->workerRack;

		sameRack = (strncmp(workerRack, firstRack, WORKER_LENGTH) == 0);
		if ((sameRack && wantSameRack) || (!sameRack && !wantSameRack))
		{
			break;
		}
	}

	return workerNode;
}


/*
 * WorkerGetRoundRobinCandidateNode takes in a list of worker nodes and returns
 * a candidate worker node from that list. To select this node, this function
 * uses the round-robin policy. An ideal round-robin implementation requires
 * keeping shared state for shard placements; and we instead approximate our
 * implementation by relying on the ever-increasing shardId. So, the first
 * worker node selected will be the node at the (shardId MOD worker node count)
 * index and the remaining candidate nodes will be the next nodes in the list.
 *
 * Note that the function returns null if the worker membership list does not
 * contain enough nodes to place all replicas.
 */
WorkerNode *
WorkerGetRoundRobinCandidateNode(List *workerNodeList, uint64 shardId,
								 uint32 placementIndex)
{
	uint32 workerNodeCount = list_length(workerNodeList);
	WorkerNode *candidateNode = NULL;

	if (placementIndex < workerNodeCount)
	{
		uint32 candidateNodeIndex = (shardId + placementIndex) % workerNodeCount;
		candidateNode = (WorkerNode *) list_nth(workerNodeList, candidateNodeIndex);
	}

	return candidateNode;
}


/*
 * WorkerGetLocalFirstCandidateNode takes in a list of worker nodes, and then
 * allocates a new worker node. The allocation is performed according to the
 * following policy: if the list is empty, the node where the caller is connecting
 * from is allocated; if the list is not empty, a node is allocated according
 * to random policy.
 */
WorkerNode *
WorkerGetLocalFirstCandidateNode(List *currentNodeList)
{
	WorkerNode *candidateNode = NULL;
	uint32 currentNodeCount = list_length(currentNodeList);

	/* choose first candidate node to be the client's host */
	if (currentNodeCount == 0)
	{
		StringInfo clientHostStringInfo = makeStringInfo();
		char *clientHost = NULL;
		char *errorMessage = ClientHostAddress(clientHostStringInfo);

		if (errorMessage != NULL)
		{
			ereport(ERROR, (errmsg("%s", errorMessage),
							errdetail("Could not find the first worker "
									  "node for local-node-first policy."),
							errhint("Make sure that you are not on the "
									"master node.")));
		}

		/* if hostname is localhost.localdomain, change it to localhost */
		clientHost = clientHostStringInfo->data;
		if (strncmp(clientHost, "localhost.localdomain", WORKER_LENGTH) == 0)
		{
			clientHost = pstrdup("localhost");
		}

		candidateNode = WorkerGetNodeWithName(clientHost);
		if (candidateNode == NULL)
		{
			ereport(ERROR, (errmsg("could not find worker node for "
								   "host: %s", clientHost)));
		}
	}
	else
	{
		/* find a candidate node different from those already selected */
		candidateNode = WorkerGetRandomCandidateNode(currentNodeList);
	}

	return candidateNode;
}


/*
 * ClientHostAddress appends the connecting client's fully qualified hostname
 * to the given StringInfo. If there is no such connection or the connection is
 * over Unix domain socket, the function fills the error message and returns it.
 * On success, it just returns NULL.
 */
static char *
ClientHostAddress(StringInfo clientHostStringInfo)
{
	Port *port = MyProcPort;
	char *clientHost = NULL;
	char *errorMessage = NULL;
	int clientHostLength = NI_MAXHOST;
	int flags = NI_NAMEREQD;    /* require fully qualified hostname */
	int nameFound = 0;

	if (port == NULL)
	{
		errorMessage = "cannot find tcp/ip connection to client";
		return errorMessage;
	}

	switch (port->raddr.addr.ss_family)
	{
		case AF_INET:
#ifdef HAVE_IPV6
		case AF_INET6:
#endif
			{
				break;
			}

		default:
		{
			errorMessage = "invalid address family in connection";
			return errorMessage;
		}
	}

	clientHost = palloc0(clientHostLength);

	nameFound = pg_getnameinfo_all(&port->raddr.addr, port->raddr.salen,
								   clientHost, clientHostLength, NULL, 0, flags);
	if (nameFound == 0)
	{
		appendStringInfo(clientHostStringInfo, "%s", clientHost);
	}
	else
	{
		StringInfo errorMessageStringInfo = makeStringInfo();
		appendStringInfo(errorMessageStringInfo, "could not resolve client host: %s",
						 gai_strerror(nameFound));

		errorMessage = errorMessageStringInfo->data;
		return errorMessage;
	}

	return errorMessage;
}


/*
 * WorkerGetNodeWithName finds and returns a node from the membership list that
 * has the given hostname. The function returns null if no such node exists.
 */
WorkerNode *
WorkerGetNodeWithName(const char *hostname)
{
	WorkerNode *workerNode = NULL;

	HASH_SEQ_STATUS status;
	hash_seq_init(&status, WorkerNodesHash);

	workerNode = (WorkerNode *) hash_seq_search(&status);
	while (workerNode != NULL)
	{
		if (workerNode->inWorkerFile)
		{
			int nameCompare = strncmp(workerNode->workerName, hostname, WORKER_LENGTH);
			if (nameCompare == 0)
			{
				hash_seq_term(&status);
				break;
			}
		}

		workerNode = (WorkerNode *) hash_seq_search(&status);
	}

	return workerNode;
}


/* Returns the number of live nodes in the cluster. */
uint32
WorkerGetLiveNodeCount(void)
{
	WorkerNode *workerNode = NULL;
	uint32 liveWorkerCount = 0;

	HASH_SEQ_STATUS status;
	hash_seq_init(&status, WorkerNodesHash);

	workerNode = (WorkerNode *) hash_seq_search(&status);
	while (workerNode != NULL)
	{
		if (workerNode->inWorkerFile)
		{
			liveWorkerCount++;
		}

		workerNode = (WorkerNode *) hash_seq_search(&status);
	}

	return liveWorkerCount;
}


/* Inserts the live worker nodes to a list, and returns the list. */
List *
WorkerNodeList(void)
{
	List *workerNodeList = NIL;
	WorkerNode *workerNode = NULL;

	HASH_SEQ_STATUS status;
	hash_seq_init(&status, WorkerNodesHash);

	workerNode = (WorkerNode *) hash_seq_search(&status);
	while (workerNode != NULL)
	{
		if (workerNode->inWorkerFile)
		{
			workerNodeList = lappend(workerNodeList, workerNode);
		}

		workerNode = (WorkerNode *) hash_seq_search(&status);
	}

	return workerNodeList;
}


/*
 * WorkerNodeActive looks up a worker node with the given name and port number
 * in the current membership list. If such a worker node exists, the function
 * returns true.
 */
bool
WorkerNodeActive(const char *nodeName, uint32 nodePort)
{
	bool workerNodeActive = false;
	bool handleFound = false;
	WorkerNode *workerNode = NULL;
	void *hashKey = NULL;

	WorkerNode *searchedNode = (WorkerNode *) palloc0(sizeof(WorkerNode));
	strlcpy(searchedNode->workerName, nodeName, WORKER_LENGTH);
	searchedNode->workerPort = nodePort;

	hashKey = (void *) searchedNode;
	workerNode = (WorkerNode *) hash_search(WorkerNodesHash, hashKey,
											HASH_FIND, &handleFound);
	if (workerNode != NULL)
	{
		if (workerNode->inWorkerFile)
		{
			workerNodeActive = true;
		}
	}

	return workerNodeActive;
}


/* Returns true if given number is odd; returns false otherwise. */
static bool
OddNumber(uint32 number)
{
	bool oddNumber = ((number % 2) == 1);
	return oddNumber;
}


/*
 * FindRandomNodeNotInList finds a random node from the shared hash that is not
 * a member of the current node list. The caller is responsible for making the
 * necessary node count checks to ensure that such a node exists.
 *
 * Note that this function has a selection bias towards nodes whose positions in
 * the shared hash are sequentially adjacent to the positions of nodes that are
 * in the current node list. This bias follows from our decision to first pick a
 * random node in the hash, and if that node is a member of the current list, to
 * simply iterate to the next node in the hash. Overall, this approach trades in
 * some selection bias for simplicity in design and for bounded execution time.
 */
static WorkerNode *
FindRandomNodeNotInList(HTAB *WorkerNodesHash, List *currentNodeList)
{
	WorkerNode *workerNode = NULL;
	HASH_SEQ_STATUS status;
	uint32 workerNodeCount = 0;
	uint32 currentNodeCount PG_USED_FOR_ASSERTS_ONLY = 0;
	bool lookForWorkerNode = true;
	uint32 workerPosition = 0;
	uint32 workerIndex = 0;

	workerNodeCount = hash_get_num_entries(WorkerNodesHash);
	currentNodeCount = list_length(currentNodeList);
	Assert(workerNodeCount > currentNodeCount);

	/*
	 * We determine a random position within the worker hash between [1, N],
	 * assuming that the number of elements in the hash is N. We then get to
	 * this random position by iterating over the worker hash. Please note that
	 * the random seed has already been set by the postmaster when starting up.
	 */
	workerPosition = (random() % workerNodeCount) + 1;
	hash_seq_init(&status, WorkerNodesHash);

	for (workerIndex = 0; workerIndex < workerPosition; workerIndex++)
	{
		workerNode = (WorkerNode *) hash_seq_search(&status);
	}

	while (lookForWorkerNode)
	{
		bool listMember = ListMember(currentNodeList, workerNode);

		if (workerNode->inWorkerFile && !listMember)
		{
			lookForWorkerNode = false;
		}
		else
		{
			/* iterate to the next worker node in the hash */
			workerNode = (WorkerNode *) hash_seq_search(&status);

			/* reached end of hash; start from the beginning */
			if (workerNode == NULL)
			{
				hash_seq_init(&status, WorkerNodesHash);
				workerNode = (WorkerNode *) hash_seq_search(&status);
			}
		}
	}

	/* we stopped scanning before completion; therefore clean up scan */
	hash_seq_term(&status);

	return workerNode;
}


/* Checks if given worker node is a member of the current list. */
static bool
ListMember(List *currentList, WorkerNode *workerNode)
{
	bool listMember = false;
	Size keySize = WORKER_LENGTH + sizeof(uint32);

	ListCell *currentCell = NULL;
	foreach(currentCell, currentList)
	{
		WorkerNode *currentNode = (WorkerNode *) lfirst(currentCell);
		if (WorkerNodeCompare(workerNode, currentNode, keySize) == 0)
		{
			listMember = true;
		}
	}

	return listMember;
}


/* ------------------------------------------------------------
 * Worker node shared hash functions follow
 * ------------------------------------------------------------
 */

/* Organize, at startup, that the resources for worker node management are allocated. */
void
WorkerNodeRegister(void)
{
	RequestAddinShmemSpace(WorkerNodeShmemSize());

	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = WorkerNodeShmemAndWorkerListInit;
}


/* Estimates the shared memory size used for managing worker nodes. */
static Size
WorkerNodeShmemSize(void)
{
	Size size = 0;
	Size hashSize = 0;

	hashSize = hash_estimate_size(MaxWorkerNodesTracked, sizeof(WorkerNode));
	size = add_size(size, hashSize);

	return size;
}


/* Initializes the shared memory used for managing worker nodes. */
static void
WorkerNodeShmemAndWorkerListInit(void)
{
	HASHCTL info;
	int hashFlags = 0;
	long maxTableSize = 0;
	long initTableSize = 0;

	maxTableSize = (long) MaxWorkerNodesTracked;
	initTableSize = maxTableSize / 8;

	/*
	 * Allocate the control structure for the hash table that maps worker node
	 * name and port numbers (char[]:uint32) to general node membership and
	 * health information.
	 */
	memset(&info, 0, sizeof(info));
	info.keysize = WORKER_LENGTH + sizeof(uint32);
	info.entrysize = sizeof(WorkerNode);
	info.hash = WorkerNodeHashCode;
	info.match = WorkerNodeCompare;
	hashFlags = (HASH_ELEM | HASH_FUNCTION | HASH_COMPARE);

	WorkerNodesHash = ShmemInitHash("Worker Node Hash",
									initTableSize, maxTableSize,
									&info, hashFlags);

	/*
	 * Load the intial contents of the worker node hash table from the
	 * configuration file.
	 */
	LoadWorkerNodeList(WorkerListFileName);

	if (prev_shmem_startup_hook != NULL)
	{
		prev_shmem_startup_hook();
	}
}


/*
 * WorkerNodeHashCode computes the hash code for a worker node from the node's
 * host name and port number. Nodes that only differ by their rack locations
 * hash to the same value.
 */
static uint32
WorkerNodeHashCode(const void *key, Size keySize)
{
	const WorkerNode *worker = (const WorkerNode *) key;
	const char *workerName = worker->workerName;
	const uint32 *workerPort = &(worker->workerPort);

	/* standard hash function outlined in Effective Java, Item 8 */
	uint32 result = 17;
	result = 37 * result + string_hash(workerName, WORKER_LENGTH);
	result = 37 * result + tag_hash(workerPort, sizeof(uint32));
	return result;
}


/*
 * CompareWorkerNodes compares two pointers to worker nodes using the exact
 * same logic employed by WorkerNodeCompare.
 */
int
CompareWorkerNodes(const void *leftElement, const void *rightElement)
{
	const void *leftWorker = *((const void **) leftElement);
	const void *rightWorker = *((const void **) rightElement);
	int compare = 0;
	Size ignoredKeySize = 0;

	compare = WorkerNodeCompare(leftWorker, rightWorker, ignoredKeySize);

	return compare;
}


/*
 * WorkerNodeCompare compares two worker nodes by their host name and port
 * number. Two nodes that only differ by their rack locations are considered to
 * be equal to each other.
 */
static int
WorkerNodeCompare(const void *lhsKey, const void *rhsKey, Size keySize)
{
	const WorkerNode *workerLhs = (const WorkerNode *) lhsKey;
	const WorkerNode *workerRhs = (const WorkerNode *) rhsKey;

	int nameCompare = 0;
	int portCompare = 0;

	nameCompare = strncmp(workerLhs->workerName, workerRhs->workerName, WORKER_LENGTH);
	if (nameCompare != 0)
	{
		return nameCompare;
	}

	portCompare = workerLhs->workerPort - workerRhs->workerPort;
	return portCompare;
}


/*
 * LoadWorkerNodeList reads and parses given membership file, and loads worker
 * nodes from this membership file into the shared hash. The function relies on
 * hba.c's tokenization method for parsing, and therefore the membership file
 * has the same syntax as other configuration files such as ph_hba.conf.
 *
 * Note that this function allows for reloading membership configuration files
 * at runtime. When that happens, old worker nodes that do not appear in the
 * file are marked as stale, but are still kept in the shared hash.
 */
void
LoadWorkerNodeList(const char *workerFilename)
{
	List *workerList = NIL;
	ListCell *workerCell = NULL;
	uint32 workerCount = 0;

	workerList = ParseWorkerNodeFile(workerFilename);

	workerCount = list_length(workerList);
	if (workerCount > MaxWorkerNodesTracked)
	{
		ereport(FATAL, (errcode(ERRCODE_CONFIG_FILE_ERROR),
						errmsg("worker node count: %u exceeds max allowed value: %d",
							   workerCount, MaxWorkerNodesTracked)));
	}
	else
	{
		ereport(INFO, (errmsg("reading nodes from worker file: %s", workerFilename)));
	}

	/* before reading file's lines, reset worker node hash */
	ResetWorkerNodesHash(WorkerNodesHash);

	/* parse file lines */
	foreach(workerCell, workerList)
	{
		WorkerNode *workerNode = NULL;
		WorkerNode *parsedNode = lfirst(workerCell);
		void *hashKey = NULL;
		bool handleFound = false;

		/*
		 * Search for the parsed worker node in the hash, and then insert parsed
		 * values. When searching, we make the hashKey point to the beginning of
		 * the parsed node; we previously set the key length and key comparison
		 * function to include both the node name and the port number.
		 */
		hashKey = (void *) parsedNode;
		workerNode = (WorkerNode *) hash_search(WorkerNodesHash, hashKey,
												HASH_ENTER, &handleFound);

		if (handleFound)
		{
			/* display notification if worker node's rack changed */
			char *oldWorkerRack = workerNode->workerRack;
			char *newWorkerRack = parsedNode->workerRack;

			if (strncmp(oldWorkerRack, newWorkerRack, WORKER_LENGTH) != 0)
			{
				ereport(INFO, (errmsg("worker node: \"%s:%u\" changed rack location",
									  workerNode->workerName, workerNode->workerPort)));
			}

			/* display warning if worker node already appeared in this file */
			if (workerNode->inWorkerFile)
			{
				ereport(WARNING, (errmsg("multiple lines for worker node: \"%s:%u\"",
										 workerNode->workerName,
										 workerNode->workerPort)));
			}
		}

		strlcpy(workerNode->workerName, parsedNode->workerName, WORKER_LENGTH);
		strlcpy(workerNode->workerRack, parsedNode->workerRack, WORKER_LENGTH);
		workerNode->workerPort = parsedNode->workerPort;
		workerNode->inWorkerFile = parsedNode->inWorkerFile;

		pfree(parsedNode);
	}
}


/*
 * ParseWorkerNodeFile opens and parses the node name and node port from the
 * specified configuration file.
 */
static List *
ParseWorkerNodeFile(const char *workerNodeFilename)
{
	FILE *workerFileStream = NULL;
	List *workerNodeList = NIL;
	char workerNodeLine[MAXPGPATH];
	char *workerFilePath = make_absolute_path(workerNodeFilename);
	char *workerPatternTemplate = "%%%u[^# \t]%%*[ \t]%%%u[^# \t]%%*[ \t]%%%u[^# \t]";
	char workerLinePattern[1024];
	const int workerNameIndex = 0;
	const int workerPortIndex = 1;

	memset(workerLinePattern, '\0', sizeof(workerLinePattern));

	workerFileStream = AllocateFile(workerFilePath, PG_BINARY_R);
	if (workerFileStream == NULL)
	{
		if (errno == ENOENT)
		{
			ereport(DEBUG1, (errmsg("worker list file located at \"%s\" is not present",
									workerFilePath)));
		}
		else
		{
			ereport(ERROR, (errcode_for_file_access(),
							errmsg("could not open worker list file \"%s\": %m",
								   workerFilePath)));
		}
		return NIL;
	}

	/* build pattern to contain node name length limit */
	snprintf(workerLinePattern, sizeof(workerLinePattern), workerPatternTemplate,
			 WORKER_LENGTH, MAX_PORT_LENGTH, WORKER_LENGTH);

	while (fgets(workerNodeLine, sizeof(workerNodeLine), workerFileStream) != NULL)
	{
		const int workerLineLength = strnlen(workerNodeLine, MAXPGPATH);
		WorkerNode *workerNode = NULL;
		char *linePointer = NULL;
		int32 nodePort = PostPortNumber; /* default port number */
		int fieldCount = 0;
		bool lineIsInvalid = false;
		char nodeName[WORKER_LENGTH + 1];
		char nodeRack[WORKER_LENGTH + 1];
		char nodePortString[MAX_PORT_LENGTH + 1];

		memset(nodeName, '\0', sizeof(nodeName));
		strlcpy(nodeRack, WORKER_DEFAULT_RACK, sizeof(nodeRack));
		memset(nodePortString, '\0', sizeof(nodePortString));

		if (workerLineLength == MAXPGPATH - 1)
		{
			ereport(ERROR, (errcode(ERRCODE_CONFIG_FILE_ERROR),
							errmsg("worker node list file line exceeds the maximum "
								   "length of %d", MAXPGPATH)));
		}

		/* trim trailing newlines preserved by fgets, if any */
		linePointer = workerNodeLine + workerLineLength - 1;
		while (linePointer >= workerNodeLine &&
			   (*linePointer == '\n' || *linePointer == '\r'))
		{
			*linePointer-- = '\0';
		}

		/* skip leading whitespace */
		for (linePointer = workerNodeLine; *linePointer; linePointer++)
		{
			if (!isspace((unsigned char) *linePointer))
			{
				break;
			}
		}

		/* if the entire line is whitespace or a comment, skip it */
		if (*linePointer == '\0' || *linePointer == '#')
		{
			continue;
		}

		/* parse line; node name is required, but port and rack are optional */
		fieldCount = sscanf(linePointer, workerLinePattern,
							nodeName, nodePortString, nodeRack);

		/* adjust field count for zero based indexes */
		fieldCount--;

		/* raise error if no fields were assigned */
		if (fieldCount < workerNameIndex)
		{
			lineIsInvalid = true;
		}

		/* no special treatment for nodeName: already parsed by sscanf */

		/* if a second token was specified, convert to integer port */
		if (fieldCount >= workerPortIndex)
		{
			char *nodePortEnd = NULL;

			errno = 0;
			nodePort = strtol(nodePortString, &nodePortEnd, 10);

			if (errno != 0 || (*nodePortEnd) != '\0' || nodePort <= 0)
			{
				lineIsInvalid = true;
			}
		}

		if (lineIsInvalid)
		{
			ereport(ERROR, (errcode(ERRCODE_CONFIG_FILE_ERROR),
							errmsg("could not parse worker node line: %s",
								   workerNodeLine),
							errhint("Lines in the worker node file must contain a valid "
									"node name and, optionally, a positive port number. "
									"Comments begin with a '#' character and extend to "
									"the end of their line.")));
		}

		/* allocate worker node structure and set fields */
		workerNode = (WorkerNode *) palloc0(sizeof(WorkerNode));

		strlcpy(workerNode->workerName, nodeName, WORKER_LENGTH);
		strlcpy(workerNode->workerRack, nodeRack, WORKER_LENGTH);
		workerNode->workerPort = nodePort;
		workerNode->inWorkerFile = true;

		workerNodeList = lappend(workerNodeList, workerNode);
	}

	FreeFile(workerFileStream);
	free(workerFilePath);

	return workerNodeList;
}


/* Marks all worker nodes in the shared hash as stale. */
static void
ResetWorkerNodesHash(HTAB *WorkerNodesHash)
{
	WorkerNode *workerNode = NULL;

	HASH_SEQ_STATUS status;
	hash_seq_init(&status, WorkerNodesHash);

	workerNode = (WorkerNode *) hash_seq_search(&status);
	while (workerNode != NULL)
	{
		workerNode->inWorkerFile = false;

		workerNode = (WorkerNode *) hash_seq_search(&status);
	}
}


/* ResponsiveWorkerNodeList returns a list of all responsive worker nodes */
List *
ResponsiveWorkerNodeList(void)
{
	List *responsiveWorkerNodeList = NULL;
	ListCell *workerNodeCell = NULL;
	List *workerNodeList = WorkerNodeList();

	foreach(workerNodeCell, workerNodeList)
	{
		bool workerNodeResponsive = false;
		WorkerNode *workerNode = lfirst(workerNodeCell);

		workerNodeResponsive = WorkerNodeResponsive(workerNode->workerName,
													workerNode->workerPort);
		if (workerNodeResponsive)
		{
			responsiveWorkerNodeList = lappend(responsiveWorkerNodeList, workerNode);
		}
	}

	return responsiveWorkerNodeList;
}


/*
 * WorkerNodeResponsive returns true if the given worker node is reponsive.
 * Otherwise, it returns false.
 *
 * This function is based on worker_node_responsive function present in the
 * shard rebalancer.
 */
static bool
WorkerNodeResponsive(const char *workerName, uint32 workerPort)
{
	bool workerNodeResponsive = false;

	int connectionId = MultiClientConnect(workerName, workerPort, NULL, NULL);
	if (connectionId != INVALID_CONNECTION_ID)
	{
		MultiClientDisconnect(connectionId);

		workerNodeResponsive = true;
	}

	return workerNodeResponsive;
}
