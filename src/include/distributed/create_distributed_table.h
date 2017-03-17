/*
 * create_distributed_table.h
 *
 *  Created on: Mar 17, 2017
 *      Author: velioglub
 */

#ifndef SRC_INCLUDE_DISTRIBUTED_CREATE_DISTRIBUTED_TABLE_H_
#define SRC_INCLUDE_DISTRIBUTED_CREATE_DISTRIBUTED_TABLE_H_

extern void ErrorIfNotSupportedConstraint(Relation relation,
												 char distributionMethod,
												 Var *distributionColumn,
												 uint32 colocationId);

#endif /* SRC_INCLUDE_DISTRIBUTED_CREATE_DISTRIBUTED_TABLE_H_ */
