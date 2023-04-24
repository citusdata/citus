/*-------------------------------------------------------------------------
 *
 * remote_subscription.h
 *	  definition of remote_subscription data types
 *
 * Copyright (c), Microsoft, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef REMOTE_SUBSCRIPTION_H
#define REMOTE_SUBSCRIPTION_H


bool RemoteSubscriptionExists(MultiConnection *conn, char *subscriptionName);
void CreateRemoteSubscription(MultiConnection *conn,
							  char *sourceConnectionString, char *subscriptionName,
							  char *publicationName, char *slotName);
void DropRemoteSubscription(MultiConnection *conn, char *subscriptionName);


#endif
