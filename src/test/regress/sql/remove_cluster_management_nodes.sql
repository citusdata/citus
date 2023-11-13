-- When needed in some tests, GetLocalNodeId() does not behave correctly.
SELECT citus_remove_node('localhost', 8887);
SELECT citus_remove_node('localhost', 9995);
SELECT citus_remove_node('localhost', 9992);
SELECT citus_remove_node('localhost', 9998);
SELECT citus_remove_node('localhost', 9997);
SELECT citus_remove_node('localhost', 8888);
