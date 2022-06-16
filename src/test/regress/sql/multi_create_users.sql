--
-- MULTI_CREATE_USERS
--
-- Create users on all nodes, they're currently automatically
-- replicated.
--

CREATE USER full_access;
CREATE USER read_access;
CREATE USER no_access;

-- allow access to various users
GRANT ALL ON TABLE lineitem, orders, lineitem, customer, nation, part, supplier TO full_access;
GRANT SELECT ON TABLE lineitem, orders, lineitem, customer, nation, part, supplier TO read_access;
