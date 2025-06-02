# Global Secondary Index in Citus

## 1. What is a Global Secondary Index?

A **global secondary index (GSI)** in Citus is an index built on one or more columns of a **distributed table**, where those columns are **not** the distribution key. Unlike local indexes that exist only within individual shards, a GSI provides **global visibility** and optionally enforces **uniqueness constraints** across all shards. Since Citus guarantees **strong consistency**, GSIs must be designed to preserve correctness under concurrent operations.

The requirement is twofold: the secondary index should assist in routing the query to the correct node, and once there, enable faster tuple lookup within the shard.

---

## 2. Example Use Case

Suppose we have a distributed `users` table:

```sql
CREATE TABLE users (
    user_id UUID PRIMARY KEY,
    email TEXT,
    created_at TIMESTAMP
);

SELECT create_distributed_table('users', 'user_id');
```

We want `email` to be searchable and possibly enforce global uniqueness. Without a GSI, queries like:

```sql
SELECT * FROM users WHERE email = 'alice@example.com';
```

would result in scatter-gather queries. With a GSI on `email`, we can:
- Route queries to the correct node without scatter-gather.
- Support fast index-based lookups.
- Optionally enforce uniqueness.

---

## 3. Preferred Implementation Path

Citus already supports creating indexes on non-distributed columns using a standard `CREATE INDEX` command. For example:

```sql
CREATE INDEX gsi ON dist_table (nonid);
```

This results in:
```
NOTICE:  issuing CREATE INDEX gsi_102037 ON citus.dist_table_102037 USING btree (nonid)
NOTICE:  issuing CREATE INDEX gsi_102038 ON citus.dist_table_102038 USING btree (nonid)
NOTICE:  issuing CREATE INDEX gsi_102039 ON citus.dist_table_102039 USING btree (nonid)
```

This behavior already creates **per-shard indexes** on each node. To complete the GSI feature:

### Step 1: Reference Table for Routing

Add a reference table (coordinator-local) that maps the secondary index column to the distribution column:

```sql
CREATE TABLE dist_table_gsi (
  nonid INTEGER,
  id INTEGER
);
SELECT create_reference_table('dist_table_gsi');
```

### Step 2: Triggers on Distributed Table

Maintain consistency using triggers:

```sql
CREATE FUNCTION populate_gsi() RETURNS trigger AS $$
BEGIN
  INSERT INTO dist_table_gsi(nonid, id) VALUES (NEW.nonid, NEW.id);
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_gsi_insert
AFTER INSERT ON dist_table
FOR EACH ROW EXECUTE FUNCTION populate_gsi();
```

Handle `UPDATE` and `DELETE` similarly to keep the reference table accurate.

### Step 3: Planner Rewrite

Intercept queries such as:
```sql
SELECT * FROM dist_table WHERE nonid = 42;
```
And rewrite them internally as:
```sql
SELECT * FROM dist_table WHERE id IN (
  SELECT id FROM dist_table_gsi WHERE nonid = 42
);
```
This helps route to the correct node(s) and leverage the local index there.

### Step 4: Integration with `CREATE INDEX`

Extend the `CREATE INDEX` logic such that if the target column is non-distributed:
- Citus creates shard-local indexes
- Sets up a GSI reference table and associated triggers
- Registers the reference table metadata for planner use

### Handling Non-Unique Keys

For non-unique cases (e.g., two users with same email):
```sql
email -> [uid1, uid2, ...]
```
The reference table supports this by simply storing multiple entries:
```sql
foo@msn.com | 100
foo@msn.com | 202
```

---

## 4. Summary

Citus already lays the groundwork for a global secondary index by allowing local indexes to be built across shards using `CREATE INDEX`. The remaining pieces involve adding a reference table on the coordinator, maintaining it via triggers, and enabling planner rewrites to use it. This path aligns with Citus' strong consistency guarantees and keeps the user experience seamless by building the GSI behavior into the standard `CREATE INDEX` workflow.

