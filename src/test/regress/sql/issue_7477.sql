
--- Test for updating a table that has a foreign key reference to another reference table.
--- Issue #7477: Distributed deadlock after issuing a simple UPDATE statement
--- https://github.com/citusdata/citus/issues/7477

CREATE TABLE table1 (id INT PRIMARY KEY);
SELECT create_reference_table('table1');
INSERT INTO table1 VALUES (1);

CREATE TABLE table2 (
    id INT,
    info TEXT,
    CONSTRAINT table1_id_fk FOREIGN KEY (id) REFERENCES table1 (id)
    );
SELECT create_reference_table('table2');
INSERT INTO table2 VALUES (1, 'test');

--- Runs the update command in parallel on workers.
--- Due to bug #7477, before the fix, the result is non-deterministic
--- and have several rows of the form:
---  localhost |     57638 | f       | ERROR:  deadlock detected
---  localhost |     57637 | f       | ERROR:  deadlock detected
---  localhost |     57637 | f       | ERROR:  canceling the transaction since it was involved in a distributed deadlock

SELECT * FROM master_run_on_worker(
    ARRAY['localhost', 'localhost','localhost', 'localhost','localhost',
            'localhost','localhost', 'localhost','localhost', 'localhost']::text[],
    ARRAY[57638, 57637, 57637, 57638, 57637, 57638, 57637, 57638, 57638, 57637]::int[],
    ARRAY['UPDATE table2 SET info = ''test_update'' WHERE id = 1',
            'UPDATE table2 SET info = ''test_update'' WHERE id = 1',
            'UPDATE table2 SET info = ''test_update'' WHERE id = 1',
            'UPDATE table2 SET info = ''test_update'' WHERE id = 1',
            'UPDATE table2 SET info = ''test_update'' WHERE id = 1',
            'UPDATE table2 SET info = ''test_update'' WHERE id = 1',
            'UPDATE table2 SET info = ''test_update'' WHERE id = 1',
            'UPDATE table2 SET info = ''test_update'' WHERE id = 1',
            'UPDATE table2 SET info = ''test_update'' WHERE id = 1',
            'UPDATE table2 SET info = ''test_update'' WHERE id = 1'
            ]::text[],
    true);

--- cleanup
DROP TABLE table2;
DROP TABLE table1;
