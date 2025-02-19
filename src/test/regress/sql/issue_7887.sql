CREATE SCHEMA issue_7887;
CREATE SCHEMA issue_7887;

CREATE TABLE local1 (
    id text not null primary key
);

CREATE TABLE reference1 (
    id int not null primary key,
    reference_col1 text not null
);
SELECT create_reference_table('reference1');

CREATE TABLE local2 (
    id int not null generated always as identity,
    local1fk text not null,
    reference1fk int not null,
    constraint loc1fk foreign key (local1fk) references local1(id),
    constraint reference1fk foreign key (reference1fk) references reference1(id),
    constraint testlocpk primary key (id)
);

INSERT INTO local1(id) VALUES ('aaaaa'), ('bbbbb'), ('ccccc');
INSERT INTO reference1(id, reference_col1) VALUES (1, 'test'), (2, 'test2'), (3, 'test3');

-- The statement that triggers the bug:
INSERT INTO local2(local1fk, reference1fk)
    SELECT id, 1
    FROM local1;

-- If you want to see the error in the regression output, you might do something like:
-- NOTE: The next line is typically how you'd test for an error in a .sql regression test
-- but with a custom "expected" file you'll confirm you get the "invalid string enlargement request size: -4" text
