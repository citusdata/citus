setup
{
	create user myuser;
	create user myuser2;
	create user myuser3;


}

teardown
{
    DROP TABLE employee,company,city;
}

session "s1"

step "s1-begin"
{
    BEGIN;
}

step "s1-grant-create-db"
{
	grant create on database postgres to myuser;
}

step "s1-grant-create-db"
{
	set session authorization myuser;
}


step "s1-create schema"
{
	create schema myschema;
}

step "s1-drop schema"
{
	drop schema myschema;
}

step "s1-end"{
	COMMIT;
}




permutation "s1-begin"  "s1-grant-create-db" "s1-grant-create-db" "s1-create schema" "s1-drop-schema" "s1-end"

