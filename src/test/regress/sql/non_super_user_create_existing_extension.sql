CREATE SCHEMA non_super_user_create_existing_extension;
SET search_path TO non_super_user_create_existing_extension;

SELECT CURRENT_USER;
CREATE EXTENSION if not exists "uuid-ossp" SCHEMA public;
CREATE EXTENSION if not exists "uuid-ossp" SCHEMA public;
CREATE USER non_super_user_ext;
SET ROLE non_super_user_ext;
SELECT CURRENT_USER;
CREATE EXTENSION if not exists "uuid-ossp" SCHEMA public;
RESET ROLE;
SELECT CURRENT_USER;
DROP USER non_super_user_ext;
DROP SCHEMA non_super_user_create_existing_extension CASCADE;
