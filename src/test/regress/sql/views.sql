SET search_path TO views_create;

SELECT * FROM "local regular view";
SELECT * FROM dist_regular_view;
SELECT * FROM local_regular_view2;
SELECT * FROM local_regular_view3;
SELECT * FROM "local regular view4";

RESET search_path;
