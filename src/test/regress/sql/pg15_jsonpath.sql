--
-- PG15 jsonpath tests
-- Relevant pg commit: e26114c817b610424010cfbe91a743f591246ff1
--
CREATE SCHEMA jsonpath;
SET search_path TO jsonpath;

CREATE TABLE jsonpath_test (id serial, sample text);
SELECT create_distributed_table('jsonpath_test', 'id');
\COPY jsonpath_test(sample) FROM STDIN
$
strict $
lax $
$.a
$.a.v
$.a.*
$.*[*]
$.a[*]
$.a[*][*]
$[*]
$[0]
$[*][0]
$[*].a
$[*][0].a.b
$.a.**.b
$.a.**{2}.b
$.a.**{2 to 2}.b
$.a.**{2 to 5}.b
$.a.**{0 to 5}.b
$.a.**{5 to last}.b
$.a.**{last}.b
$.a.**{last to 5}.b
$+1
$-1
$--+1
$.a/+-1
1 * 2 + 4 % -3 != false
$.g ? ($.a == 1)
$.g ? (@ == 1)
$.g ? (@.a == 1)
$.g ? (@.a == 1 || @.a == 4)
$.g ? (@.a == 1 && @.a == 4)
$.g ? (@.a == 1 || @.a == 4 && @.b == 7)
$.g ? (@.a == 1 || !(@.a == 4) && @.b == 7)
$.g ? (@.a == 1 || !(@.x >= 123 || @.a == 4) && @.b == 7)
$.g ? (@.x >= @[*]?(@.a > "abc"))
$.g ? ((@.x >= 123 || @.a == 4) is unknown)
$.g ? (exists (@.x))
$.g ? (exists (@.x ? (@ == 14)))
$.g ? ((@.x >= 123 || @.a == 4) && exists (@.x ? (@ == 14)))
$.g ? (+@.x >= +-(+@.a + 2))
$a
$a.b
$a[*]
$.g ? (@.zip == $zip)
$.a[1,2, 3 to 16]
$.a[$a + 1, ($b[*]) to -($[0] * 2)]
$.a[$.a.size() - 3]
"last"
$.last
$[last]
$[$[0] ? (last > 0)]
null.type()
(1).type()
1.2.type()
"aaa".type()
true.type()
$.double().floor().ceiling().abs()
$.keyvalue().key
$.datetime()
$.datetime("datetime template")
$ ? (@ starts with "abc")
$ ? (@ starts with $var)
$ ? (@ like_regex "pattern")
$ ? (@ like_regex "pattern" flag "")
$ ? (@ like_regex "pattern" flag "i")
$ ? (@ like_regex "pattern" flag "is")
$ ? (@ like_regex "pattern" flag "isim")
$ ? (@ like_regex "pattern" flag "q")
$ ? (@ like_regex "pattern" flag "iq")
$ ? (@ like_regex "pattern" flag "smixq")
$ < 1
($ < 1) || $.a.b <= $x
($).a.b
($.a.b).c.d
($.a.b + -$.x.y).c.d
(-+$.a.b).c.d
1 + ($.a.b + 2).c.d
1 + ($.a.b > 2).c.d
($)
(($))
((($ + 1)).a + ((2)).b ? ((((@ > 1)) || (exists(@.c)))))
$ ? (@.a < 1)
$ ? (@.a < -1)
$ ? (@.a < +1)
$ ? (@.a < .1)
$ ? (@.a < -.1)
$ ? (@.a < +.1)
$ ? (@.a < 0.1)
$ ? (@.a < -0.1)
$ ? (@.a < +0.1)
$ ? (@.a < 10.1)
$ ? (@.a < -10.1)
$ ? (@.a < +10.1)
$ ? (@.a < 1e1)
$ ? (@.a < -1e1)
$ ? (@.a < +1e1)
$ ? (@.a < .1e1)
$ ? (@.a < -.1e1)
$ ? (@.a < +.1e1)
$ ? (@.a < 0.1e1)
$ ? (@.a < -0.1e1)
$ ? (@.a < +0.1e1)
$ ? (@.a < 10.1e1)
$ ? (@.a < -10.1e1)
$ ? (@.a < +10.1e1)
$ ? (@.a < 1e-1)
$ ? (@.a < -1e-1)
$ ? (@.a < +1e-1)
$ ? (@.a < .1e-1)
$ ? (@.a < -.1e-1)
$ ? (@.a < +.1e-1)
$ ? (@.a < 0.1e-1)
$ ? (@.a < -0.1e-1)
$ ? (@.a < +0.1e-1)
$ ? (@.a < 10.1e-1)
$ ? (@.a < -10.1e-1)
$ ? (@.a < +10.1e-1)
$ ? (@.a < 1e+1)
$ ? (@.a < -1e+1)
$ ? (@.a < +1e+1)
$ ? (@.a < .1e+1)
$ ? (@.a < -.1e+1)
$ ? (@.a < +.1e+1)
$ ? (@.a < 0.1e+1)
$ ? (@.a < -0.1e+1)
$ ? (@.a < +0.1e+1)
$ ? (@.a < 10.1e+1)
$ ? (@.a < -10.1e+1)
$ ? (@.a < +10.1e+1)
0
0.0
0.000
0.000e1
0.000e2
0.000e3
0.0010
0.0010e-1
0.0010e+1
0.0010e+2
.001
.001e1
1.
1.e1
1.2.e
(1.2).e
1e3
1.e3
1.e3.e
1.e3.e4
1.2e3
1.2.e3
(1.2).e3
1..e
1..e3
(1.).e
(1.).e3
1?(2>3)
\.

-- Cast the text into jsonpath on the worker nodes.
SELECT sample, sample::jsonpath FROM jsonpath_test ORDER BY id;

-- Pull the data, and cast on the coordinator node
WITH samples as (SELECT id, sample FROM jsonpath_test OFFSET 0)
SELECT sample, sample::jsonpath FROM samples ORDER BY id;

-- now test some cases where trailing junk causes errors
\COPY jsonpath_test(sample) FROM STDIN

last
1.type()
$ ? (@ like_regex "(invalid pattern")
$ ? (@ like_regex "pattern" flag "xsms")
@ + 1
00
1.e
1.2e3a
\.

-- the following tests try to evaluate type casting on worker, followed by coordinator
SELECT sample, sample::jsonpath FROM jsonpath_test WHERE sample = '';
WITH samples as (SELECT id, sample FROM jsonpath_test WHERE sample = '' OFFSET 0)
SELECT sample, sample::jsonpath FROM samples;

SELECT sample, sample::jsonpath FROM jsonpath_test WHERE sample = 'last';
WITH samples as (SELECT id, sample FROM jsonpath_test WHERE sample = 'last' OFFSET 0)
SELECT sample, sample::jsonpath FROM samples;

SELECT sample, sample::jsonpath FROM jsonpath_test WHERE sample = '1.type()';
WITH samples as (SELECT id, sample FROM jsonpath_test WHERE sample = '1.type()' OFFSET 0)
SELECT sample, sample::jsonpath FROM samples;

SELECT sample, sample::jsonpath FROM jsonpath_test WHERE sample = '$ ? (@ like_regex "(invalid pattern")';
WITH samples as (SELECT id, sample FROM jsonpath_test WHERE sample = '$ ? (@ like_regex "(invalid pattern")' OFFSET 0)
SELECT sample, sample::jsonpath FROM samples;

SELECT sample, sample::jsonpath FROM jsonpath_test WHERE sample = '$ ? (@ like_regex "pattern" flag "xsms")';
WITH samples as (SELECT id, sample FROM jsonpath_test WHERE sample = '$ ? (@ like_regex "pattern" flag "xsms")' OFFSET 0)
SELECT sample, sample::jsonpath FROM samples;

SELECT sample, sample::jsonpath FROM jsonpath_test WHERE sample = '@ + 1';
WITH samples as (SELECT id, sample FROM jsonpath_test WHERE sample = '@ + 1' OFFSET 0)
SELECT sample, sample::jsonpath FROM samples;

SELECT sample, sample::jsonpath FROM jsonpath_test WHERE sample = '00';
WITH samples as (SELECT id, sample FROM jsonpath_test WHERE sample = '00' OFFSET 0)
SELECT sample, sample::jsonpath FROM samples;

SELECT sample, sample::jsonpath FROM jsonpath_test WHERE sample = '1.e';
WITH samples as (SELECT id, sample FROM jsonpath_test WHERE sample = '1.e' OFFSET 0)
SELECT sample, sample::jsonpath FROM samples;

SELECT sample, sample::jsonpath FROM jsonpath_test WHERE sample = '1.2e3a';
WITH samples as (SELECT id, sample FROM jsonpath_test WHERE sample = '1.2e3a' OFFSET 0)
SELECT sample, sample::jsonpath FROM samples;

DROP SCHEMA jsonpath CASCADE;
