set time zone PST8PDT;
-- Date and timestamp
DROP FUNCTION IF EXISTS datefunc1_nochange(date);
DROP FUNCTION IF EXISTS datefunc1(date);
DROP FUNCTION IF EXISTS datefunc2(date);
DROP FUNCTION IF EXISTS datefunc3_year(date);
DROP FUNCTION IF EXISTS datefunc3_mon(date);
DROP FUNCTION IF EXISTS datefunc3_mday(date);
DROP FUNCTION IF EXISTS timefunc1(time);
DROP FUNCTION IF EXISTS timetzfunc1(timetz);
DROP FUNCTION IF EXISTS timestampfunc1(timestamp);
DROP FUNCTION IF EXISTS timestamptzfunc1(timestamptz);

CREATE OR REPLACE FUNCTION datefunc1_nochange(date) RETURNS date AS '$libdir/gppc_test' LANGUAGE c IMMUTABLE STRICT;
CREATE OR REPLACE FUNCTION datefunc1(date) RETURNS date AS '$libdir/gppc_test' LANGUAGE c IMMUTABLE STRICT;
CREATE OR REPLACE FUNCTION datefunc2(date) RETURNS date AS '$libdir/gppc_test' LANGUAGE c IMMUTABLE STRICT;
CREATE OR REPLACE FUNCTION datefunc3_year(date) RETURNS date AS '$libdir/gppc_test' LANGUAGE c IMMUTABLE STRICT;
CREATE OR REPLACE FUNCTION datefunc3_mon(date) RETURNS date AS '$libdir/gppc_test' LANGUAGE c IMMUTABLE STRICT;
CREATE OR REPLACE FUNCTION datefunc3_mday(date) RETURNS date AS '$libdir/gppc_test' LANGUAGE c IMMUTABLE STRICT;
CREATE OR REPLACE FUNCTION timefunc1(time) RETURNS time AS '$libdir/gppc_test' LANGUAGE c IMMUTABLE STRICT;
CREATE OR REPLACE FUNCTION timetzfunc1(timetz) RETURNS timetz AS '$libdir/gppc_test' LANGUAGE c STABLE STRICT;
CREATE OR REPLACE FUNCTION timestampfunc1(timestamp) RETURNS timestamp AS '$libdir/gppc_test' LANGUAGE c IMMUTABLE STRICT;
CREATE OR REPLACE FUNCTION timestamptzfunc1(timestamptz) RETURNS timestamptz AS '$libdir/gppc_test' LANGUAGE c STABLE STRICT;

SELECT datefunc1_nochange('1900-01-01');
SELECT datefunc1('1898-12-31');
SELECT datefunc1('2012-02-29');
SELECT datefunc2('2013-03-01');
SELECT datefunc3_year('1900-01-01');
SELECT datefunc3_year('00-14-37');
SELECT datefunc3_year('02-11-03');
SELECT datefunc3_mon('2012-01-29');
SELECT datefunc3_mon('2012-03-29');
SELECT datefunc3_mon('2011-03-29');
SELECT datefunc3_mday('2012-03-01');
SELECT datefunc3_mday('2013-03-01');
SELECT datefunc3_mday('1900-01-01');
SELECT timefunc1('15:00:01');
SELECT timetzfunc1('15:00:01 UTC');
SELECT timestampfunc1('2011-02-24 15:00:01');
SELECT timestamptzfunc1('2011-02-24 15:00:01 UTC');
