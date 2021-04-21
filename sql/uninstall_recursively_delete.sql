/*
 * Author: <trlorenz@hotmail.com>
 * Created at: 2021-04-21 13:46:19 -0700
 *
 */

SET client_min_messages = warning;

BEGIN;
DROP FUNCTION recursively_delete(regclass, anyelement, boolean);
DROP FUNCTION _recursively_delete (regclass, text[], int, jsonb, int, text[], jsonb, jsonb);
DROP VIEW v_fk_cons;
COMMIT;
