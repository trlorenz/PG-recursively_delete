CREATE EXTENSION recursively_delete VERSION '0.1.5';

SELECT extversion
FROM pg_catalog.pg_extension
WHERE extname = 'recursively_delete';
