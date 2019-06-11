CREATE OR REPLACE VIEW v_fk_cons AS
WITH
fk_constraints AS (
  SELECT
    pg_constraint.oid,
    pg_constraint.conname AS name,
    CASE pg_constraint.confdeltype
      WHEN 'a'::CHAR THEN 'NO ACTION'::text
      WHEN 'c'::CHAR THEN 'CASCADE'::text
      WHEN 'd'::CHAR THEN 'DEFAULT'::text
      WHEN 'n'::CHAR THEN 'SET NULL'::text
      WHEN 'r'::CHAR THEN 'RESTRICT'::text
      ELSE NULL::text
    END AS delete_rule,
    pg_constraint.conrelid,
    pg_constraint.confrelid,
    pg_constraint.conkey,
    pg_constraint.confkey,
    pg_ns_tcon.nspname AS ctab_schema_name,
    pg_class_tcon.relname AS ctab_name,
    pg_ns_trel.nspname AS ptab_schema_name,
    pg_class_trel.relname AS ptab_name
   FROM
    pg_constraint, pg_namespace pg_ns_tcon, pg_class pg_class_tcon,
    pg_namespace pg_ns_trel, pg_class pg_class_trel
  WHERE
    pg_constraint.conrelid = pg_class_tcon.oid AND pg_class_tcon.relnamespace = pg_ns_tcon.oid
      AND
    pg_constraint.confrelid = pg_class_trel.oid AND pg_class_trel.relnamespace = pg_ns_trel.oid
      AND
    pg_constraint.contype = 'f'::CHAR
),
ctab_pk_attrs AS (
  SELECT
    fk_constraints.oid,
    array_agg(pg_attribute.attname ORDER BY pg_index.indkey_subscript) AS ctab_pk_col_names,
    array_agg(format_type(pg_attribute.atttypid, pg_attribute.atttypmod) ORDER BY pg_index.indkey_subscript) AS ctab_pk_col_types
  FROM
    fk_constraints
      INNER JOIN
    pg_attribute
        ON
          fk_constraints.conrelid = pg_attribute.attrelid
      INNER JOIN
    (SELECT *, generate_subscripts(indkey, 1) AS indkey_subscript FROM pg_index) AS pg_index
        ON
          pg_attribute.attrelid = pg_index.indrelid AND pg_attribute.attnum = pg_index.indkey[pg_index.indkey_subscript]
            AND
          pg_index.indisprimary
  GROUP BY
    fk_constraints.oid
),
ctab_fk_attrs AS (
  SELECT
    fk_constraints.oid,
    array_agg(pg_attribute.attname ORDER BY fk_constraints.conkey_subscript) AS ctab_fk_col_names,
    array_agg(format_type(pg_attribute.atttypid, pg_attribute.atttypmod) ORDER BY fk_constraints.conkey_subscript) AS ctab_fk_col_types
  FROM
    pg_attribute
      INNER JOIN
    (SELECT *, generate_subscripts(fk_constraints.conkey, 1) AS conkey_subscript FROM fk_constraints) AS fk_constraints
        ON
          pg_attribute.attrelid = fk_constraints.conrelid AND pg_attribute.attnum = fk_constraints.conkey[fk_constraints.conkey_subscript]
  GROUP BY
    fk_constraints.oid
),
ptab_pk_attrs AS (
  SELECT
    fk_constraints.oid,
    array_agg(pg_attribute.attname ORDER BY pg_index.indkey_subscript) AS ptab_pk_col_names,
    array_agg(format_type(pg_attribute.atttypid, pg_attribute.atttypmod) ORDER BY pg_index.indkey_subscript) AS ptab_pk_col_types
  FROM
    fk_constraints
      INNER JOIN
    pg_attribute
        ON
          fk_constraints.confrelid = pg_attribute.attrelid
      INNER JOIN
    (SELECT *, generate_subscripts(indkey, 1) AS indkey_subscript FROM pg_index) AS pg_index
        ON
          pg_attribute.attrelid = pg_index.indrelid AND pg_attribute.attnum = pg_index.indkey[pg_index.indkey_subscript]
            AND
          pg_index.indisprimary
  GROUP BY
    fk_constraints.oid
)
SELECT
  fk_constraints.oid,
  fk_constraints.name,
  fk_constraints.delete_rule,
  fk_constraints.conrelid AS ctab_oid,
  fk_constraints.ctab_schema_name,
  fk_constraints.ctab_name,
  ctab_pk_attrs.ctab_pk_col_names,
  ctab_pk_attrs.ctab_pk_col_types,
  ctab_fk_attrs.ctab_fk_col_names,
  ctab_fk_attrs.ctab_fk_col_types,
  fk_constraints.confrelid AS ptab_oid,
  fk_constraints.ptab_schema_name,
  fk_constraints.ptab_name,
  ptab_pk_attrs.ptab_pk_col_names,
  ptab_pk_attrs.ptab_pk_col_types
FROM fk_constraints
  INNER JOIN ctab_pk_attrs USING (oid)
  INNER JOIN ctab_fk_attrs USING (oid)
  INNER JOIN ptab_pk_attrs USING (oid)
ORDER BY fk_constraints.name;
