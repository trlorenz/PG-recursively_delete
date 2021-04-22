\echo Use "CREATE EXTENSION recursively_delete" to load this file. \quit

SET client_min_messages = warning;

CREATE VIEW v_fk_cons AS
WITH
fk_constraints AS (
  SELECT
    pg_constraint.oid,
    pg_constraint.conname AS name,
    pg_constraint.confdeltype AS delete_action,
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
ptab_uk_attrs AS (
  SELECT
    fk_constraints.oid,
    array_agg(pg_attribute.attname ORDER BY fk_constraints.confkey_subscript) AS ptab_uk_col_names,
    array_agg(format_type(pg_attribute.atttypid, pg_attribute.atttypmod) ORDER BY fk_constraints.confkey_subscript) AS ptab_uk_col_types
  FROM
    pg_attribute
      INNER JOIN
    (SELECT *, generate_subscripts(fk_constraints.confkey, 1) AS confkey_subscript FROM fk_constraints) AS fk_constraints
        ON
          pg_attribute.attrelid = fk_constraints.confrelid AND pg_attribute.attnum = fk_constraints.confkey[fk_constraints.confkey_subscript]
  GROUP BY
    fk_constraints.oid
)
SELECT
  fk_constraints.oid,
  fk_constraints.name,
  fk_constraints.delete_action,
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
  ptab_uk_attrs.ptab_uk_col_names,
  ptab_uk_attrs.ptab_uk_col_types
FROM fk_constraints
  INNER JOIN ctab_pk_attrs USING (oid)
  INNER JOIN ctab_fk_attrs USING (oid)
  INNER JOIN ptab_uk_attrs USING (oid);

CREATE FUNCTION _recursively_delete(
         ARG_table           REGCLASS                         ,
         ARG_pk_col_names    TEXT[]                           ,
        _ARG_depth           INT      DEFAULT  0              ,
        _ARG_fk_con          JSONB    DEFAULT  NULL           ,
        _ARG_flat_graph_i_up INT      DEFAULT  NULL           ,
        _ARG_path            TEXT[]   DEFAULT  ARRAY[]::TEXT[],
  INOUT _ARG_circ_deps       JSONB    DEFAULT '[]'            ,
  INOUT _ARG_flat_graph      JSONB    DEFAULT '[]'
)
LANGUAGE plpgsql
AS $$
DECLARE
  VAR_circ_dep          JSONB ;
  VAR_ctab_fk_col_names JSONB ;
  VAR_ctab_pk_col_names JSONB ;
  VAR_fk_con_rec        RECORD;
  VAR_flat_graph_i      INT   ;
  VAR_flat_graph_node   JSONB ;
  VAR_i                 INT   ;
  VAR_path_pos_of_oid   INT   ;
BEGIN
  IF _ARG_depth = 0 THEN
    _ARG_path := _ARG_path || ARRAY['ROOT'];

    VAR_ctab_pk_col_names := array_to_json(ARG_pk_col_names)::JSONB;

    -- Not really a statement of truth, but a convenient thing to pretend. For the initial
    -- "bootstrap" CTE auxiliary statement, this HACK takes care of equating the user's ARG_in with
    -- the table's primary key.
    VAR_ctab_fk_col_names := VAR_ctab_pk_col_names;
  ELSE
    VAR_ctab_fk_col_names := _ARG_fk_con->>'ctab_fk_col_names';
    VAR_ctab_pk_col_names := _ARG_fk_con->>'ctab_pk_col_names';
  END IF;

  VAR_flat_graph_i := jsonb_array_length(_ARG_flat_graph);

  -- The "flat graph" is a collection of foreign-key-constraint-describing objects ("nodes")
  -- arranged in correct order for composition of the final query.

  -- (Note that while flat_graph_node->>'i' always reports the index of a flat graph node in the
  -- flat graph, flat_graph_node->>'i_up' reports the index of the node's parent, which isn't
  -- necessarily i - 1.)
  _ARG_flat_graph := _ARG_flat_graph || jsonb_build_object(
    'ctab_fk_col_names',  VAR_ctab_fk_col_names,
    'ctab_name'        ,  ARG_table,
    'ctab_oid'         , _ARG_fk_con->>'ctab_oid',
    'ctab_pk_col_names',  VAR_ctab_pk_col_names,
    'cte_aux_stmt_name',  format('del_%s$%s', VAR_flat_graph_i, _ARG_path[array_upper(_ARG_path, 1)]),
    'delete_action'    , _ARG_fk_con->>'delete_action',
    'depth'            , _ARG_depth,
    'i'                ,  VAR_flat_graph_i,
    'i_up'             , _ARG_flat_graph_i_up,
    'path'             ,  array_to_json(_ARG_path)::JSONB,
    'ptab_uk_col_names', _ARG_fk_con->'ptab_uk_col_names'
  );

  IF _ARG_fk_con->>'delete_action' IN ('d', 'n') THEN
    RETURN;
  END IF;

  <<FK_CON>>
  FOR VAR_fk_con_rec IN (SELECT * FROM v_fk_cons WHERE ptab_oid = ARG_table ORDER BY name) LOOP
    VAR_path_pos_of_oid := array_position(_ARG_path, VAR_fk_con_rec.oid::TEXT);

    IF VAR_path_pos_of_oid IS NOT NULL THEN
    --^ If the id of the foreign key constraint already exists in the current path, we know the
    -- constraint is the beginning (ending?) of a circular dependency.

      VAR_circ_dep := '[]';

      -- Populate VAR_circ_dep with the interdependent queue elements comprising the circle (which
      -- we'll call "deppers").
      FOR VAR_i IN VAR_path_pos_of_oid .. array_length(_ARG_path, 1) LOOP
        FOR VAR_flat_graph_node IN SELECT jsonb_array_elements(_ARG_flat_graph) LOOP
          IF VAR_flat_graph_node->'path' = array_to_json(_ARG_path[1:VAR_i])::JSONB THEN
            VAR_circ_dep := VAR_circ_dep || VAR_flat_graph_node;

            EXIT;
          END IF;
        END LOOP;
      END LOOP;

      -- Make the first depper in the circle think its parent is the last depper in the circle.
      VAR_circ_dep := jsonb_set(VAR_circ_dep, ARRAY['0', 'i_up'], VAR_circ_dep->-1->'i');

      _ARG_circ_deps := _ARG_circ_deps || jsonb_build_array(VAR_circ_dep);

      CONTINUE FK_CON;
    END IF;

    SELECT * INTO _ARG_circ_deps, _ARG_flat_graph
    FROM _recursively_delete(
       format('%I.%I', VAR_fk_con_rec.ctab_schema_name, VAR_fk_con_rec.ctab_name),
       NULL,
       --
      _ARG_depth + 1,
       row_to_json(VAR_fk_con_rec)::JSONB,
       VAR_flat_graph_i,
      _ARG_path || VAR_fk_con_rec.oid::TEXT,
      --
      _ARG_circ_deps,
      _ARG_flat_graph
    );
  END LOOP;
END;
$$;

DROP FUNCTION IF EXISTS recursively_delete;

CREATE FUNCTION recursively_delete(
  ARG_table     REGCLASS                ,
  ARG_in        ANYELEMENT              ,
  ARG_for_realz BOOL       DEFAULT FALSE
) RETURNS integer
LANGUAGE plpgsql
AS $$
DECLARE
  VAR_version               TEXT      DEFAULT '0.1.3'        ;
  --
  VAR_circ_dep              JSONB                            ;
  VAR_circ_depper           JSONB                            ;
  VAR_circ_depper_up        JSONB                            ;
  VAR_circ_deps             JSONB                            ;
  VAR_ctab_fk_cols_list     TEXT                             ;
  VAR_ctab_pk_cols_list     TEXT                             ;
  VAR_cte_aux_stmts         TEXT[]    DEFAULT ARRAY[]::TEXT[];
  VAR_del_result_rec        RECORD                           ;
  VAR_del_results           JSONB     DEFAULT '{}'           ;
  VAR_del_results_cursor    REFCURSOR                        ;
  VAR_final_query           TEXT                             ;
  VAR_flat_graph            JSONB                            ;
  VAR_flat_graph_node       JSONB                            ;
  VAR_in                    TEXT                             ;
  VAR_join_comperand_l      TEXT                             ;
  VAR_join_comperand_r      TEXT                             ;
  VAR_pk_col_names          TEXT[]    DEFAULT ARRAY[]::TEXT[];
  VAR_recursive_term        TEXT                             ;
  VAR_recursive_term_from   TEXT[]                           ;
  VAR_recursive_term_select TEXT[]                           ;
  VAR_recursive_term_where  TEXT[]                           ;
  VAR_selects_for_union     TEXT[]                           ;
BEGIN
  -- ...
  SELECT array_agg(pg_attribute.attname ORDER BY pg_index.indkey_subscript) AS ptab_pk_col_names INTO VAR_pk_col_names
  FROM
    pg_attribute
      INNER JOIN
    (SELECT *, generate_subscripts(indkey, 1) AS indkey_subscript FROM pg_index) AS pg_index
        ON
          pg_attribute.attrelid = pg_index.indrelid AND pg_attribute.attnum = pg_index.indkey[pg_index.indkey_subscript]
            AND
          pg_index.indisprimary
  WHERE
    pg_attribute.attrelid = ARG_table;

  IF array_length(VAR_pk_col_names, 1) = 1 THEN
    CASE pg_typeof(ARG_in)::TEXT
      WHEN 'character varying', 'text', 'uuid' THEN
        VAR_in := format('%L', ARG_in);
      WHEN 'character varying[]', 'text[]', 'uuid' THEN
        VAR_in := string_agg(format('%L', ael), ', ') FROM unnest(ARG_in) ael;
      WHEN 'integer' THEN
        VAR_in := ARG_in;
      WHEN 'integer[]' THEN
        VAR_in := array_to_string(ARG_in, ', ');
      ELSE
        RAISE 'ARG_in "%" is of an unexpected type: %', ARG_in, pg_typeof(ARG_in);
    END CASE;
  ELSE
    CASE pg_typeof(ARG_in)::TEXT
      WHEN 'character varying[]', 'text[]', 'uuid[]' THEN
        IF array_ndims(ARG_in) = 1 THEN
          VAR_in := format('(%s)', (SELECT string_agg(format('%L', ael), ', ') FROM unnest(ARG_in) ael));
        ELSE
          VAR_in := string_agg(format('(%s)', (SELECT string_agg(format('%L', ael), ', ') FROM jsonb_array_elements_text(ael) ael)), ', ') FROM jsonb_array_elements(array_to_json(ARG_in)::JSONB) ael;
        END IF;
      WHEN 'integer[]' THEN
        IF array_ndims(ARG_in) = 1 THEN
          VAR_in := format('(%s)', array_to_string(ARG_in, ', '));
        ELSE
          VAR_in := string_agg(format('(%s)', (SELECT string_agg(ael, ', ') FROM jsonb_array_elements_text(ael) ael)), ', ') FROM jsonb_array_elements(array_to_json(ARG_in)::JSONB) ael;
        END IF;
      ELSE
        RAISE 'ARG_in "%" for %-column primary key is of an unexpected type: %', ARG_in, array_length(VAR_pk_col_names, 1), pg_typeof(ARG_in);
    END CASE;
  END IF;

  IF (VAR_in IS NULL OR VAR_in = '') THEN
    VAR_in := 'NULL';
  END IF;

  SELECT * INTO VAR_circ_deps, VAR_flat_graph FROM _recursively_delete(ARG_table, VAR_pk_col_names);

  FOR VAR_flat_graph_node IN SELECT jsonb_array_elements(VAR_flat_graph) LOOP
    IF VAR_flat_graph_node->>'delete_action' IN ('d', 'n') THEN
      VAR_cte_aux_stmts := VAR_cte_aux_stmts || format('%I AS (SELECT NULL)', VAR_flat_graph_node->>'cte_aux_stmt_name');
    ELSE
      VAR_recursive_term := NULL;

      IF (VAR_flat_graph_node->>'depth')::INT != 0 THEN
      -- ^The root CTE aux statement is never allowed to be recursive.

        <<LOOP_BUILDING_RECURSIVE_TERM>>
        FOR VAR_circ_dep IN SELECT jsonb_array_elements(VAR_circ_deps) LOOP
          IF VAR_flat_graph_node->>'i' IN (SELECT jsonb_array_elements(VAR_circ_dep)->>'i') THEN
            VAR_recursive_term_from  := ARRAY[]::TEXT[];
            VAR_recursive_term_where := ARRAY[]::TEXT[];

            VAR_recursive_term_select := array_agg(format('%I.%I', format('t%s', VAR_flat_graph_node->>'i'), ael))
              FROM jsonb_array_elements_text(VAR_flat_graph_node->'ctab_pk_col_names') ael;

            FOR VAR_circ_depper IN SELECT * FROM jsonb_array_elements(VAR_circ_dep) LOOP
              VAR_recursive_term_from := VAR_recursive_term_from || format('%s %I',
                VAR_circ_depper->>'ctab_name', format('t%s', VAR_circ_depper->>'i')
              );

              VAR_join_comperand_l := string_agg(format('%I.%I', format('t%s', VAR_circ_depper->>'i'), ael), ', ')
                FROM jsonb_array_elements_text(VAR_circ_depper->'ctab_fk_col_names') ael;

              VAR_circ_depper_up := VAR_flat_graph->((VAR_circ_depper->>'i_up')::INT);

              IF VAR_flat_graph_node->>'ctab_oid' = VAR_circ_depper_up->>'ctab_oid' THEN
                VAR_join_comperand_r := string_agg(format('self_ref.%I', ael), ', ')
                  FROM jsonb_array_elements_text(VAR_flat_graph_node->'ctab_pk_col_names') ael;
              ELSE
                VAR_join_comperand_r := string_agg(format('%I.%I', format('t%s', VAR_circ_depper_up->>'i'), ael), ', ')
                  FROM jsonb_array_elements_text(VAR_circ_depper_up->'ctab_pk_col_names') ael;
              END IF;

              VAR_recursive_term_where := VAR_recursive_term_where || format('(%s) = (%s)', VAR_join_comperand_l, VAR_join_comperand_r);
            END LOOP;

            VAR_recursive_term_from := VAR_recursive_term_from || ARRAY['self_ref'];

            VAR_recursive_term := format('SELECT %s FROM %s WHERE %s',
              array_to_string(VAR_recursive_term_select, ', '),
              array_to_string(VAR_recursive_term_from, ', '),
              array_to_string(VAR_recursive_term_where , ' AND ')
            );

            EXIT LOOP_BUILDING_RECURSIVE_TERM;
          END IF;
        END LOOP;
      END IF;

      VAR_ctab_fk_cols_list := string_agg(format('%I', ael), ', ') FROM jsonb_array_elements_text(VAR_flat_graph_node->'ctab_fk_col_names') ael;
      VAR_ctab_pk_cols_list := string_agg(format('%I', ael), ', ') FROM jsonb_array_elements_text(VAR_flat_graph_node->'ctab_pk_col_names') ael;

      IF (VAR_flat_graph_node->>'depth')::INT != 0 THEN
        VAR_in := format('SELECT %s FROM %I',
          (SELECT string_agg(format('%I', ael), ', ') FROM jsonb_array_elements_text(VAR_flat_graph_node->'ptab_uk_col_names') ael),
          VAR_flat_graph->((VAR_flat_graph_node->>'i_up')::INT)->>'cte_aux_stmt_name'
        );
      END IF;

      VAR_recursive_term := coalesce(VAR_recursive_term, format('SELECT %s', array_to_string(array_fill('NULL'::TEXT, ARRAY[jsonb_array_length(VAR_flat_graph_node->'ctab_pk_col_names')]), ', ')));

      VAR_cte_aux_stmts := VAR_cte_aux_stmts || format($CTE_AUX_STMT$
        %I AS (
          DELETE FROM %s WHERE (%s) IN (
            WITH RECURSIVE
            self_ref (%s) AS (
              SELECT %s FROM %s WHERE (%s) IN (%s)
                UNION
              %s
            )
            SELECT %s FROM self_ref
          ) RETURNING *
        )
      $CTE_AUX_STMT$,
        VAR_flat_graph_node->>'cte_aux_stmt_name',
        VAR_flat_graph_node->>'ctab_name', VAR_ctab_pk_cols_list,
        VAR_ctab_pk_cols_list,
        VAR_ctab_pk_cols_list, VAR_flat_graph_node->>'ctab_name', VAR_ctab_fk_cols_list, VAR_in,
        VAR_recursive_term,
        VAR_ctab_pk_cols_list
      );
    END IF;
  END LOOP;

  FOR VAR_flat_graph_node IN SELECT jsonb_array_elements(VAR_flat_graph) LOOP
    VAR_selects_for_union := VAR_selects_for_union || format('SELECT %L AS queue_i, count(*) AS n_del FROM %I',
      VAR_flat_graph_node->>'i', VAR_flat_graph_node->>'cte_aux_stmt_name'
    );
  END LOOP;

  BEGIN
    VAR_final_query := format('WITH %s %s',
      array_to_string(VAR_cte_aux_stmts, ','), array_to_string(VAR_selects_for_union, ' UNION ')
    );

    -- RAISE INFO '%', VAR_final_query;

    OPEN VAR_del_results_cursor FOR EXECUTE VAR_final_query;

    LOOP
      FETCH VAR_del_results_cursor INTO VAR_del_result_rec;

      IF VAR_del_result_rec IS NULL THEN
        CLOSE VAR_del_results_cursor;

        EXIT;
      END IF;

      VAR_del_results := jsonb_set(VAR_del_results, ARRAY[VAR_del_result_rec.queue_i], VAR_del_result_rec.n_del::TEXT::JSONB);
    END LOOP;

    IF NOT ARG_for_realz THEN
      RAISE INFO 'DAMAGE PREVIEW (recursively_delete v%)', VAR_version;
      RAISE INFO '';

      FOR VAR_flat_graph_node IN SELECT jsonb_array_elements(VAR_flat_graph) LOOP
        RAISE INFO '%', format('%9s %1s %1s %s%s%s',
          (CASE WHEN VAR_flat_graph_node->>'delete_action' IN ('d', 'n') THEN '~' ELSE VAR_del_results->>(VAR_flat_graph_node->>'i') END),              -- N recs deleted (or to be deleted)
          VAR_flat_graph_node->>'delete_action',                                                                                                        -- FK constraint type
          (CASE WHEN VAR_flat_graph_node->>'i' IN (SELECT jsonb_array_elements(jsonb_array_elements(VAR_circ_deps))->>'i') THEN E'\u221E' ELSE '' END), -- Circular dependency indicator
          repeat('| ', coalesce((VAR_flat_graph_node->>'depth')::INTEGER, 0)),                                                                          -- Indentation
          VAR_flat_graph_node->>'ctab_name',                                                                                                            -- Relation schema/name
          (CASE WHEN (VAR_flat_graph_node->>'depth')::INT = 0 THEN '' ELSE format('.%s', VAR_flat_graph_node->>'ctab_fk_col_names') END)                -- Relation FK cols (referencing parent)
        );
      END LOOP;

      RAISE INFO '';

      -- 'ABORT': Five characters.
      RAISE EXCEPTION USING errcode = 'ABORT';
    END IF;
  EXCEPTION
    WHEN SQLSTATE 'ABORT' THEN
      NULL;
    WHEN OTHERS THEN
      RAISE;
  END;

  RETURN CASE WHEN ARG_for_realz THEN (VAR_del_results->>'0')::INT ELSE 0 END;
END;
$$;
