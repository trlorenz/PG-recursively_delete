CREATE OR REPLACE FUNCTION recursively_delete(
  ARG_table     REGCLASS                ,
  ARG_in        ANYELEMENT              ,
  ARG_for_realz BOOL       DEFAULT FALSE
) RETURNS INT AS $$
DECLARE
  VAR_circ_dep              JSONB                         ;
  VAR_circ_depper           JSONB                         ;
  VAR_circ_depper_up        JSONB                         ;
  VAR_circ_deps             JSONB                         ;
  VAR_ctab_fk_cols_list     TEXT                          ;
  VAR_ctab_pk_cols_list     TEXT                          ;
  VAR_cte_aux_stmts         TEXT[] DEFAULT ARRAY[]::TEXT[];
  VAR_del_result_rec        RECORD                        ;
  VAR_del_results           JSONB DEFAULT '{}'            ;
  VAR_del_results_cursor    REFCURSOR                     ;
  VAR_final_query           TEXT                          ;
  VAR_flat_graph            JSONB                         ;
  VAR_flat_graph_node       JSONB                         ;
  VAR_in                    TEXT                          ;
  VAR_join_comperand_l      TEXT                          ;
  VAR_join_comperand_r      TEXT                          ;
  VAR_pk_col_names          TEXT[] DEFAULT ARRAY[]::TEXT[];
  VAR_recursive_term        TEXT                          ;
  VAR_recursive_term_from   TEXT[]                        ;
  VAR_recursive_term_select TEXT[]                        ;
  VAR_recursive_term_where  TEXT[]                        ;
  VAR_selects_for_union     TEXT[]                        ;
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
      WHEN 'character varying', 'text' THEN
        VAR_in := ARG_in;
      WHEN 'character varying[]', 'text[]' THEN
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
      WHEN 'character varying', 'text' THEN
        VAR_in := ARG_in;
      WHEN 'character varying[]', 'text[]' THEN
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
$$ LANGUAGE PLPGSQL;
