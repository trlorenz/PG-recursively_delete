CREATE OR REPLACE FUNCTION _recursively_delete(
         ARG_table           REGCLASS                         ,
         ARG_pk_col_names    TEXT[]                           ,
        _ARG_depth           INT      DEFAULT  0              ,
        _ARG_fk_con          JSONB    DEFAULT  NULL           ,
        _ARG_flat_graph_i_up INT      DEFAULT  NULL           ,
        _ARG_path            TEXT[]   DEFAULT  ARRAY[]::TEXT[],
  INOUT _ARG_circ_deps       JSONB    DEFAULT '[]'            ,
  INOUT _ARG_flat_graph      JSONB    DEFAULT '[]'
) AS $$
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
  FOR VAR_fk_con_rec IN (SELECT * FROM v_fk_cons WHERE ptab_oid = ARG_table) LOOP
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
$$ LANGUAGE PLPGSQL;
