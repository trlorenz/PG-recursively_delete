CREATE OR REPLACE FUNCTION _recursively_delete(
         ARG_table        REGCLASS,
         ARG_pk_col_names TEXT[],
        _ARG_depth        INT       DEFAULT  0,
        _ARG_fk_con       JSONB     DEFAULT  NULL,
        _ARG_queue_i_up   INT       DEFAULT  NULL,
        _ARG_path         TEXT[]    DEFAULT  ARRAY[]::TEXT[],
  INOUT _ARG_circ_deps    JSONB     DEFAULT '[]',
  INOUT _ARG_queue        JSONB     DEFAULT '[]'
) AS $$
DECLARE
  VAR_circ_dep        JSONB;
  VAR_fk_col_names    JSONB;
  VAR_fk_con_rec      RECORD;
  VAR_i               INT;
  VAR_path_pos_of_oid INT;
  VAR_pk_col_names    JSONB;
  VAR_queue_elem      JSONB;
  VAR_queue_i         INT;
BEGIN
  IF _ARG_depth = 0 THEN
    _ARG_path := _ARG_path || ARRAY['ROOT'];

    VAR_pk_col_names := to_json(ARG_pk_col_names);

    -- Not really a statement of truth, but a convenient thing to pretend. For the initial
    -- "bootstrap" CTE auxiliary statement, this HACK takes care of equating the user's ARG_in with
    -- the table's primary key.
    VAR_fk_col_names := VAR_pk_col_names;
  ELSE
    VAR_fk_col_names := _ARG_fk_con->>'ctab_fk_col_names';
    VAR_pk_col_names := _ARG_fk_con->>'ctab_pk_col_names';
  END IF;

  VAR_queue_i := jsonb_array_length(_ARG_queue);

  -- The "queue" is a collection of foreign-key-constraint-describing objects ("queue_elems")
  -- arranged in correct order for composition of the final query -- sort of a flattened graph.
  -- (Note that while queue_elem->>'i' always reports the index of a queue_elem in the queue,
  -- queue_elem->>'i_up' reports the index of the element's parent, which isn't necessarily i - 1.)
  _ARG_queue := _ARG_queue || jsonb_build_object(
    'ctab_oid'         , _ARG_fk_con->>'ctab_oid',
    'ctab_name'        ,  ARG_table,
    'ctab_pk_col_names',  VAR_pk_col_names,
    'ctab_fk_col_names',  VAR_fk_col_names,
    'delete_rule'      , _ARG_fk_con->>'delete_rule',
    --
    'cte_aux_stmt_name',  format('del_%s$%s', VAR_queue_i, _ARG_path[array_upper(_ARG_path, 1)]),
    'depth'            , _ARG_depth,
    'i'                ,  VAR_queue_i,
    'i_up'             , _ARG_queue_i_up,
    'path'             ,  to_jsonb(_ARG_path)
  );

  IF _ARG_fk_con->>'delete_rule' = 'SET NULL' THEN
    RETURN;
  END IF;

  <<FK_CON>>
  FOR VAR_fk_con_rec IN (SELECT * FROM v_fk_cons WHERE ptab_oid = ARG_table) LOOP
    VAR_path_pos_of_oid := array_position(_ARG_path, VAR_fk_con_rec.oid::TEXT);

    IF VAR_path_pos_of_oid IS NOT NULL THEN
    --^ If the id of the foreign key constraint already exists in the current path, we know the
    -- constraint is the beginning (ending?) of a circular dependency.

      VAR_circ_dep := '[]';

      -- Populate VAR_circ_dep with the interdependent queue elements comprising the circle
      -- ("deppers").
      FOR VAR_i IN VAR_path_pos_of_oid .. array_length(_ARG_path, 1) LOOP
        FOR VAR_queue_elem IN SELECT jsonb_array_elements(_ARG_queue) LOOP
          IF VAR_queue_elem->'path' = to_jsonb(_ARG_path[1:VAR_i]) THEN
            VAR_circ_dep := VAR_circ_dep || VAR_queue_elem;

            EXIT;
          END IF;
        END LOOP;
      END LOOP;

      -- Make the first depper in the circle think its parent is the last depper in the circle.
      VAR_circ_dep := jsonb_set(VAR_circ_dep, ARRAY['0', 'i_up'], VAR_circ_dep->-1->'i');

      _ARG_circ_deps := _ARG_circ_deps || jsonb_build_array(VAR_circ_dep);

      CONTINUE FK_CON;
    END IF;

    SELECT * INTO _ARG_circ_deps, _ARG_queue
    FROM _recursively_delete(
       format('%I.%I', VAR_fk_con_rec.ctab_schema_name, VAR_fk_con_rec.ctab_name),
       NULL,
       --
      _ARG_depth + 1,
       to_jsonb(VAR_fk_con_rec),
       VAR_queue_i,
      _ARG_path || VAR_fk_con_rec.oid::TEXT,
      --
      _ARG_circ_deps,
      _ARG_queue
    );
  END LOOP;
END;
$$ LANGUAGE PLPGSQL;
