# PG-recursively_delete

Delete records and foreign-key dependents, regardless of constraint type.

- Provides an ASCII preview of the deletion target and its graph of dependents.
- Performs deletion in a single query using recursive CTEs.
- Handles circular dependencies, intra- and inter-table.
- Handles composite keys.
- Skips 'set default' and 'set null' constraints.

### Disclaimers

- Obviously, use recursively_delete at your own risk. Test it on non-crucial data before using it in production to gain a degree of confidence with it. **Make backups.** This software purposely destroys data and is not guaranteed bug-free.

- recursively_delete was written not for transactional use-cases, but as an administration tool for special occasions. Performance wasn't the main consideration.

- recursively_delete was developed for PostgreSQL 10.10. It might work for other versions; it might not. (Feedback is welcome!) UPDATE 2020-12-17: Been using recursively_delete on PG 13.1 for some months, now, with no issues.

### Installation

* Clone.

* `cd recursively_delete/`

* `make`

* `make install` (or `sudo make install`)

* Log into your DB and `create extension recursively_delete;`

### Signature

```PLpgSQL
recursively_delete(
  ARG_table     REGCLASS                ,
  ARG_in        ANYELEMENT              ,
  ARG_for_realz BOOL       DEFAULT FALSE
) RETURNS INT
```

##### ARG_table

The table from which you'll be deleting records, with or without a qualifying schema.

##### ARG_in

A specifier for the records you'll be deleting. Loosely speaking, this would be something that'd work in an IN clause like so:

```PLpgSQL
...WHERE my_table.primary_key IN (ARG_in)...
```

Possibilities include:

- An integer: 22
- An array of integers: ARRAY[22, 33, 44]
- A string: 'foo'::TEXT
- An array of strings: ARRAY['foo', 'bar', 'baz']
- A uuid: '12345678-90ab-cdef-1234-567890abcdef'::UUID
- An array of uuids: ARRAY['12345678-90ab-cdef-1234-567890abcdef', '12345678-90ab-cdef-1234-567890abcdef', '12345678-90ab-cdef-1234-567890abcdef']
- For [composite keys](#delete-three-records-on-a-composite-primary-key), an array of arrays:
  -  ARRAY[[22, 33], [44, 55], [66, 77]]
  -  ARRAY[['22', 'foo'], ['33', 'bar'], ['44', 'baz']]
- A subquery returning one of the above:
  - (SELECT array_agg(id) FROM my_table WHERE on_the_chopping_block = true)
  - (SELECT array_agg(ARRAY[id1::TEXT, id2::TEXT, id3::TEXT]) FROM my_table WHERE on_the_chopping_block = true)

*Note that since ANYELEMENT considers untyped text to be type-ambiguous, it's necessary to explicitly type any text value given for ARG_in (e.g. 'foo'::TEXT). Otherwise brace yourself for something like 'ERROR:  could not determine polymorphic type because input has type unknown'.*

##### ARG_for_realz

When false, instructs recursively_delete **not** to delete any records, but instead produce an ASCII representation of the graph of records that **would** be deleted if ARG_for_realz were true. It's false by default.

*Be advised that previews are calculated by running **actual deletions** in a transaction that's ultimately rolled back.*

##### Return value

recursively_delete returns the number of records **explicitly** deleted (not including the records implicitly deleted pursuant to foreign key dependency). When ARG_for_realz is false, recursively_delete always returns zero.

### Examples

##### Preview the deletion of a single record:

```PLpgSQL
-- Clobber noisy context in output:

\set VERBOSITY terse

-- Then...

select recursively_delete('users', 4402);

-- ...or:

select recursively_delete('users', 4402, false);
```

```
INFO:          1     users
INFO:          4 a   | ad_submissions.["user_id"]
INFO:        182 a   | broadcasts.["created_by"]
INFO:        512 c   | | channel_selections.["broadcast_id"]
INFO:          0 c   | | post_approvals.["broadcast_id"]
INFO:          1 c   | | post_rejections.["broadcast_id"]
INFO:        326 c   | | recipient_selections.["broadcast_id"]
INFO:        309 c ∞ | | subchannel_statuses.["broadcast_id"]
INFO:        293 a ∞ | | | engagements.["subchannel_status_id"]
INFO:        245 c ∞ | | | | conversations.["engagement_id"]
INFO:          0 a ∞ | | | | | broadcasts.["conversation_id"]
INFO:          0 c   | | | | | | channel_selections.["broadcast_id"]
INFO:          0 c   | | | | | | post_approvals.["broadcast_id"]
INFO:          0 c   | | | | | | post_rejections.["broadcast_id"]
INFO:          0 c   | | | | | | recipient_selections.["broadcast_id"]
INFO:          0 a ∞ | | | | | engagements.["parent_conversation_id"]
INFO:         94 c   | | | | | interaction_updates.["conversation_id"]
INFO:          ~ n   | broadcasts.["edited_by"]
INFO:          0 a   | engagements.["user_id"]
INFO:          0 c ∞ | | conversations.["engagement_id"]
INFO:          0 a ∞ | | | broadcasts.["conversation_id"]

...etc., abridged (this was a big graph)...

 recursively_delete
--------------------
                  0
(1 row)

```

The first three columns are, at each given node:

1. The number of records to be deleted. ('~' indicates that no deletion will be attempted at the node on account of a 'set default' or 'set null' constraint.)
2. The FK constraint type: one of 'a', 'r', 'c', 'n', or 'd' ('no action', 'restrict', 'cascade', 'set null', or 'set default').
3. An indicator of participation in a circular dependency.


The graph indicates affected tables and how each relates to its parent vis-à-vis the deletion operation. For example, at the top of the graph above, one *user* record would be deleted. As a result of this, four *ad_submissions* records would be deleted because of a 'no action' constraint on the *ad_submissions.user_id* column relating to *users*; and 182 *broadcasts* records would be deleted because of a 'no action' constraint on the *broadcasts.created_by* column relating to *users*; and 512 *channel_selections* records would be deleted because of a 'cascade' constraint on the *channel_selections.broadcast_id* column relating to *broadcasts*; and so on.

##### Go ahead and delete that single record:

```PLpgSQL
select recursively_delete('users', 4402, true);
```

```
 recursively_delete
--------------------
                  1
(1 row)
```

##### Delete three records on a composite primary key:

```PLpgSQL
select recursively_delete('widgets', ARRAY[['foo', '22'], ['bar', '33'], ['baz', '44']], true);
```

```
 recursively_delete
--------------------
                  3
(1 row)
```

*Note that the order of the key columns in each subarray above isn't arbitrary; recursively_delete assumes composite key columns are given in the same order as they were given in the applicable index definition (using pg_index.indkey_subscript).*

*Note also that, in this contrived example, the types of the columns of the composite primary key are TEXT and INTEGER. Since the elements of a Postgres Array must be of a consistent type, a situation like this calls for using untyped text elements; Postgres will perform the necessary coercions.*
