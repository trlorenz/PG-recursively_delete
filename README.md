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

### Signature

```PLpgSQL
recursively_delete(
  ARG_table     REGCLASS                ,
  ARG_in        ANYELEMENT              ,
  ARG_for_realz BOOL       DEFAULT FALSE
) RETURNS INT
```
