v0.1.4 - 2021-04-22

  * Extensionized. Thanks to David Fetter.

v0.1.3 - 2021-04-20

  * Removed `ORDER BY` from view.

v0.1.2 - 2020-12-31

  * Fixed borked CTE aux statement being generated when VAR_in resolves to NULL or an empty STRING.

v0.1.1 - 2020-12-17

  * Fixed borked CTE aux statement being generated when VAR_in resolves to empty ARRAY.
  * Added a header to the preview, to display the recursively_delete version.

v0.1.0 - 2020-06-030

  * Added explicit support for UUID keys.
  * Breaking change: Removed ability to pass an SQL string for the ARG_in parameter. An actual
    subquery serves the purpose better and without ambiguity.

v0.0.0 - < 2020-06-30
