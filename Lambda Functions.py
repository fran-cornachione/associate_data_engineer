-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### Lambda Functions

-- COMMAND ----------

-- MAGIC %md
-- MAGIC %md
-- MAGIC `explode`
-- MAGIC
-- MAGIC Returns a set of rows by un-nesting collection.
-- MAGIC
-- MAGIC **Arguments**
-- MAGIC
-- MAGIC - `collection`: An `ARRAY` or `MAP` expression.
-- MAGIC
-- MAGIC **Returns**
-- MAGIC
-- MAGIC A set of rows composed of the elements of the array or the keys and values of the map. The column produced by `explode` of an array is named `col`. The columns for a map are called key and value.
-- MAGIC
-- MAGIC If `collection` is `NULL` no rows are produced. To return a single row with NULLs for the array or map values use the `explode_outer()` function.
-- MAGIC
-- MAGIC `explode` can only be placed in the `SELECT` list as the root of an expression or following a LATERAL VIEW. When placing the function in the `SELECT` list there must be no other generator function in the same SELECT list or UNSUPPORTED_GENERATOR.MULTI_GENERATOR is raised.

-- COMMAND ----------

-- DBTITLE 1,explode - Example
SELECT explode(array(10, 20)) AS elem, 'Spark';
 -- 10 Spark
 -- 20 Spark

SELECT explode(map(1, 'a', 2, 'b')) AS (num, val), 'Spark';
 -- 1   a   Spark
 -- 2   b   Spark

SELECT explode(array(1, 2)), explode(array(3, 4));
  -- Error: UNSUPPORTED_GENERATOR.MULTI_GENERATOR

-- The difference between explode() and explode_outer() is that explode_outer() returns NULL if the array is NULL.
SELECT explode_outer(c1) AS elem, 'Spark' FROM VALUES(array(10, 20)), (null) AS T(c1);
 -- 10   Spark
 -- 20   Spark
 -- NULL Spark

SELECT explode(c1) AS elem, 'Spark' FROM VALUES(array(10, 20)), (null) AS T(c1);
 -- 10 Spark
 -- 20 Spark

-- COMMAND ----------

-- MAGIC %md
-- MAGIC `exists`
-- MAGIC
-- MAGIC Returns true if `func` is true for any element in `expr` or `query` returns at least one row.
-- MAGIC
-- MAGIC **Arguments**
-- MAGIC
-- MAGIC - `expr`: An ARRAY expression.
-- MAGIC - `func`: A lambda function.
-- MAGIC - `query`: Any Query.
-- MAGIC
-- MAGIC **Returns**
-- MAGIC
-- MAGIC A BOOLEAN.
-- MAGIC
-- MAGIC The lambda function must result in a boolean and operate on one parameter, which represents an element in the array.
-- MAGIC
-- MAGIC `exists(query)` can only be used in the `WHERE` clause and few other specific cases.

-- COMMAND ----------

-- DBTITLE 1,exists - Syntax
exists(expr, func);

exists(query);

-- COMMAND ----------

-- DBTITLE 1,exists - Example
-- exists(expr, func)
SELECT exists(array(1, 2, 3), x -> x % 2 == 0); -- True (2 is even)

SELECT exists(array(1, 2, 3), x -> x % 2 == 10); -- False (no element is divisible by 10) 

SELECT exists(array(0, NULL, 2, 3, NULL), x -> x IS NULL); -- True (NULL is found)

SELECT exists(array(1, 2, 3), x -> x IS NULL); -- False (no NULL is found)


-- exists(query)
SELECT count(*) FROM VALUES(1)
WHERE exists(SELECT * FROM VALUES(1), (2), (3) AS t(c1) WHERE c1 = 2); -- True (2 is found)

SELECT count(*) FROM VALUES(1)
WHERE exists(SELECT * FROM VALUES(1), (NULL), (3) AS t(c1) WHERE c1 = 2); -- False (no 2 is found)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC `filter`
-- MAGIC
-- MAGIC Filters the array in `expr` using the function `func`.
-- MAGIC
-- MAGIC **Arguments**
-- MAGIC
-- MAGIC - `expr`: An ARRAY expression.
-- MAGIC - `func`: A lambda function.
-- MAGIC
-- MAGIC **Returns**
-- MAGIC
-- MAGIC The result is of the same type as `expr`.
-- MAGIC
-- MAGIC The lambda function may use one or two parameters where the first parameter represents the element and the second the index into the array.

-- COMMAND ----------

-- DBTITLE 1,filter - Syntax
filter(expr, func)

-- COMMAND ----------

-- DBTITLE 1,filter - Examples
SELECT filter(array(1, 2, 3), x -> x % 2 == 1); -- [1, 3] (Keep odd numbers)

SELECT filter(array(0, 2, 3), (x, i) -> x > i); -- [2, 3] (Keep numbers greater than their index)

SELECT filter(array(0, null, 2, 3, null), x -> x IS NOT NULL); -- [0, 2, 3] (Keep non-NULLs)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC `forall`
-- MAGIC
-- MAGIC Tests whether `func` holds for **all elements in the array**.
-- MAGIC
-- MAGIC **Arguments**
-- MAGIC
-- MAGIC - `expr`: An ARRAY expression.
-- MAGIC - `func`: A lambda function returning a BOOLEAN.
-- MAGIC
-- MAGIC **Returns**
-- MAGIC
-- MAGIC A BOOLEAN.
-- MAGIC
-- MAGIC The lambda function uses one parameter passing an element of the array.

-- COMMAND ----------

-- DBTITLE 1,forall - Syntax
forall(expr, func)

-- COMMAND ----------

-- DBTITLE 1,forall - Examples
SELECT forall(array(1, 2, 3), x -> x % 2 == 0); -- False (only 2 is even)

SELECT forall(array(2, 4, 8), x -> x % 2 == 0); -- True (ALL are even)

SELECT forall(array(1, NULL, 3), x -> x % 2 == 0); -- False (no element is even)

SELECT forall(array(2, NULL, 8), x -> x % 2 == 0); -- Null (2 and 8 are even, but NULL is found)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC `map_filter()`
-- MAGIC
-- MAGIC Filters entries in the map in `expr` using the function `func`.
-- MAGIC
-- MAGIC **Arguements**
-- MAGIC
-- MAGIC - `expr`: A MAP expression.
-- MAGIC - `func`: A lambda function with two parameters returning a BOOLEAN. The first parameter takes the key the second parameter takes the value.
-- MAGIC
-- MAGIC **Returns**
-- MAGIC
-- MAGIC The result is the same type as `expr`.

-- COMMAND ----------

-- DBTITLE 1,map_filter - Syntax
map_filter(expr, func)

-- COMMAND ----------

-- DBTITLE 1,map_filter - Syntax
-- Where k is the key and v is the value
SELECT map_filter(map(1, 0, 2, 2, 3, -1), (k, v) -> k > v); -- {1 -> 0, 3 -> -1}

-- COMMAND ----------

-- MAGIC %md
-- MAGIC `map_zip_with`
-- MAGIC
-- MAGIC Merges `map1` and `map2` into a single map.
-- MAGIC
-- MAGIC **Arguments**
-- MAGIC
-- MAGIC - `map1`: A MAP expression.
-- MAGIC - `map2`: A MAP expression of the same key type as `map1`
-- MAGIC - `func`: A lambda function taking three parameters. The first parameter is the key, followed by the values from each map.
-- MAGIC
-- MAGIC **Returns**
-- MAGIC
-- MAGIC A MAP where the key matches the key type of the input maps and the value is typed by the return type of the lambda function.
-- MAGIC
-- MAGIC If a key is not matched by one side the respective value provided to the lambda function is `NULL`.

-- COMMAND ----------

-- DBTITLE 1,map_zip_with - Syntax
map_zip_with(map1, map2, func)

-- COMMAND ----------

-- DBTITLE 1,map_zip_with - Examples
SELECT map_zip_with(map(1, 'a', 2, 'b'), map(1, 'x', 2, 'y'), (k, v1, v2) -> concat(v1, v2))-- {1 -> ax, 2 -> by}

-- COMMAND ----------

-- MAGIC %md
-- MAGIC `transform`
-- MAGIC
-- MAGIC Transforms elements in an array in `expr` using the function `func`.
-- MAGIC
-- MAGIC **Arguments**
-- MAGIC
-- MAGIC - `expr`: An ARRAY expression.
-- MAGIC - `func`: A lambda function.
-- MAGIC
-- MAGIC **Returns**
-- MAGIC
-- MAGIC An ARRAY of the type of the lambda function's result.
-- MAGIC
-- MAGIC The lambda function must have 1 or 2 parameters. The first parameter represents the element, the optional second parameter represents the index of the element.
-- MAGIC
-- MAGIC The lambda function produces a new value for each element in the array.

-- COMMAND ----------

-- DBTITLE 1,transform - Syntax
transform(expr, func)

-- COMMAND ----------

-- DBTITLE 1,transform - Examples
SELECT transform(array(1, 2, 3), x -> x + 1); -- [2, 3, 4] (Add 1 to each element)

SELECT transform(array(1, 2, 3), (x, i) -> x + i); -- [1, 3, 5] (Add index to each element)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC `transform_keys`
-- MAGIC
-- MAGIC Transforms keys in a map in `expr` using the function `func`.
-- MAGIC
-- MAGIC **Arguments**
-- MAGIC
-- MAGIC - `expr`: A MAP expression.
-- MAGIC - `func`: A lambda function.
-- MAGIC
-- MAGIC **Returns**
-- MAGIC
-- MAGIC A MAP where the keys have the type of the result of the lambda functions and the values have the type of the `expr` MAP values.
-- MAGIC
-- MAGIC The lambda function must have 2 parameters. The first parameter represents the key. The second parameter represents the value.
-- MAGIC
-- MAGIC The lambda function produces a new key for each entry in the map.

-- COMMAND ----------

-- DBTITLE 1,transform_keys - Syntax
SELECT transform_keys(map_from_arrays(array(1, 2, 3), array(1, 2, 3)), (k, v) -> k + 1); -- {2 -> 1, 3 -> 2, 4 -> 3}
SELECT transform_keys(map_from_arrays(array(1, 2, 3), array(1, 2, 3)), (k, v) -> k + v); -- {2 -> 1, 4 -> 2, 6 -> 3}

-- COMMAND ----------

-- MAGIC %md
-- MAGIC `aggregate`
-- MAGIC
-- MAGIC Aggregates elements in an array using a custom aggregator. This function is a synonym for `reduce` function.
-- MAGIC
-- MAGIC **Arguments**
-- MAGIC
-- MAGIC - `expr`: An ARRAY expression.
-- MAGIC - `start`: An initial value of any type.
-- MAGIC - `merge`: A lambda function used to aggregate the current element.
-- MAGIC - `finish`: An optional lambda function used to finalize the aggregation.
-- MAGIC
-- MAGIC **Returns**
-- MAGIC
-- MAGIC The result type matches the result type of the `finish` lambda function if exists or `start`.
-- MAGIC
-- MAGIC Applies an expression to an initial state and all elements in the array, and reduces this to a single state. The final state is converted into the final result by applying a `finish` function.
-- MAGIC
-- MAGIC The `merge` function takes two parameters. The first being the accumulator, the second the element to be aggregated. The accumulator and the result must be of the type of `start`. The optional `finish` function takes one parameter and returns the final result.

-- COMMAND ----------

-- DBTITLE 1,aggregate - Examples
SELECT aggregate(array(1, 2, 3), 0, (acc, x) -> acc + x); -- 6 (Adds x to each element)

SELECT aggregate(array(1, 2, 3), 0, (acc, x) -> acc + x, acc -> acc * 10); -- 60 (Adds x to each element and multiply by 10)

SELECT aggregate(array(1, 2, 3, 4),
                   named_struct('sum', 0, 'cnt', 0),
                   (acc, x) -> named_struct('sum', acc.sum + x, 'cnt', acc.cnt + 1),
                   acc -> acc.sum / acc.cnt) AS avg -- 2.5 ()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC `array_sort`
-- MAGIC
-- MAGIC Returns `array` sorted according to `func`.
-- MAGIC
-- MAGIC **Arguments**
-- MAGIC
-- MAGIC - `array`: An expression that evaluates to an array.
-- MAGIC - `func`: A lambda function defining the sort order.
-- MAGIC
-- MAGIC **Returns**
-- MAGIC
-- MAGIC - The result type matches the type of `array`.
-- MAGIC
-- MAGIC - If `func` is omitted, the array is sorted in ascending order.
-- MAGIC
-- MAGIC - If `func` is provided it takes two arguments representing two elements of the array.
-- MAGIC
-- MAGIC - The function must return -1, 0, or 1 depending on whether the first element is less than, equal to, or greater than the second element.
-- MAGIC
-- MAGIC - If the `func` returns other values (including `NULL`), `array_sort` fails and raises an error.
-- MAGIC
-- MAGIC - `NULL` elements are placed at the end of the returned array.

-- COMMAND ----------

-- DBTITLE 1,array_sort - Syntax
array_sort(array, func)

-- COMMAND ----------

-- DBTITLE 1,array_sort - Examples
SELECT array_sort(array(5, 6, 1),
                   (left, right) -> CASE WHEN left < right THEN -1
                                         WHEN left > right THEN 1 ELSE 0 END); -- [1, 5, 6]

SELECT array_sort(array('bc', 'ab', 'dc'),
                    (left, right) -> CASE WHEN left IS NULL and right IS NULL THEN 0
                                          WHEN left IS NULL THEN -1
                                          WHEN right IS NULL THEN 1
                                          WHEN left < right THEN 1
                                          WHEN left > right THEN -1 ELSE 0 END); -- [dc, bc, ab]

SELECT array_sort(array('b', 'd', null, 'c', 'a')); -- [a, b, c, d, NULL] (Ascending order)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC `array_append`
-- MAGIC
-- MAGIC Returns `array` appended by `elem`.
-- MAGIC
-- MAGIC **Arguments**
-- MAGIC
-- MAGIC - `array`: An ARRAY.
-- MAGIC - `elem`: An expression of the same type as the elements of `array`.
-- MAGIC
-- MAGIC **Returns**
-- MAGIC
-- MAGIC An ARRAY of the same type as `array`.

-- COMMAND ----------

-- DBTITLE 1,array_append - Syntax
array_append(array, elem)

-- COMMAND ----------

-- DBTITLE 1,array_append - Examples
SELECT array_append(array(1, 2, 3), 0); -- [1, 2, 3, 0] (Appends 0)

SELECT array_append(array(1, 2, 3), NULL); -- [1, 2, 3, NULL] (Appends NULL)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC `array_distinct`
-- MAGIC
-- MAGIC Removes duplicate values from `array`.
-- MAGIC
-- MAGIC **Arguments**
-- MAGIC
-- MAGIC - `array`: An ARRAY expression.
-- MAGIC
-- MAGIC **Returns**
-- MAGIC
-- MAGIC The function returns an array of the same type as the input argument where all duplicate values have been removed.

-- COMMAND ----------

-- DBTITLE 1,array_distinct - Syntax
array_distinct(array)

-- COMMAND ----------

-- DBTITLE 1,array_distinct - Examples
SELECT array_distinct(array(1, 2, 3, NULL, 3)); -- [1, 2, 3, NULL] (3 was duplicated)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC `array_except`
-- MAGIC
-- MAGIC Returns an array of the elements in `array1` but not in `array2`.
-- MAGIC
-- MAGIC **Arguments**
-- MAGIC
-- MAGIC - `array1`: An ARRAY of any type with comparable elements.
-- MAGIC - `array2`: An ARRAY of elements sharing a least common type with the elements of `array1`.
-- MAGIC
-- MAGIC **Returns**
-- MAGIC
-- MAGIC An ARRAY of matching type to `array1` with no duplicates.

-- COMMAND ----------

-- DBTITLE 1,array_except - Syntax
array_except(array1, array2)

-- COMMAND ----------

-- DBTITLE 1,array_except - Examples
SELECT array_except(array(1, 2, 2, 3), array(1, 1, 3, 5)); -- [2] (2 is the only element in array1 but not in array2)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC `array_compact`
-- MAGIC
-- MAGIC Removes `NULL` elements from array.
-- MAGIC
-- MAGIC **Arguments**
-- MAGIC
-- MAGIC - `array`: An ARRAY expression.
-- MAGIC
-- MAGIC **Returns**
-- MAGIC
-- MAGIC The function returns an array of the same type as the input argument where all `NULL` values have been removed.

-- COMMAND ----------

-- DBTITLE 1,array_compact - Syntax
array_compact(array)

-- COMMAND ----------

-- DBTITLE 1,array_compact - Examples
SELECT array_compact(array(1, 2, NULL, 3, NULL, 3)); -- [1, 2, 3, 3]

SELECT array_compact(array(NULL, NULL)); -- []

-- COMMAND ----------

-- MAGIC %md
-- MAGIC `array_union`
-- MAGIC
-- MAGIC Returns an `array` of the elements in the union of `array1` and `array2` without duplicates.
-- MAGIC
-- MAGIC **Arguments**
-- MAGIC
-- MAGIC - `array1`: An ARRAY.
-- MAGIC - `array2`: An ARRAY of the same type as `array1`.
-- MAGIC
-- MAGIC **Returns**
-- MAGIC
-- MAGIC An ARRAY of the same type as `array`.

-- COMMAND ----------

-- DBTITLE 1,array_union - Syntax
array_union(array1, array2)

-- COMMAND ----------

-- DBTITLE 1,array_union - Examples
SELECT array_union(array(1, 2, 2, 3), array(1, 3, 5)); -- [1, 2, 3, 5]

-- COMMAND ----------

-- MAGIC %md
-- MAGIC `array_remove`
-- MAGIC
-- MAGIC Removes all occurrences of element from `array`.
-- MAGIC
-- MAGIC **Arguments**
-- MAGIC
-- MAGIC - `array`: An ARRAY.
-- MAGIC - `element`: An expression of a type sharing a least common type with the elements of `array`.
-- MAGIC
-- MAGIC **Returns**
-- MAGIC
-- MAGIC The result type matched the type of the array.
-- MAGIC
-- MAGIC - If the element to be removed is `NULL`, the result is `NULL`.

-- COMMAND ----------

-- DBTITLE 1,array_remove - Syntax
array_remove(array, element)

-- COMMAND ----------

-- DBTITLE 1,array_remove - Examples
SELECT array_remove(array(1, 2, 3, NULL, 3, 2), 3); -- [1, 2, NULL, 2]

SELECT array_remove(array(1, 2, 3, NULL, 3, 2), NULL); -- NULL

-- COMMAND ----------

-- MAGIC %md
-- MAGIC `array_intersect`
-- MAGIC
-- MAGIC Returns an `array` of the elements in the intersection of `array1` and `array2`.
-- MAGIC
-- MAGIC **Arguments**
-- MAGIC
-- MAGIC - `array1`: An ARRAY of any type with comparable elements.
-- MAGIC - `array2`: n ARRAY of elements sharing a least common type with the elements of `array1`.
-- MAGIC
-- MAGIC **Returns**
-- MAGIC
-- MAGIC An ARRAY of matching type to `array1` with no duplicates and elements contained in both `array1` and `array2`.

-- COMMAND ----------

-- DBTITLE 1,array_intersect - Syntax
array_intersect(array1, array2)

-- COMMAND ----------

-- DBTITLE 1,array_intersect - Examples
SELECT array_intersect(array(1, 2, 3), array(1, 3, 3, 5)); -- [1, 3] (1 and 3 are present in both arrays)