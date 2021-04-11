# pandas-alchemy

`pandas-alchemy` is a Python package for analyzing data in SQL
databases using a Pandas compatible interface.

While SQL the language *absolutely* sucks, SQL databases are
great. Databases shine at handling larger-than-memory data. There have
been countless hours of engineering spent on optimizing their
performance. Furthermore, often times, the data we need to analyze is
already in some databases.

`pandas-alchemy` implements SQL based DataFrame and Series. Unlike
`read_sql()` in Pandas, the entire table is *not* loaded into the main
memory. Instead, a SQL query is stored and is used to back a DataFrame
or a Series.

```python
import pandas as pd
from pandas_alchemy import init_db, DataFrame


# Connect to the database using an sqlalchemy database URL
init_db('postgresql://foo:bar@localhost/quux')
df = DataFrame.from_table('foobar')
df.head()  # Just treat it like a Pandas DataFrame
df + [1,2,3]  # Arithmetic works too

pd_df = pd.DataFrame({'baz': [1, 2], 'quux': ['a', 'b']})
df + pd_df  # Pandas interoperable

# Getting a Series from the DataFrame
# Data is not actually loaded into memory, either
df.baz

# Actually retrieve data and convert to a Pandas DataFrame
df.to_pandas()
```

## :warning: *Un*usability Warning :warning:
This project is in its *very* early development. Many features are still
missing. While it is the goal of this project, it is not possible, as of
now, to replace a Pandas DataFrame or Series with a `pandas-alchemy`
counterpart and to expect the program to JustWork<sup>:tm:</sup>.

### `pandas_alchemy.use_repr_workaround()`
Currently, `DataFrame.__repr__()` and `Series.__repr__()` is not
implemented yet. By calling `pandas_alchemy.use_repr_workaround()`,
`DataFrame.__repr__()` and `Series.__repr__()` will retrieve all data,
convert itself to a Pandas counterpart, and then `repr()` that Pandas
counterpart. **Note that this will usually defeat the whole point of
using `pandas-alchemy`.**

## Exception-to-exception compatibility
`pandas-alchemy` aims to be completely transparent to the program. There
should be zero difference between the behaviour of a `pandas-alchemy`
DataFrame and a Pandas DataFrame, even when an exception is
raised. Differences between behaviours, including differences in
exceptions raised (to a reasonable extent), are considered *bugs* for
the purpose of this project. Please open an issue if you have spotted
such a difference.

## Usage

1. `pandas_alchemy.init_db(<sqlalchemy database URL>)`
2. `df = pandas_alchemy.DataFrame.from_table(<table>)`
3. Treat `df` as a Pandas DataFrame and do something awesome about it.
4. Profit!!!

### init\_db(\*args, \*\*kwargs)
Create a database connection. If already connected, raise RuntimeError.

\*args and \*\*kwargs are passed directly to sqlalchemy.create\_engine().
See the documentation of sqlalchemy.create\_engien()

### close\_db()
Close the database connection. If not connected yet, raise RuntimeError.

### DataFrame(index, columns, cte)
**Probably _not_ something you are looking for.**

Create a DataFrame using index, columns and cte.
index is a Pandas Index representing the levels in the DataFrame's index.
columns is a Pandas Index representing the column names.
cte is a sqlalchemy CTE that backs the DataFrame.

### DataFrame.from\_table(table, schema=None, columns=None, index=None)
Load table from the database as a DataFrame.

If columns is not None, it is taken as an ordered list of
columns to be included in the DataFrame.

If index is a list-like object, it is taken as an ordered
list of columns whose values are the DataFrame's index.
Otherwise, if index is not None, it is taken as the name
of the column to become the DataFrame's index.

### DataFrame.from\_pandas(df, optional=False)
Convert the Pandas DataFrame df to a DataFrame.
If df is not an instance of pandas.DataFrame, return it as is
when optional is True. Otherwise, raise TypeError.

### DataFrame.to\_pandas()
Convert the DataFrame to a Pandas DataFrame.

### Series(index, columns, cte, name)
**Probably _not_ something you are looking for.**

Create a Series using index, columns and cte, with name name.
index is a Pandas Index representing the levels in the Series's index.
columns is a Pandas Index representing the column names.
cte is a sqlalchemy CTE that backs the Series.
name is the name of the Series.

### Series.from\_pandas(seq, name=None, optional=False)
Convert the Pandas Series seq to a Series.
If name is not None, it will be used as the name of
the resulting Series instead of seq.name.
If seq is not an instance of pandas.Series, return it as is
when optional is True, Otherwise, raise TypeError.

### Series.to\_pandas()
Convert the Series to a Pandas Series.

## Pandas API Coverage
See [API\_COVERAGE.md](API_COVERAGE.md).

## Known Issues & Limitations
- Cannot distinguish `0.0` and `-0.0` (IEEE float)
- Returns None for NaN in SQLite3 if every value in the column is None
- Lacks support for arithmetic between two MultiIndex DataFrame/Series
