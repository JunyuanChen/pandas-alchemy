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
