from .db import init_db, close_db
from .alchemy import DataFrame, Series


def use_repr_workaround():
    """
    Until we actually implement __repr__() for DataFrame and Series,
    convert DataFrame and Series to their Pandas counterpart and use
    that __repr__() method.

    This function is intentionally not exported. It is subject to
    removal once proper __repr__() methods are implemented.
    """
    def to_pandas_repr(self):
        return repr(self.to_pandas())

    DataFrame.__repr__ = to_pandas_repr
    Series.__repr__ = to_pandas_repr


__all__ = ["init_db", "close_db", "DataFrame", "Series"]
