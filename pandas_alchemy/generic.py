import pandas as pd
import sqlalchemy as sa
from . import utils
from . import indexer


class GenericMixin:
    def __len__(self):
        count = sa.select([sa.func.count()])
        return count.select_from(self._cte).scalar()

    @property
    def empty(self):
        return len(self) == 0

    @property
    def shape(self):
        return (len(self), len(self._columns))[:self.ndim]

    @property
    def size(self):
        return len(self) * len(self._columns)

    @property
    def index(self):
        data = sa.select(self._idx()).execute()
        if self._is_mindex:
            return pd.MultiIndex(data, names=self._index)
        return pd.Index(data.scalars(), name=self._index[0])

    @property
    def iat(self):
        return indexer._iAtIndexer(self)

    def bool(self):
        if self.size != 1:
            raise ValueError(f"The truth value of a {self.__class.__name__} "
                             f"is ambiguous. Use a.empty, a.bool(), "
                             f"a.item(), a.any() or a.all().")
        result = sa.select(self._cte).scalar()
        if not pd.api.types.is_bool(result):
            raise ValueError(f"bool cannot act on a non-boolean "
                             f"single element {self.__class__.__name__}")
        return result

    @utils.copied
    def head(self, n=5):
        self._cte = sa.select(self._cte).limit(n).cte()

    @utils.copied
    def tail(self, n=5):
        offset = max(0, len(self) - n)
        query = sa.select(self._cte).limit(n)
        if offset:
            self._cte = query.offset(offset).cte()
        else:
            self._cte = query.cte()

    @utils.copied
    def _cast(self, new_type):
        cols = [sa.cast(c, new_type) for c in self._cols()]
        self._cte = sa.select(self._idx() + cols).cte()

    @utils.copied
    def _app(self, func):
        cols = [func(c) for c in self._cols()]
        self._cte = sa.select(self._idx() + cols).cte()

    @utils.copied
    def isna(self):
        self._app(lambda c: c.is_(None), inplace=True)

    @utils.copied
    def notna(self):
        self._app(lambda c: c.is_not(None), inplace=True)

    @utils.copied
    def abs(self):
        self._app(sa.func.abs, inplace=True)

    @utils.copied
    def round(self, decimals=0, *args, **kwargs):
        self._app(lambda c: sa.func.round(c, decimals), inplace=True)

    def pipe(self, func, *args, **kwargs):
        if isinstance(func, tuple):
            func, data_keyword = func
            kwargs[data_keyword] = self
            return func(*args, **kwargs)
        return func(self, *args, **kwargs)

    isnull = isna
    notnull = notna


__all__ = ["GenericMixin"]
