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
            err = ("The truth value of a {} is ambiguous. "
                   "Use a.empty, a.bool(), a.item(), a.any() or a.all().")
            raise ValueError(err.format(self.__class__.__name__))
        result = sa.select(self._cte).scalar()
        if not pd.api.types.is_bool(result):
            err = "bool cannot act on a non-boolean single element {}"
            raise ValueError(err.format(self.__class__.__name__))
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


__all__ = ["GenericMixin"]
