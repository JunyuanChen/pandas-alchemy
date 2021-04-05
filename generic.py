import operator
import functools
import sqlalchemy as sa
import pandas as pd
from . import db
from . import utils


class GenericMixin:
    def __len__(self):
        return sa.select([sa.func.count()]).select_from(self._query).scalar()

    def _idx_at(self, i):
        if i == -1:
            raise KeyError("No index at {}".format(i))
        return self._query.columns[utils.idx_at(i)]

    def _col_at(self, i):
        if i == -1:
            return sa.sql.expression.Null()
        return self._query.columns[utils.col_at(i)]

    def _idx(self):
        return [self._idx_at(i) for i in range(len(self._index))]

    def _cols(self):
        return [self._col_at(i) for i in range(len(self._columns))]

    @property
    def empty(self):
        return len(self) == 0

    @property
    def size(self):
        return functools.reduce(operator.mul, self.shape)

    @property
    def index(self):
        data = sa.select(self._idx()).execute()
        if len(self._index) == 1:
            return pd.Index(data.scalars(), name=self._index[0])
        return pd.MultiIndex(data, names=self._index)

    def bool(self):
        if self.size != 1:
            err = ("The truth value of a {} is ambiguous. "
                   "Use a.empty, a.bool(), a.item(), a.any() or a.all().")
            raise ValueError(err.format(self.__class__.__name__))
        result = self._query.scalar()
        if not pd.api.types.is_bool(result):
            err = "bool cannot act on a non-boolean single element {}"
            raise ValueError(err.format(self.__class__.__name__))
        return result

    def head(self, n=5):
        query = sa.select(self._query).limit(n)
        return self.__class__(self._index, self._columns, query)

    def tail(self, n=5):
        offset = max(0, len(self) - n)
        query = sa.select(self._query).limit(n).offset(offset)
        return self.__class__(self._index, self._columns, query)

    def _cast(self, new_type):
        cols = [sa.cast(c, new_type) for c in self._cols()]
        query = sa.select(self._idx() + utils.label_cols(cols))
        return self.__class__(self._index, self._columns, query)

    def _app(self, func):
        cols = [func(c) for c in self._cols()]
        query = sa.select(self._idx() + utils.label_cols(cols))
        return self.__class__(self._index, self._columns, query)

    def _join(self, other, level=None):
        if len(self._index) == 1 and len(other._index == 1):
            index = pd.Index(list(self._index))
            idx = [sa.func.coalesce(self._idx_at(0), other._idx_at(0))]
            join_cond = self._idx_at(0) == other._idx_at(0)
            return index, idx, join_cond
        if level is None:
            index, l_idx, r_idx = self._index.join(other._index, how="outer",
                                                   return_indexers=True)
            zipped = utils.zip_indexers(len(index), l_idx, r_idx)
            pairs = [(self._idx_at(i), other._idx_at(j)) for i, j in zipped]
            idx = [sa.func.coalesce(lhs, rhs) for lhs, rhs in pairs]
            join_cond = [lhs == rhs for lhs, rhs in pairs]
            join_cond = functools.reduce(operator.and_(join_cond))
            return index, idx, join_cond
        return self._join_level(other, level)

    def _join_level(self, other, level):
        if len(self._index) == 1:
            join_main = other
            join_target = self._idx_at(0)
        elif len(other._index) == 1:
            join_main = self
            join_target = other._idx_at(0)
        else:
            raise TypeError("Join on level between two "
                            "MultiIndex objects is ambiguous")
        index = join_main._index
        try:
            join_idx = index.get_loc(level)
        except KeyError:
            try:
                level = index[level]
                join_idx = index.get_loc(level)
            except IndexError:
                if isinstance(level, int):
                    if level < 0:
                        err = ("Too many levels: Index has only {} levels, "
                               "{} is not a valid level number")
                    else:
                        err = ("Too many levels: Index has "
                               "only {} levels, not {}")
                    raise IndexError(err.format(len(index), level))
                else:
                    raise KeyError("Level {} not found".format(level))
        join_col = join_main._idx_at(join_idx)
        join_cond = join_col == join_target
        idx = []
        for i in range(len(index)):
            col = join_main._idx_at(i)
            if i == join_idx:
                col = sa.func.coalesce(col, join_target)
            idx.append(col)
        return index, idx, join_cond

    def _fetch(self):
        return db.metadata().bind.execute(self._query)
