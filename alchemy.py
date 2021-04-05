import operator
import functools
import pandas as pd
import sqlalchemy as sa
import db
import utils
import generic
import ops_mixin


def process_row(index, data):
    if not pd.api.types.is_list_like(index):
        index = (index,)
    if not pd.api.types.is_list_like(data):
        data = (data,)
    idx = [sa.literal(i) for i in index]
    cols = [sa.literal(c) for c in data]
    return sa.select(utils.label_idx(idx) + utils.label_cols(cols))


def dataframe_op(op, name=None, before=None, after=None):
    def op_func(self, other, axis="columns", level=None, fill_value=None):
        df = self if before is None else before(self)
        result = df._op(op, other, axis=axis, level=level,
                        fill_value=fill_value)
        return result if after is None else after(result)

    def rop_func(self, other, axis="columns", level=None, fill_value=None):
        df = self if before is None else before(self)
        result = df._op(op, other, axis=axis, level=level,
                        fill_value=fill_value, reverse=True)
        return result if after is None else after(result)

    if name is None:
        name = op.__name__
    op_func.__name__ = name
    rop_func.__name__ = "r" + name
    return op_func, rop_func


class DataFrame(generic.GenericMixin, ops_mixin.OpsMixin):
    ndim = 2

    def __init__(self, index, columns, query):
        if not isinstance(query, sa.sql.selectable.CTE):
            query = query.cte()
        self._index = index
        self._columns = columns
        self._query = query
        self.columns = columns

    def __getattr__(self, name):
        try:
            col = self._columns.get_loc(name)
            cols = utils.label_cols([self._col_at(col)])
            query = sa.select(self._idx() + cols)
            return Series(self._index, pd.Index([name]), query)
        except KeyError:
            return self.__getattribute__(name)

    @staticmethod
    def _normalize_axis(axis):
        if axis in (0, "index"):
            return 0
        if axis in (1, "columns"):
            return 1
        err = "No axis named {} for object type DataFrame"
        raise ValueError(err.format(axis))

    @property
    def shape(self):
        return len(self), len(self.columns)

    def iterrows(self):
        for row in self._fetch():
            data = pd.Series(row[len(self._index)], index=self.columns)
            if len(self._index) == 1:
                yield row[0], data
            else:
                yield row[:len(self._index)], data

    def _op(self, op, other, axis="columns", level=None,
            fill_value=None, reverse=False):
        def app_op(lhs, rhs):
            result = op(rhs, lhs) if reverse else op(lhs, rhs)
            if fill_value is None:
                return result
            return sa.func.coalesce(result, fill_value)
        axis = DataFrame._normalize_axis(axis)
        if pd.api.types.is_scalar(other):
            cols = [app_op(c, other) for c in self._cols()]
            query = sa.select(self._idx() + utils.label_cols(cols))
            return DataFrame(self._index, self.columns, query)
        if axis == 1 and isinstance(other, (Series, pd.Series)):
            joined = self.columns.join(other.index, how="outer",
                                       level=level, return_indexers=True)
            columns, self_idx, other_idx = joined
            zipped = utils.zip_indexers(len(columns), self_idx, other_idx)
            other = list(other)
            cols = [app_op(self._col_at(i), other[j]) for i, j in zipped]
            query = sa.select(self._idx() + utils.label_cols(cols))
            return DataFrame(self._index, columns, query)
        if axis == 0 and isinstance(other, (Series, pd.Series)):
            other = Series.from_pandas(other, optional=True)
            index, idx, join_cond = self._join(other, level=level)
            cols = [app_op(c, other._col_at(0)) for c in self._cols()]
            joined = self._query.join(other._query, join_cond, full=True)
            selects = sa.select(utils.label_idx(idx) + utils.label_cols(cols))
            return DataFrame(index, self.columns, selects.select_from(joined))
        if isinstance(other, (DataFrame, pd.DataFrame)):
            other = DataFrame.from_pandas(other, optional=True)
            index, idx, join_cond = self._join(other, level=level)
            joined = self.columns.join(other.columns, how="outer",
                                       level=level, return_indexers=True)
            columns, self_idx, other_idx = joined
            zipped = utils.zip_indexers(len(columns), self_idx, other_idx)
            cols = [app_op(self._col_at(i), other._col_at(j))
                    for i, j in zipped]
            joined = self._query.join(other._query, join_cond, full=True)
            selects = sa.select(utils.label_idx(idx) + utils.label_cols(cols))
            return DataFrame(index, columns, selects.select_from(joined))
        if pd.api.types.is_list_like(other):
            other = list(other)
            if axis == 1:
                if len(other) != len(self.columns):
                    err = ("Unable to coerce to Series, "
                           "length must be {}: given {}")
                    raise ValueError(err.format(len(self.columns), len(other)))
                cols = [app_op(self._col_at(i), other[i])
                        for i in range(len(self.columns))]
                query = sa.select(self._idx() + utils.label_cols(cols))
                return DataFrame(self._index, self.columns, query)
            row_count = len(self)
            if len(other) != row_count:
                err = "Unable to coerce to Series, length must be {}: given {}"
                raise ValueError(err.format(row_count, len(other)))
            raise NotImplementedError
        err = "Cannot broadcast np.ndarray with operand of type {}"
        raise TypeError(err.format(type(other)))

    add, radd = dataframe_op(operator.add)
    sub, rsub = dataframe_op(operator.sub)
    mul, rmul = dataframe_op(operator.mul)
    div, rdiv = dataframe_op(operator.truediv, name="div",
                             before=lambda df: df._cast(sa.NUMERIC))
    truediv, rtruediv = dataframe_op(operator.truediv,
                                     before=lambda df: df._cast(sa.NUMERIC))
    floordiv, rfloordiv = dataframe_op(operator.truediv, name="floordiv",
                                       after=lambda df: df._app(sa.func.floor))
    mod, rmod = dataframe_op(operator.mod)
    pow, rpow = dataframe_op(operator.pow)

    def to_pandas(self):
        index = []
        columns = []
        for row in self._fetch():
            if len(self._index) == 1:
                index.append(row[0])
            else:
                index.append(row[:len(self._index)])
            columns.append(row[len(self._index):])
        if len(self._index) == 1:
            index = pd.Index(index, name=self._index[0])
        else:
            index = pd.MultiIndex.from_tuples(index, names=self._index)
        df = pd.DataFrame.from_records(columns, columns=self._columns)
        return df.set_index(index)

    @staticmethod
    def from_pandas(df, optional=False):
        if not isinstance(df, pd.DataFrame):
            if optional:
                return df
            raise TypeError("Must be a Pandas DataFrame")
        query = sa.union_all(*[process_row(index, data)
                               for index, data in df.iterrows()])
        query.bind = db.metadata().bind
        index = pd.Index(df.index.names)
        return DataFrame(index, df.columns, query)


class Series(generic.GenericMixin):
    ndim = 1

    def __init__(self, index, columns, query):
        if not isinstance(query, sa.sql.selectable.CTE):
            query = query.cte()
        self._index = index
        self._columns = columns
        self._query = query

    def __iter__(self):
        for row in self._fetch():
            yield row[-1]

    @property
    def name(self):
        return self._columns[0]

    @property
    def shape(self):
        return len(self),

    def iteritems(self):
        for row in self._fetch():
            if len(self._index) == 1:
                yield row[0], row[1]
            else:
                yield row[:len(self._index)], row[-1]

    def to_pandas(self):
        index = []
        value = []
        for row in self._fetch():
            if len(self._index) == 1:
                index.append(row[0])
            else:
                index.append(row[:len(self._index)])
            value.append(row[-1])
        if len(self._index == 1):
            index = pd.Index(index, name=self._index[0])
        else:
            index = pd.MultiIndex.from_tuples(index, names=self._index)
        return pd.Series(value, index=index)

    @staticmethod
    def from_pandas(seq, optional=False):
        if not isinstance(seq, pd.Series):
            if optional:
                return seq
            raise TypeError("Must be a Pandas Series")
        query = sa.union_all(*[process_row(index, data)
                               for index, data in seq.iteritems()])
        query.bind = db.metadata().bind
        index = pd.Index(seq.index.names)
        columns = pd.Index((seq.name,))
        return Series(index, columns, query)


__all__ = ['DataFrame', 'Series']
