import operator
import pandas as pd
import sqlalchemy as sa
from . import db
from . import utils
from . import dialect
from . import base
from . import generic
from . import ops_mixin


def row_to_query(index, data):
    if not pd.api.types.is_list_like(index):
        index = [index]
    else:
        index = list(index)
    if not pd.api.types.is_list_like(data):
        data = [data]
    else:
        data = list(data)
    return sa.select([sa.literal(v) for v in index + data])


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


class DataFrame(base.BaseFrame, generic.GenericMixin, ops_mixin.OpsMixin):
    ndim = 2
    _AXIS_MAPPER = utils.merge(base.BaseFrame._AXIS_MAPPER,
                               {1: 1, "columns": 1})

    def __getattr__(self, name):
        try:
            col = self.__dict__["_columns"].get_loc(name)
            query = sa.select(self._idx() + [self._col_at(col)])
            return Series(self._index, pd.Index([name]), query.cte(), name)
        except KeyError:
            return self.__getattribute__(name)

    @property
    def columns(self):
        return self._columns

    def iterrows(self):
        for row in self._fetch():
            data = pd.Series(row[len(self._index):], index=self._columns)
            if self._is_mindex:
                yield row[:len(self._index)], data
            else:
                yield row[0], data

    def _get_value(self, index, col, takeable=False):
        if takeable:
            err = "index {} is out of bounds for axis 0 with size {}"
            col = utils.wrap(col, len(self._columns))
            if col < 0 or col >= len(self._columns):
                raise IndexError(err.format(col, len(self._columns)))
            row_count = len(self)
            index = utils.wrap(index, row_count)
            if index < 0 or index >= row_count:
                raise IndexError(err.format(index, row_count))
            col = sa.select([self._col_at(col)])
            return col.limit(1).offset(index).scalar()
        raise NotImplementedError

    @utils.copied
    def _op(self, op, other, axis="columns", level=None,
            fill_value=None, reverse=False):
        axis = 1 if axis is None else self._get_axis(axis)

        def app_op(lhs, rhs):
            result = op(rhs, lhs) if reverse else op(lhs, rhs)
            if fill_value is None:
                return result
            return sa.func.coalesce(result, fill_value)

        if pd.api.types.is_scalar(other):
            cols = [app_op(c, other) for c in self._cols()]
            self._cte = sa.select(self._idx() + cols).cte()
            return
        if isinstance(other, (Series, pd.Series)):
            other = Series.from_pandas(other, optional=True)
            if axis == 1:
                columns, idxers = self._join_cols(other.index)
                other = list(other)
                other.append(sa.sql.expression.Null())  # other[-1] => NULL
                cols = [app_op(self._col_at(i), other[j]) for i, j in idxers]
                self._cte = sa.select(self._idx() + cols).cte()
                self._columns = columns
                return
            index, idx, join_cond = self._join_idx(other, level=level)
            cols = [app_op(c, other._col_at(0)) for c in self._cols()]
            self._cte = dialect.CURRENT["full_outer_join"](
                self._cte, other._cte, join_cond, idx + cols
            ).cte()
            self._index = index
            return
        if isinstance(other, (DataFrame, pd.DataFrame)):
            raise NotImplementedError
        if pd.api.types.is_list_like(other):
            other = list(other)
            err = "Unable to coerce to Series, length must be {}: given {}"
            if axis == 1:
                num_cols = len(self._columns)
                if len(other) != num_cols:
                    raise ValueError(err.format(num_cols, len(other)))
                cols = [app_op(self._col_at(i), other[i])
                        for i in range(num_cols)]
                self._cte = sa.select(self._idx() + cols).cte()
                return
            num_rows = len(self)
            if len(other) != num_rows:
                raise ValueError(err.format(num_rows, len(other)))
            other = Series.from_list(other)
            other_rowid = other._idx_at(0)
            this, other, joined = self._paste_join(other, other_rowid)
            cols = [app_op(c, other._col_at(0)) for c in this._cols()]
            query = sa.select(this._idx() + cols).select_from(joined)
            self._cte = query.cte()
            return
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
            columns.append(row[len(self._index):])
            if self._is_mindex:
                index.append(row[:len(self._index)])
            else:
                index.append(row[0])
        if self._is_mindex:
            index = pd.MultiIndex.from_tuples(index, names=self._index)
        else:
            index = pd.Index(index, name=self._index[0])
        df = pd.DataFrame.from_records(columns, columns=self._columns)
        return df.set_index(index)

    @staticmethod
    def from_pandas(df, optional=False):
        if not isinstance(df, pd.DataFrame):
            if optional:
                return df
            raise TypeError("Must be a Pandas DataFrame")
        query = sa.union_all(*[row_to_query(index, data)
                               for index, data in df.iterrows()])
        query.bind = db.metadata().bind
        index = pd.Index(df.index.names)
        return DataFrame(index, df.columns, query.cte())

    @staticmethod
    def from_table(table, schema=None, columns=None, index=None):
        """
        Load table from the database as a DataFrame.

        If columns is not None, it is taken as an ordered list of
        columns to be included in the DataFrame.

        If index is a list-like object, it is taken as an ordered
        list of columns whose values are the DataFrame's index.
        Otherwise, if index is not None, it is taken as the name
        of the column to become the DataFrame's index.
        """
        tbl = sa.Table(table, db.metadata(), schema=schema,
                       extend_existing=True, autoload=True)
        cols = [c.name for c in tbl.columns]
        if index is None:
            idx = [sa.func.row_number().over() - 1]
            index = pd.Index((None,))
        else:
            if not pd.api.types.is_list_like(index):
                index = (index,)
            index = pd.Index(index)
            for i in index:
                cols.pop(cols.index(i))
            idx = [tbl.columns[i].label(None) for i in index]
        if columns is None:
            columns = pd.Index(cols)
        else:
            columns = pd.Index(columns)
            for c in columns:
                cols.index(c)
        cols = [tbl.columns[i].label(None) for i in columns]
        query = sa.select(idx + cols)
        return DataFrame(index, columns, query.cte())


class Series(base.BaseFrame, generic.GenericMixin):
    ndim = 1

    def __init__(self, index, columns, cte, name):
        super().__init__(index, columns, cte)
        self.name = name

    def __iter__(self):
        for row in self._fetch():
            yield row[-1]

    def _get_value(self, label, takeable=False):
        if takeable:
            row_count = len(self)
            label = utils.wrap(label, row_count)
            if label < 0 or label > row_count:
                err = "index {} is out of bounds for axis 0 with size {}"
                raise IndexError(err.format(label, row_count))
            col = sa.select([self._col_at(0)])
            return col.limit(1).offset(label).scalar()
        raise NotImplementedError

    def to_pandas(self):
        index = []
        value = []
        for row in self._fetch():
            value.append(row[-1])
            if self._is_mindex:
                index.append(row[:-1])
            else:
                index.append(row[0])
        if self._is_mindex:
            index = pd.MultiIndex.from_tuples(index, names=self._index)
        else:
            index = pd.Index(index, name=self._index[0])
        return pd.Series(value, index=index, name=self.name)

    @staticmethod
    def from_pandas(seq, name=None, optional=False):
        if not isinstance(seq, pd.Series):
            if optional:
                return seq
            raise TypeError("Must be a Pandas Series")
        if name is None:
            name = seq.name
        query = sa.union_all(*[row_to_query(index, data)
                               for index, data in seq.iteritems()])
        query.bind = db.metadata().bind
        index = pd.Index(seq.index.names)
        columns = pd.Index((name,))
        return Series(index, columns, query.cte(), name)

    @staticmethod
    def from_list(values, name=None):
        query = sa.union_all(*[sa.select([sa.literal(i), sa.literal(v)])
                               for i, v in enumerate(values)])
        query.bind = db.metadata().bind
        index = pd.Index([None])
        columns = pd.Index([None])
        return Series(index, columns, query.cte(), name)


__all__ = ["DataFrame", "Series"]
