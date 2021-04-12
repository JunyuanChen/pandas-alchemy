import operator
import collections
import pandas as pd
import sqlalchemy as sa
from . import db
from . import utils
from . import coercion
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
        result = df._op(op,
                        other,
                        axis=axis,
                        level=level,
                        fill_value=fill_value)
        return result if after is None else after(result)

    def rop_func(self, other, axis="columns", level=None, fill_value=None):
        df = self if before is None else before(self)
        result = df._op(op,
                        other,
                        axis=axis,
                        level=level,
                        fill_value=fill_value,
                        reverse=True)
        return result if after is None else after(result)

    if name is None:
        name = op.__name__
    op_func.__name__ = name
    rop_func.__name__ = "r" + name
    return op_func, rop_func


def dataframe_cmp(op, name=None, before=None, after=None):
    def cmp_func(self, other, axis="columns", level=None):
        df = self if before is None else before(self)
        result = df._op(op, other, axis=axis, level=level)
        return result if after is None else after(result)

    cmp_func.__name__ = op.__name__ if name is None else name
    return cmp_func


def series_op(op, name=None, before=None, after=None):
    def op_func(self, other, level=None, fill_value=None, axis=0):
        seq = self if before is None else before(self)
        result = seq._op(op,
                         other,
                         level=level,
                         fill_value=fill_value,
                         axis=axis)
        return result if after is None else after(result)

    def rop_func(self, other, level=None, fill_value=None, axis=0):
        seq = self if before is None else before(self)
        result = seq._op(op,
                         other,
                         level=level,
                         fill_value=fill_value,
                         axis=axis,
                         reverse=True)
        return result if after is None else after(result)

    if name is None:
        name = op.__name__
    op_func.__name__ = name
    rop_func.__name__ = "r" + name
    return op_func, rop_func


def series_cmp(op, name=None, before=None, after=None):
    def cmp_func(self, other, level=None, axis=0):
        seq = self if before is None else before(self)
        result = seq._op(op, other, level=level, axis=axis)
        return result if after is None else after(result)

    cmp_func.__name__ = op.__name__ if name is None else name
    return cmp_func


class DataFrame(base.BaseFrame, generic.GenericMixin, ops_mixin.OpsMixin):
    ndim = 2
    _AXIS_MAPPER = utils.merge(base.BaseFrame._AXIS_MAPPER, {
        1: 1,
        "columns": 1
    })

    def __getattr__(self, name):
        try:
            col = self.__dict__["_columns"].get_loc(name)
            return self._seq_at(col)
        except KeyError:
            return self.__getattribute__(name)

    def _seq_at(self, i, name=None):
        """ Return the Series corresponding to column i. """
        if name is None:
            name = self._columns[i]
        query = sa.select(self._idx() + [self._col_at(i)])
        return Series(self._index, pd.Index([name]), query.cte(), name)

    @property
    def columns(self):
        return self._columns

    def iterrows(self):
        for row in self._fetch():
            idx = row[:len(self._index)] if self._is_mindex else row[0]
            data = pd.Series(row[len(self._index):], index=self._columns)
            yield idx, data

    def iteritems(self):
        for i, col in enumerate(self._columns):
            yield col, self._seq_at(i, name=col)

    def itertuples(self, index=True, name='Pandas'):
        fields = list(self._columns)
        if index:
            fields.insert(0, "Index")
        named_tuple = collections.namedtuple(name, fields, rename=True)
        for row in self._fetch():
            if index:
                idx = row[:len(self._index)] if self._is_mindex else row[0]
                yield named_tuple(idx, *row[len(self._index):])
            else:
                yield named_tuple(*row[len(self._index):])

    def _get_value(self, index, col, takeable=False):
        if takeable:
            col = utils.wrap(col, len(self._columns))
            if col < 0 or col >= len(self._columns):
                # TODO Monitor changes in pandas and adjust "axis 0"
                #
                # While technically it should be axis 1, pandas 1.2.3
                # says axis 0 in the corresponding exception.
                raise IndexError(f"index {col} is out of bounds for "
                                 f"axis 0 with size {len(self._columns)}")
            row_count = len(self)
            index = utils.wrap(index, row_count)
            if index < 0 or index >= row_count:
                raise IndexError(f"index {index} is out of bounds for "
                                 f"axis 0 with size {row_count}")
            col = sa.select([self._col_at(col)])
            return col.limit(1).offset(index).scalar()
        raise NotImplementedError

    @utils.copied
    def _op(self,
            op,
            other,
            axis="columns",
            level=None,
            fill_value=None,
            reverse=False):
        axis = 1 if axis is None else self._get_axis(axis)

        def app_op(lhs, rhs):
            if reverse:
                lhs, rhs = rhs, lhs
            result = coercion.app_op_coerced(op, lhs, rhs)
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
            cols = [app_op(c, other._the_col) for c in self._cols()]
            self._join_idx(other, cols, level=level, inplace=True)
            return
        if isinstance(other, (DataFrame, pd.DataFrame)):
            other = DataFrame.from_pandas(other)
            if self._cte == other._cte:
                # Ensure different names for self join
                self._cte = self._cte.alias()
            columns, idxers = self._join_cols(other._columns)
            cols = [
                app_op(self._col_at(i), other._col_at(j)) for i, j in idxers
            ]
            self._join_idx(other, cols, level=level, inplace=True)
            self._columns = columns
            return
        if pd.api.types.is_list_like(other):
            other = list(other)
            if axis == 1:
                num_cols = len(self._columns)
                if len(other) != num_cols:
                    raise ValueError(f"Unable to coerce to Series, length "
                                     f"must be {num_cols}: given {len(other)}")
                cols = [
                    app_op(self._col_at(i), other[i]) for i in range(num_cols)
                ]
                self._cte = sa.select(self._idx() + cols).cte()
                return
            num_rows = len(self)
            if len(other) != num_rows:
                raise ValueError(f"Unable to coerce to Series, length "
                                 f"must be {num_rows}: given {len(other)}")
            other = Series.from_list(other)
            other_rowid = other._idx_at(0)
            this, other, joined = self._paste_join(other, other_rowid)
            cols = [app_op(c, other._the_col) for c in this._cols()]
            query = sa.select(this._idx() + cols).select_from(joined)
            self._cte = query.cte()
            return
        raise TypeError(f"Cannot broadcast np.ndarray with "
                        f"operand of type {type(other)}")

    add, radd = dataframe_op(operator.add)
    sub, rsub = dataframe_op(operator.sub)
    mul, rmul = dataframe_op(operator.mul)
    div, rdiv = dataframe_op(operator.truediv, name="div")
    truediv, rtruediv = dataframe_op(operator.truediv)
    floordiv, rfloordiv = dataframe_op(operator.floordiv)
    mod, rmod = dataframe_op(operator.mod)
    pow, rpow = dataframe_op(operator.pow)

    eq = dataframe_cmp(operator.eq)
    ne = dataframe_cmp(operator.ne)
    le = dataframe_cmp(operator.le)
    lt = dataframe_cmp(operator.lt)
    ge = dataframe_cmp(operator.ge)
    gt = dataframe_cmp(operator.gt)

    @utils.copied
    def applymap(self, func, na_action=None):
        def app_func(col):
            if na_action is None:
                return func(col)
            return sa.case((col.is_(None), col), else_=func(col))

        self._app(app_func, inplace=True)

    def to_pandas(self):
        index = []
        columns = []
        for row in self._fetch():
            columns.append(row[len(self._index):])
            idx = row[:len(self._index)] if self._is_mindex else row[0]
            index.append(idx)
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
            raise TypeError("Must be a pandas DataFrame")
        query = sa.union_all(
            *[row_to_query(index, data) for index, data in df.iterrows()])
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
        tbl = sa.Table(table,
                       db.metadata(),
                       schema=schema,
                       extend_existing=True,
                       autoload=True)
        cols = [c.name for c in tbl.columns]
        if index is None:
            idx = [sa.func.row_number().over() - 1]
            index = pd.Index((None, ))
        else:
            if not pd.api.types.is_list_like(index):
                index = (index, )
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


class Series(base.BaseFrame, generic.GenericMixin, ops_mixin.OpsMixin):
    ndim = 1

    def __init__(self, index, columns, cte, name):
        super().__init__(index, columns, cte)
        self.name = name

    def __iter__(self):
        for row in self._fetch():
            yield row[-1]

    @property
    def _the_col(self):
        """ Return THE column of the Series. """
        return self._col_at(0)

    def iteritems(self):
        for row in self._fetch():
            idx = row[:-1] if self._is_mindex else row[0]
            yield idx, row[-1]

    def _get_value(self, label, takeable=False):
        if takeable:
            row_count = len(self)
            label = utils.wrap(label, row_count)
            if label < 0 or label > row_count:
                raise IndexError(f"index {label} is out of bounds "
                                 f"for axis 0 with size {row_count}")
            col = sa.select([self._the_col])
            return col.limit(1).offset(label).scalar()
        raise NotImplementedError

    @utils.copied
    def _op(self,
            op,
            other,
            level=None,
            fill_value=None,
            axis=0,
            reverse=False):
        if axis is not None:
            # Since there is only one possible axis for Series,
            # we don't need to do anything besides validation.
            self._get_axis(axis)

        def app_op(lhs, rhs):
            if reverse:
                lhs, rhs = rhs, lhs
            result = coercion.app_op_coerced(op, lhs, rhs)
            if fill_value is None:
                return result
            return sa.func.coalesce(result, fill_value)

        if pd.api.types.is_scalar(other):
            col = app_op(self._the_col, other)
            self._cte = sa.select(self._idx() + [col]).cte()
            return
        if isinstance(other, (Series, pd.Series)):
            other = Series.from_pandas(other, optional=True)
            if self._cte == other._cte:
                # Ensure different names for self join
                self._cte = self._cte.alias()
            col = app_op(self._the_col, other._the_col)
            self._join_idx(other, [col], level=level, inplace=True)
            return
        if isinstance(other, (DataFrame, pd.DataFrame)):
            other = DataFrame.from_pandas(other, optional=True)
            return other.radd(self,
                              axis=axis,
                              level=level,
                              fill_value=fill_value)
        if pd.api.types.is_list_like(other):
            other = list(other)
            if len(other) == 1:
                col = app_op(self._the_col, other[0])
                self._cte = sa.select(self._idx() + [col]).cte()
                return
            row_count = len(self)
            if len(other) != row_count:
                if reverse:
                    lhs, rhs = len(other), row_count
                else:
                    lhs, rhs = row_count, len(other)
                raise ValueError(f"operands could not be broadcast together "
                                 f"with shapes ({lhs},) ({rhs},)")
            other = Series.from_list(other)
            other_rowid = other._idx_at(0)
            this, other, joined = self._paste_join(other, other_rowid)
            col = app_op(this._the_col, other._the_col)
            query = sa.select(this._idx() + [col]).select_from(joined)
            self._cte = query.cte()
            return
        raise TypeError(f"Cannot broadcast np.ndarray with "
                        f"operand of type {type(other)}")

    add, radd = series_op(operator.add)
    sub, rsub = series_op(operator.sub)
    mul, rmul = series_op(operator.mul)
    div, rdiv = series_op(operator.truediv, name="div")
    truediv, rtruediv = series_op(operator.truediv)
    floordiv, rfloordiv = series_op(operator.floordiv)
    mod, rmod = series_op(operator.mod)
    pow, rpow = series_op(operator.pow)

    eq = series_cmp(operator.eq)
    ne = series_cmp(operator.ne)
    le = series_cmp(operator.le)
    lt = series_cmp(operator.lt)
    ge = series_cmp(operator.ge)
    gt = series_cmp(operator.gt)

    def to_pandas(self):
        index = []
        value = []
        for row in self._fetch():
            idx = row[:-1] if self._is_mindex else row[0]
            index.append(idx)
            value.append(row[-1])
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
            raise TypeError("Must be a pandas Series")
        if name is None:
            name = seq.name
        query = sa.union_all(
            *[row_to_query(index, data) for index, data in seq.iteritems()])
        query.bind = db.metadata().bind
        index = pd.Index(seq.index.names)
        columns = pd.Index((name, ))
        return Series(index, columns, query.cte(), name)

    @staticmethod
    def from_list(values, name=None):
        query = sa.union_all(*[
            sa.select([sa.literal(i), sa.literal(v)])
            for i, v in enumerate(values)
        ])
        query.bind = db.metadata().bind
        index = pd.Index([None])
        columns = pd.Index([None])
        return Series(index, columns, query.cte(), name)


__all__ = ["DataFrame", "Series"]
