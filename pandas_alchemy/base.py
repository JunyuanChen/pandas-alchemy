import sqlalchemy as sa
from . import utils


class BaseFrame:
    _AXIS_MAPPER = {0: 0, "index": 0, "rows": 0}

    def __init__(self, index, columns, cte):
        self._index = index
        self._columns = columns
        self._cte = cte

    @property
    def _is_mindex(self):
        return len(self._index) > 1

    def _idx(self):
        return list(self._cte.columns)[:len(self._index)]

    def _cols(self):
        return list(self._cte.columns)[len(self._index):]

    def _idx_at(self, i):
        return self._cte.columns[i]

    def _col_at(self, i, null=True):
        if i == -1 and null:
            return sa.sql.expression.Null()
        i += len(self._index)
        return self._cte.columns[i]

    def _get_axis(self, axis):
        axis_num = self._AXIS_MAPPER.get(axis)
        if axis_num is None:
            err = "No axis named {} for object type {}"
            raise ValueError(err.format(axis, self.__class__.__name__))
        return axis_num

    def _fetch(self):
        return sa.select(self._cte).execute()

    @utils.copied
    def _add_rowid(self):
        rowid = sa.literal_column("ROW_NUMBER() OVER () - 1", sa.Integer)
        cte_columns = list(self._cte.columns)
        cte_columns.append(rowid.label("i_rowid"))
        self._cte = sa.select(cte_columns).cte()

    def _join_cols(self, other_index, how="outer"):
        joined, l_idxer, r_idxer = self._columns.join(other_index, how=how,
                                                      return_indexers=True)
        l_idxer = range(len(joined)) if l_idxer is None else l_idxer
        r_idxer = range(len(joined)) if r_idxer is None else r_idxer
        return joined, zip(l_idxer, r_idxer)

    @utils.copied
    def _paste_join(self, other, other_rowid=None):
        """ Join two BaseFrame on rowid. """
        self._add_rowid(inplace=True)
        self_cols = list(self._cte.columns)
        if other_rowid is None:
            other = other._add_rowid()
            other_cols = list(other._cte.columns)
        else:
            other_cols = list(other._cte.columns)
            other_cols.append(other_rowid)
        join_cond = self_cols[-1] == other_cols[-1]
        return self_cols[:-1], other_cols[:-1], join_cond
