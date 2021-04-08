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
        total = len(self._index) + len(self._columns)
        return list(self._cte.columns)[len(self._index):total]

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
            raise ValueError(f"No axis named {axis} for object "
                             f"type {self.__class__.__name__}")
        return axis_num

    def _fetch(self):
        return sa.select(self._cte).execute()

    @utils.copied
    def _add_rowid(self):
        cte_columns = list(self._cte.columns)
        cte_columns.append(sa.func.row_number().over() - 1)
        self._cte = sa.select(cte_columns).cte()

    def _join_cols(self, other_index, how="outer"):
        joined, l_idxer, r_idxer = self._columns.join(other_index,
                                                      how=how,
                                                      return_indexers=True)
        l_idxer = range(len(joined)) if l_idxer is None else l_idxer
        r_idxer = range(len(joined)) if r_idxer is None else r_idxer
        return joined, zip(l_idxer, r_idxer)

    def _join_idx(self, other, level=None):
        if not self._is_mindex and not other._is_mindex:
            join_cond = self._idx_at(0) == other._idx_at(0)
            idx = [sa.func.coalesce(self._idx_at(0), other._idx_at(0))]
            return self._index, idx, join_cond
        raise NotImplementedError

    @utils.copied
    def _paste_join(self, other, other_rowid=None):
        """ Join two BaseFrame on rowid. """
        self._add_rowid(inplace=True)
        if other_rowid is None:
            other = other._add_rowid()
            other_rowid = other._cte.columns[-1]
        total = len(self._index) + len(self._columns)
        join_cond = self._cte.columns[total] == other_rowid
        joined = self._cte.join(other._cte, join_cond)
        return self, other, joined


__all__ = ["BaseFrame"]
