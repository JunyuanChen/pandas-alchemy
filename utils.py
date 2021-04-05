def col_at(i):
    return "c_{}".format(i)


def idx_at(i):
    return "i_{}".format(i)


def label_cols(cols):
    return [cols[i].label(col_at(i)) for i in range(len(cols))]


def label_idx(idx):
    return [idx[i].label(idx_at(i)) for i in range(len(idx))]


def zip_indexers(length, *args):
    return zip(*map(lambda x: range(length) if x is None else x, args))
