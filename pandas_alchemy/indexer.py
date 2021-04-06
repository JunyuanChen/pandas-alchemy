class _iAtIndexer:
    name = 'iat'

    def __init__(self, obj):
        self.obj = obj

    def __getitem__(self, key):
        if not isinstance(key, tuple):
            key = (key,)
        for k in key:
            if not isinstance(k, int):
                err = "iAt based indexing can only have integer indexers"
                raise ValueError(err)
        return self.obj._get_value(*key, takeable=True)

    @property
    def ndim(self):
        return self.obj.ndim
