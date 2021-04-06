import copy
import functools


def copied(f):
    @functools.wraps(f)
    def self_copied(self, *args, **kwargs):
        inplace = kwargs.get("inplace")
        if inplace:
            del kwargs["inplace"]
            return f(self, *args, **kwargs)
        this = copy.copy(self)
        result = f(this, *args, **kwargs)
        return this if result is None else result
    return self_copied


def merge(dict_1, dict_2):
    result = copy.copy(dict_1)
    result.update(dict_2)
    return result


def wrap(value, bound):
    return bound + value if value < 0 else value


__all__ = ["copied", "merge", "wrap"]
