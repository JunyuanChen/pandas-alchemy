class OpsMixin:
    def __add__(self, other):
        return self.add(other)

    def __radd__(self, other):
        return self.radd(other)

    def __sub__(self, other):
        return self.sub(other)

    def __rsub__(self, other):
        return self.rsub(other)

    def __mul__(self, other):
        return self.mul(other)

    def __rmul__(self, other):
        return self.rmul(other)

    def __div__(self, other):
        return self.div(other)

    def __rdiv__(self, other):
        return self.rdiv(other)

    def __truediv__(self, other):
        return self.truediv(other)

    def __rtruediv__(self, other):
        return self.rtruediv(other)

    def __floordiv__(self, other):
        return self.floordiv(other)

    def __rfloordiv__(self, other):
        return self.rfloordiv(other)

    def __mod__(self, other):
        return self.mod(other)

    def __rmod__(self, other):
        return self.rmod(other)

    def __pow__(self, other):
        return self.pow(other)

    def __rpow__(self, other):
        return self.rpow(other)

    def __eq__(self, other):
        return self.eq(other)

    def __ne__(self, other):
        return self.ne(other)

    def __le__(self, other):
        return self.le(other)

    def __lt__(self, other):
        return self.lt(other)

    def __ge__(self, other):
        return self.ge(other)

    def __gt__(self, other):
        return self.gt(other)


__all__ = ["OpsMixin"]
