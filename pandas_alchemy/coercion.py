import operator
import sqlalchemy as sa
from . import dialect

COERCIONS = {}


def coerce(op, lhs_type, rhs_type=None.__class__):
    def decorator(f):
        if op not in COERCIONS:
            COERCIONS[op] = {(lhs_type, rhs_type): f}
        else:
            COERCIONS[op][(lhs_type, rhs_type)] = f
        return f

    return decorator


def get_type(value):
    if hasattr(value, 'type') and hasattr(value.type, 'python_type'):
        return value.type.python_type
    return type(value)


def cast(value, new_type):
    if hasattr(value, 'type') and hasattr(value.type, 'python_type'):
        return sa.cast(value, new_type)
    if isinstance(new_type, type):
        # sa.INTEGER vs sa.INTEGER()
        new_type = new_type()
    return new_type.python_type(value)


def app_op_coerced(op, lhs, rhs=None):
    if op not in COERCIONS:
        return op(lhs, rhs)
    lhs_type = get_type(lhs)
    rhs_type = get_type(rhs)
    for key, value in COERCIONS[op].items():
        if issubclass(lhs_type, key[0]) and issubclass(rhs_type, key[1]):
            return value(op, lhs, rhs)
    return op(lhs, rhs)


NUMERIC = (int, float, complex)


def sane_division(lhs, rhs, floor=False):
    # sa.cast() is used to ensure lhs and rhs are Column expressions
    lhs = sa.cast(lhs, sa.FLOAT)
    rhs = sa.cast(rhs, sa.FLOAT)
    is_inf = dialect.CURRENT["is_inf"]
    is_nan = dialect.CURRENT["is_nan"]
    sign = sa.func.sign
    # Ideally we should be able to handle 0.0 vs -0.0, but due to
    # the limitations of SQL we will just treat them all as 0.0
    return sa.case((is_inf(lhs) & is_inf(rhs), float('nan')),
                   (is_nan(lhs), lhs), (is_inf(rhs), 0.0),
                   (rhs == 0, sign(lhs) * float("inf")),
                   else_=sa.func.floor(lhs / rhs) if floor else lhs / rhs)


@coerce(operator.truediv, NUMERIC, NUMERIC)
def truediv_numeric(_, lhs, rhs):
    return sane_division(lhs, rhs)


@coerce(operator.floordiv, NUMERIC, NUMERIC)
def floordiv_numeric(_, lhs, rhs):
    return sane_division(lhs, rhs, floor=True)


@coerce(operator.mod, NUMERIC, NUMERIC)
def mod_numeric(_, lhs, rhs):
    expr = sa.cast(lhs, sa.NUMERIC) % sa.cast(rhs, sa.NUMERIC)
    # See sane_division()
    lhs = sa.cast(lhs, sa.FLOAT)
    rhs = sa.cast(rhs, sa.FLOAT)
    is_inf = dialect.CURRENT["is_inf"]
    is_nan = dialect.CURRENT["is_nan"]
    sign = sa.func.sign
    return sa.case((is_inf(lhs) | is_nan(lhs) | (rhs == 0), float("nan")),
                   (is_inf(rhs) & (sign(lhs) == -sign(rhs)), rhs),
                   (is_inf(rhs) & (sign(lhs) != -sign(rhs)), lhs),
                   else_=expr)


@coerce(operator.add, bool, bool)
@coerce(operator.sub, bool, bool)
@coerce(operator.mul, bool, bool)
@coerce(operator.truediv, bool, bool)
@coerce(operator.pow, bool, bool)
def op_bool_bool(op, lhs, rhs):
    lhs = cast(lhs, sa.INTEGER)
    rhs = cast(rhs, sa.INTEGER)
    return app_op_coerced(op, lhs, rhs)


@coerce(operator.add, bool, NUMERIC)
@coerce(operator.sub, bool, NUMERIC)
@coerce(operator.mul, bool, NUMERIC)
@coerce(operator.truediv, bool, NUMERIC)
@coerce(operator.pow, bool, NUMERIC)
def op_bool_numeric(op, lhs, rhs):
    lhs = cast(lhs, sa.INTEGER)
    return app_op_coerced(op, lhs, rhs)


@coerce(operator.add, NUMERIC, bool)
@coerce(operator.sub, NUMERIC, bool)
@coerce(operator.mul, NUMERIC, bool)
@coerce(operator.truediv, NUMERIC, bool)
@coerce(operator.pow, NUMERIC, bool)
def op_numeric_bool(op, lhs, rhs):
    rhs = cast(rhs, sa.INTEGER)
    return app_op_coerced(op, lhs, rhs)


__all__ = ['COERCIONS', 'coerce', 'app_op_coerced']
