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


@coerce(operator.truediv, NUMERIC, NUMERIC)
def truediv_numeric(_, lhs, rhs):
    return dialect.CURRENT["sane_division"](lhs, rhs)


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
