import operator
import sqlalchemy as sa

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


@coerce(operator.add, bool, bool)
@coerce(operator.sub, bool, bool)
@coerce(operator.mul, bool, bool)
@coerce(operator.pow, bool, bool)
def op_bool_bool(op, lhs, rhs):
    return op(cast(lhs, sa.INTEGER), cast(rhs, sa.INTEGER))


@coerce(operator.add, bool, NUMERIC)
@coerce(operator.sub, bool, NUMERIC)
@coerce(operator.mul, bool, NUMERIC)
@coerce(operator.pow, bool, NUMERIC)
def op_bool_numeric(op, lhs, rhs):
    return op(cast(lhs, sa.INTEGER), rhs)


@coerce(operator.add, NUMERIC, bool)
@coerce(operator.sub, NUMERIC, bool)
@coerce(operator.mul, NUMERIC, bool)
@coerce(operator.pow, NUMERIC, bool)
def op_numeric_bool(op, lhs, rhs):
    return op(lhs, cast(rhs, sa.INTEGER))


__all__ = ['COERCIONS', 'coerce', 'app_op_coerced']
