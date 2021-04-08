import math
import copy
import pandas as pd
import sqlalchemy as sa

AUGMENTATION = {}
POLYFILL = {}
CURRENT = None


def augment_engine(engine):
    global CURRENT
    CURRENT = copy.copy(POLYFILL)
    for aug in AUGMENTATION.get(engine.name, []):
        aug(engine)


def augment(db_name):
    def decorator(f):
        if db_name not in AUGMENTATION:
            AUGMENTATION[db_name] = [f]
        else:
            AUGMENTATION[db_name].append(f)
        return f

    return decorator


def polyfill(f):
    POLYFILL[f.__name__] = f
    return f


def refill(name):
    def decorator(f):
        def refiller(engine):
            CURRENT[name] = f

        return refiller

    return decorator


def with_raw_connection(f):
    def raw_connection(engine):
        con = engine.raw_connection().connection
        return f(con)

    return raw_connection


@polyfill
def full_outer_join(lhs, rhs, cond, selects):
    left = sa.select(selects).select_from(lhs.join(rhs, cond, isouter=True))
    right = sa.select(selects).select_from(rhs.join(lhs, cond, isouter=True))
    return sa.union_all(left, right.where(cond.is_(None)))


@augment("postgresql")
@refill("full_outer_join")
def postgresql_full_outer_join(lhs, rhs, cond, selects):
    return sa.select(selects).select_from(lhs.join(rhs, cond, full=True))


@augment("sqlite")
@with_raw_connection
def sqlite_floor_function(con):
    con.create_function("floor", 1, math.floor)


@augment("sqlite")
def sqlite_NA_adapters(engine):
    register = engine.dialect.dbapi.register_adapter
    register(pd.NA.__class__, lambda _: None)
    register(pd.NaT.__class__, lambda _: None)


@augment("postgresql")
def postgresql_NA_adapters(engine):
    ext = engine.dialect.dbapi.extensions

    def adapter(value):
        return ext.AsIs("NULL")

    ext.register_adapter(pd.NA.__class__, adapter)
    ext.register_adapter(pd.NaT.__class__, adapter)


__all__ = [
    "AUGMENTATION", "POLYFILL", "CURRENT", "augment_engine", "augment",
    "polyfill", "refill", "with_raw_connection"
]
