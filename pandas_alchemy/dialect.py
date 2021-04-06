import math


AUGMENTATION = {}


def augment_engine(engine):
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


def with_raw_connection(f):
    def raw_connection(engine):
        con = engine.raw_connection().connection
        return f(con)
    return raw_connection


@augment('sqlite')
@with_raw_connection
def sqlite_floor_function(con):
    con.create_function('floor', 1, math.floor)
