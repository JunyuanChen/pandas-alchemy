import sqlalchemy as sa


METADATA = None


def metadata():
    if METADATA is None:
        raise RuntimeError("Not connected")
    return METADATA


def init_db(*args, **kwargs):
    global METADATA
    if METADATA is not None:
        raise RuntimeError("Already connected")
    engine = sa.create_engine(*args, **kwargs)
    METADATA = sa.MetaData(engine)


def close_db():
    global METADATA
    if METADATA is None:
        raise RuntimeError("Not connected")
    METADATA.bind.dispose()
    METADATA = None


__all__ = ["METADATA", "metadata", "init_db", "close_db"]
