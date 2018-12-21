from sqlalchemy import create_engine
from sqlalchemy import MetaData


def make_engine():
    """Creates an in memory sqlite database
    From the docs:
        The Engine is the starting point for any SQLAlchemy application.
        It’s “home base” for the actual database and its DBAPI, delivered to the SQLAlchemy application through a
        connection pool and a Dialect, which describes how to talk to a specific kind of database/DBAPI combination.
    """
    return create_engine('sqlite://')


def make_metadata(engine):
    """
    Creates a MetaData object
    From the docs:
        MetaData is a container object that keeps together many different features of a database
        (or multiple databases) being described.
    """
    # Note: Schema can be specified here using
    return MetaData(engine)
