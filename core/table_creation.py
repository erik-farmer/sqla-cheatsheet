from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy import Table

from core.utils import make_engine
from core.utils import make_metadata


def make_table(metadata_obj):
    """
    Using our metadata object we can define a basic table.
    Note:
         One column MUST be identified as the `primary_key`
    """
    pets_table = Table('pets', metadata_obj,
        Column('id', Integer, primary_key=True),
        Column('type', String),
        Column('name', String)
    )
    return pets_table


if __name__ == '__main__':
    engine = make_engine()
    metadata = make_metadata(engine)
    table = make_table(metadata)
    table.create() # metadata.create_all could be used as well
