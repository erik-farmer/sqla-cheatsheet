from sqlalchemy import Column
from sqlalchemy import ForeignKey
from sqlalchemy import Integer
from sqlalchemy import select
from sqlalchemy import String
from sqlalchemy import Table

from core.utils import make_engine
from core.utils import make_metadata

ENGINE = make_engine()
METADATA = make_metadata(ENGINE)

PETS = Table('pets', METADATA,
             Column('id', Integer, primary_key=True),
             Column('type', ForeignKey('pet_types.id')),
             Column('name', String)
             )

PET_TYPES = Table('pet_types', METADATA,
                  Column('id', Integer, primary_key=True),
                  Column('type', String)
                  )


def make_tables():
    METADATA.create_all()


def make_types():
    types = [
        {'type': 'dog'},
        {'type': 'cat'},
        {'type': 'bird'},
    ]
    insert_statement = PET_TYPES.insert().values(types)
    with ENGINE.connect() as conn:
        conn.execute(insert_statement)


def make_pets():
    with ENGINE.connect() as conn:
        dog_id = PET_TYPES.select(PET_TYPES.c.type=='dog')
        result = conn.execute(dog_id)
    dog_type = result.fetchone()
    dog_id = dog_type.id
    with ENGINE.connect() as conn:
        conn.execute(PETS.insert().values([{'type': dog_id, 'name': 'Wedge'}]))


def make_pet_with_subquery():
    with ENGINE.connect() as conn:
        cat_id_query = select([PET_TYPES.c.id]).where(PET_TYPES.c.type == 'cat')
        conn.execute(PETS.insert().values([{'type': cat_id_query, 'name': 'Chester'}]))


if __name__ == '__main__':
    make_tables()
    make_types()
    make_pets()
    make_pet_with_subquery()