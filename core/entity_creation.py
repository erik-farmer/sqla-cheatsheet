from core.utils import make_engine
from core.utils import make_metadata
from core.table_creation import make_table


def make_pet_entities():
    engine = make_engine()
    metadata = make_metadata(engine)
    table = make_table(metadata)
    table.create()
    table.insert().values({'type': 'dog', 'name': 'Wedge'})


if __name__ == '__main__':
    make_pet_entities()
