from sqlalchemy import Column, Integer, String, Table
from sqlalchemy.sql import select

from core.utils import make_engine
from core.utils import make_metadata

ENGINE = make_engine()
METADATA = make_metadata(ENGINE)

COMMENTS = Table('comments', METADATA,
    Column('id', Integer, primary_key=True),
    Column('parent', Integer),
    Column('content', String)
)

CLOSURE = Table('closure', METADATA,
    Column('id', Integer, primary_key=True),
    Column('parent', Integer),
    Column('child', Integer),
    Column('depth', Integer),
)


def make_tables():
    METADATA.create_all()


def create_comment(content, parent_id):
    with ENGINE.connect() as conn:
        stmt = COMMENTS.insert().values({'content': content, 'parent': parent_id})
        comment_result = conn.execute(stmt)
        new_id = comment_result.inserted_primary_key[0]
        # Set a new comment as new closure with a depth of zero.
        conn.execute(CLOSURE.insert().values({'parent': new_id, 'child': new_id, 'depth': 0}))
    return new_id


def create_closure(parent_id, child_id):
    """Generated SQL:
    INSERT INTO closure (parent, child, depth)
    SELECT closure_1.parent, closure_2.child, closure_1.depth + closure_2.depth + ? AS anon_1
        FROM closure AS closure_1, closure AS closure_2
        WHERE closure_1.child = ? AND closure_2.parent = ?
    """
    p, c = CLOSURE.alias(), CLOSURE.alias()
    subq = select([p.c.parent, c.c.child, (p.c.depth + c.c.depth+1)]).where(
        p.c.child == parent_id).where(
        c.c.parent == child_id
    )
    with ENGINE.connect() as conn:
        # from_select docs
        # http://docs.sqlalchemy.org/en/latest/core/dml.html?highlight=from_select#sqlalchemy.sql.expression.Insert.from_select
        conn.execute(CLOSURE.insert().from_select(['parent', 'child', 'depth'], subq))


if __name__ == '__main__':
    NON_CHILD_PARENT_ID = 0
    make_tables()

    new_id = create_comment('Hi my name is Foo', NON_CHILD_PARENT_ID)
    create_closure(NON_CHILD_PARENT_ID, new_id)

    response_id = create_comment('Hi Foo! My name is Bar', new_id)
    create_closure(new_id, response_id)

    final_id = create_comment('It was nice to meet you Bar', response_id)
    create_closure(response_id, final_id)

    # To verify final output...
    # with ENGINE.connect() as conn:
    #     closure = conn.execute(
    #         select([CLOSURE.c.parent, CLOSURE.c.child, CLOSURE.c.depth])
    #     ).fetchall()
    #     for i in closure:
    #         print(i)
    """ Output:
    (1, 1, 0) # Self reference
    (2, 2, 0) # Self reference
    (1, 2, 1) # 2 responds to 1 at a depth of 1
    (3, 3, 0) # Self reference
    (2, 3, 1) # 3 responds to 2 at a depth of 1
    (1, 3, 2) # 3 is ALSO a response to 1 at a depth of 2
    
    This allows us to ask things like:
     'What are all the child comments given a parent id?'
     'What is the longest thread?'
    """
    from collections import defaultdict
    # lets pretend we order by parent and then depth
    closure = [
        (1, 1, 0),
        (1, 2, 1),
        (1, 3, 2),
        (2, 2, 0),
        (3, 3, 0),
        (2, 3, 1),
    ]
    for entry in closure:
        parent, child, depth = entry[0], entry[1], entry[2]
