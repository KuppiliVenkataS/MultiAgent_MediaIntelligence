from neo4j import GraphDatabase

# add authentication details here
driver = GraphDatabase.driver("bolt://neo4j:7687", auth=("neo4j","password"))


MERGE_ENTITY = """
MERGE (e:Entity {id:$id})
SET e.name=$name, e.type=$type, e.aliases=coalesce(e.aliases,[]) + $aliases
"""


MERGE_EVENT = """
MERGE (ev:Event {id:$id}) SET ev.type=$type, ev.when=$when, ev.summary=$summary
WITH ev
UNWIND $actors AS aid MATCH (a:Entity {id:aid}) MERGE (a)-[:PART_OF]->(ev)
"""


# TODO

# call with session.write_transaction(...TBD...)