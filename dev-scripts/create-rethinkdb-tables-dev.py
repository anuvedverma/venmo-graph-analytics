import rethinkdb as r

conn = r.connect('localhost', 28015, db='venmo_graph_analytics_dev').repl()
r.db_drop('venmo_graph_analytics_dev').run(conn)
r.db_create('venmo_graph_analytics_dev').run(conn)
r.table_create('users').run()
r.table_create('community').run()
print r.db_list().run()

