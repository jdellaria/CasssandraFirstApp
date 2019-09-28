
to compile:
mvn compile

to execute:
mvn exec:java -Dexec.mainClass="com.mycompany.app.App"

Cassandra Command Line:
cqlsh
cqlsh> describe keyspaces;
cqlsh> use system_schema;
cqlsh:system_schema> select keyspace_name, table_name from tables where keyspace_name = 'nlp';
cqlsh> use nlp;
cqlsh:nlp> describe tables;



