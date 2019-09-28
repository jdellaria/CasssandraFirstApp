package com.mycompany.app;


import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;


/**
 * Hello world!
 *
 */
public class App
{

    static String[] CONTACT_POINTS = {"127.0.0.1"};
    static int PORT = 9042;

    public static void main1( String[] args )
    {
        System.out.println( "Hello World!" );
    }

    public static void main( String[] args )
    {

      App client = new App();

      try {

        client.connect(CONTACT_POINTS, PORT);
        client.createSchema();
        client.loadData();
        client.querySchema();

      } finally {
        client.close();
      }
    }

    Cluster cluster;

    Session session;

    /**
     * Initiates a connection to the cluster specified by the given contact point.
     *
     * @param contactPoints the contact points to use.
     * @param port the port to use.
     */
    public void connect(String[] contactPoints, int port) {

      cluster = Cluster.builder().addContactPoints(contactPoints).withPort(port).build();

      System.out.printf("Connected to cluster: %s%n", cluster.getMetadata().getClusterName());

      session = cluster.connect();
    }

    /** Creates the schema (keyspace) and tables for this example. */
    public void createSchema() {

      session.execute(
          "CREATE KEYSPACE IF NOT EXISTS simplex WITH replication "
              + "= {'class':'SimpleStrategy', 'replication_factor':1};");


      session.execute(
          "CREATE KEYSPACE IF NOT EXISTS nlp WITH replication "
              + "= {'class':'SimpleStrategy', 'replication_factor':1};");


      session.execute(
          "CREATE TABLE IF NOT EXISTS simplex.songs ("
              + "id uuid PRIMARY KEY,"
              + "title text,"
              + "album text,"
              + "artist text,"
              + "tags set<text>,"
              + "data blob"
              + ");");

      session.execute(
          "CREATE TABLE IF NOT EXISTS simplex.playlists ("
              + "id uuid,"
              + "title text,"
              + "album text, "
              + "artist text,"
              + "song_id uuid,"
              + "PRIMARY KEY (id, title, album, artist)"
              + ");");
              
      session.execute(
          "CREATE TABLE IF NOT EXISTS nlp.documents ("
              + "id uuid PRIMARY KEY,"
              + "title text,"
              + "content text,"
              + "tags set<text>,"
              + "data blob"
              + ");");
      session.execute(
          "CREATE TABLE IF NOT EXISTS nlp.sentences ("
              + "id uuid PRIMARY KEY,"
              + "documentId uuid,"
              + "sentance text"
              + ");");


    }

    /** Inserts data into the tables. */
    public void loadData() {

      session.execute(
          "INSERT INTO simplex.songs (id, title, album, artist, tags) "
              + "VALUES ("
              + "756716f7-2e54-4715-9f00-91dcbea6cf50,"
              + "'La Petite Tonkinoise',"
              + "'Bye Bye Blackbird',"
              + "'Joséphine Baker',"
              + "{'jazz', '2013'})"
              + ";");

      session.execute(
          "INSERT INTO simplex.playlists (id, song_id, title, album, artist) "
              + "VALUES ("
              + "2cc9ccb7-6221-4ccb-8387-f22b6a1b354d,"
              + "756716f7-2e54-4715-9f00-91dcbea6cf50,"
              + "'La Petite Tonkinoise',"
              + "'Bye Bye Blackbird',"
              + "'Joséphine Baker'"
              + ");");
    }

    /** Queries and displays data. */
    public void querySchema() {

      ResultSet results =
          session.execute(
              "SELECT * FROM simplex.playlists "
                  + "WHERE id = 2cc9ccb7-6221-4ccb-8387-f22b6a1b354d;");

      System.out.printf("%-30s\t%-20s\t%-20s%n", "title", "album", "artist");
      System.out.println(
          "-------------------------------+-----------------------+--------------------");

      for (Row row : results) {

        System.out.printf(
            "%-30s\t%-20s\t%-20s%n",
            row.getString("title"), row.getString("album"), row.getString("artist"));
      }
    }

    /** Closes the session and the cluster. */
    public void close() {
      session.close();
      cluster.close();
    }
}
