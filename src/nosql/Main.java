/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package nosql;

/**
 *
 * @author Alex
 */
public class Main {

    public static void main (String args[]) {
        neo4jExecution();
    }

    public static void neo4jExecution()
    {
        System.out.println("Neo4j execution start");
        DBAdapterNeo4j adapter = new DBAdapterNeo4j();
        adapter.connect();
        System.out.println("Neo4j database connection established");

//        adapter.insertOperation();
        adapter.obtainQueryParameters();

        adapter.doQuery1();
        adapter.doQuery2();
        adapter.doQuery3();
        adapter.doQuery4();
        
        adapter.disconnect();
    }

}
