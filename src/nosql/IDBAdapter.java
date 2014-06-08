/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package nosql;

/**
 *
 * @author Alex
 */
public interface IDBAdapter {
    public void connect();
    public void disconnect();
    public float insertFirstBulk();
    public float insertSecondBulk();
    public float executeQuery1(); 
    public float executeQuery2();  
    public float executeQuery3();
    public float executeQuery4();
}
