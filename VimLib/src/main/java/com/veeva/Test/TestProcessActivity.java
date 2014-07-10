/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package vim.Test;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import vim.ProcessActivity;

/**
 *
 * @author Luke Waldron
 */
public class TestProcessActivity {
    public static void main(String[] args) {
        ProcessActivity processActivity = new ProcessActivity();
        List<HashMap<String,String>> activityRecords;
        processActivity.setSQLFetchSize(100);
        String host = "localhost";
        String database = "VIM";
        String port = "3306";
        String UserName = "root";
        String Password = "Veeva456";
        String project = "BIOGEN"; 
        String jobName = "LoadFileToDB";
        String country = "SE";
        
        processActivity.setBatchSize(100);
        try {
            
            processActivity.start(host, database, port, UserName, Password, project, jobName, country);
            // get the records
            activityRecords = processActivity.getNextBatch();
            // print the records
            for(HashMap<String,String> activityRecord : activityRecords) {
                Iterator<String> keySetIterator = activityRecord.keySet().iterator();
                while(keySetIterator.hasNext()){
                    String key = keySetIterator.next();
                    System.out.println("key: " + key + " value: " + activityRecord.get(key));
                }
            }

        } catch (Exception ex) {
            Logger.getLogger(TestProcessActivity.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
}
