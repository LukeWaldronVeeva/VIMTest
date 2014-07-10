/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package vim;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

/**
 *
 * @author Luke Waldron
 */
public class ProcessActivity implements IBatch {

    private int FetchSize = 200;
    private int BatchSize = 200;
    private ResultSet Result;
    private ResultSetMetaData ResultMD;
    private Connection conn;
    private Date LastRunTime;
    private String Country;
    
    @Override
    public void start(String host, String database, String port, String UserName, String Password, String project, String jobName, String country) throws Exception {
        
        //set country code
        Country = country;
        
        //Setup connection
        Class.forName("com.mysql.jdbc.Driver");
        conn =DriverManager.getConnection("jdbc:mysql://"+host+":"+port+"/"+database,UserName,Password);
        Statement stmt = conn.createStatement();
        stmt.setFetchSize(FetchSize);
        
        String stringLastRunTime = getLastRuntime(stmt, project, jobName);
        
        //Get all the records from the Stage Activity table
        String sql = "SELECT * FROM StageActivity WHERE ("
            + " lastModifiedTime >= '" + stringLastRunTime
            + "') AND ("
            + "act_Country = '"+country+"')";
        
        Result = stmt.executeQuery(sql);
        ResultMD = Result.getMetaData();
    }

    @Override
    public void setSQLFetchSize(int fetchSize) {
        FetchSize = fetchSize;
    }

    @Override
    public void setBatchSize(int batchSize) {
        BatchSize = batchSize;
    }

    @Override
    public List<HashMap<String, String>> getNextBatch() throws Exception {
        
        List<HashMap<String,String>> activityRecords = new ArrayList();
        String roleVarName;
        
        while(Result.next()) {
            //
            HashMap<String,String> activityRecord = new HashMap();
            
            // iterate through the 10 role fields
            for(int i=1; i<=10; i++){
                roleVarName = "act_Role_"+i;
                // If the role is null do not save the Activity
                if(Result.getString(roleVarName)!= null && !Result.getString(roleVarName).isEmpty()){
                    //get required fields
                    for(int j=1; j<=(ResultMD.getColumnCount() ); j++) {
                        // save the role for the current iteration with the iterator appended
                        if(ResultMD.getColumnName(j).equals(roleVarName))
                            activityRecord.put("act_Role", Result.getString(j)+"_"+i);
                        // appended the iterator to ACT_ID_CEGEDIM
                        else if (ResultMD.getColumnName(j).equals("act_External_ID"))
                            activityRecord.put(ResultMD.getColumnName(j), Result.getString(j)+"_"+i);
                        // save all other variables which are not roles
                        else if(!ResultMD.getColumnName(j).contains("act_Role_"))
                            activityRecord.put(ResultMD.getColumnName(j), Result.getString(j)); 
                    }
                    //add to activity records
                    activityRecords.add(activityRecord);
                    activityRecord = new HashMap();
                }
            }
        }
        
        
        if (activityRecords.isEmpty())
            return null;   
        else
            return activityRecords;
    }
    
    @Override
    protected void finalize() throws Throwable {
        try {
            super.finalize();
            conn.close();
        } catch(Exception e) {
            throw e;
        }
        
    }
    
    private String getLastRuntime(Statement stmt, String project, String jobName) throws SQLException{
        
        //Setup LastRunTime
        String sql = "select jobFinishTime from JobActivity where project = '"+project+"' and jobName = '"+jobName+"' and jobStatus = 'Finished' order by id desc";
        Result = stmt.executeQuery(sql);
        if (Result.next()) {
            LastRunTime = Result.getDate(1);
        }
        
        if (LastRunTime == null) {
           Calendar defaultTime = Calendar.getInstance();
           defaultTime.add(Calendar.DAY_OF_MONTH, -90);
           LastRunTime = defaultTime.getTime();
        }
                 
        return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(LastRunTime);
    }
    
    /*public static void main(String[] args) {
        
        try {
        
        ProcessActivity pwa = new ProcessActivity();
        
        pwa.start(null, null, null, null, null, "BIOGEN", "LoadFileToDB", "SE");
        
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        
    }*/
}
