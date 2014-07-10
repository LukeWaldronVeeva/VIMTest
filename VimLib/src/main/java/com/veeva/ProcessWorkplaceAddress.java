package vim;

/**
 *
 * @author isomogyi
 */
import java.util.HashMap;
import java.util.Calendar;
import java.util.List;
import java.util.ArrayList;
import java.util.Date;
import java.sql.ResultSet;
import java.sql.DriverManager;
import java.sql.Statement;
import java.sql.ResultSetMetaData;
import java.sql.Connection;
import org.apache.commons.lang3.StringUtils;

import java.text.SimpleDateFormat;

public class ProcessWorkplaceAddress implements IBatch {
    
    int FetchSize = 200;
    int BatchSize = 200;
    ResultSet Result;
    ResultSetMetaData ResultMD;
    Connection conn;
    Date LastRunTime;
    String Country;
    
    public ProcessWorkplaceAddress() {    
    }
      
    @Override
    public void start(String host, String port, String database, String userName, String password, String project, String jobName, String country) throws Exception {
        
        //set country code
        Country = country;
        
        //Setup connection
        Class.forName("com.mysql.jdbc.Driver");
        conn =DriverManager.getConnection("jdbc:mysql://localhost:3306/VIM","root","Veeva456");
        Statement stmt = conn.createStatement();
        stmt.setFetchSize(FetchSize);
        
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
                 
        String stringLastRunTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(LastRunTime);
        
        //Get workplaces with addresses
        sql =
            "SELECT "
            + " sa.*,"         
            + " swa.wkpadr_Process_Code,"
            + " swa.wkpadr_External_Id,"
            + " sw.wkp_External_ID,"
            + " sw.wkp_Name,"
            + " swa.PrimaryFlag,"
            + " swa.BillingFlag,"
            + " swa.MailingFLag,"
            + " swa.DeliveryFlag,"
            + " swa.SecondaryFlag"
            + " FROM StageWorkplaceAddress swa"
            + " JOIN StageAddress sa on swa.adr_External_ID = sa.adr_External_ID "
            + " JOIN StageWorkplace sw on swa.wkp_External_ID = sw.wkp_External_ID"
            + " WHERE ("
            + " swa.LastModifiedTime >= '" + stringLastRunTime
            + "' OR sw.LastModifiedTime >= '" + stringLastRunTime
            + "' OR sa.LastModifiedTime >= '" + stringLastRunTime
            + "' ) "
            + " AND swa.wkpadr_Country = '" + Country
            + "' ORDER BY swa.lastModifiedTime"
            ;
        
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
    public List<HashMap<String,String>> getNextBatch() throws Exception {
        
        List<HashMap<String,String>> addressRecords = new ArrayList();
        String accountExternalId = null;
        String wkpExternalID = null;
        String adrExternalID = null;
        String wkpadrExternalID = null;
        String wkpName = null;
        boolean activeFlag = false;
        boolean primaryFlag = false;
        boolean secondaryFlag = false;
        boolean billingFlag = false;
        boolean mailingFlag = false;
        boolean deliveryFlag = false;
        
        
        while(Result.next()) {
            
            //add all Address fields here + Workplace Account
            HashMap<String,String> addressRecord = new HashMap();
            
            //get required fields
            for(int i=1; i<=(ResultMD.getColumnCount() ); i++) {
                
                switch (ResultMD.getColumnName(i).toLowerCase()) {
                      case "wkp_external_id": wkpExternalID = Result.getString(i); break;
                      case "wkp_name": wkpName = Result.getString(i); break;
                      case "wkpadr_external_id": wkpadrExternalID = Result.getString(i); break;
                      case "primaryflag": primaryFlag = Result.getBoolean(i); break;
                      case "secondaryflag": secondaryFlag = Result.getBoolean(i); break;
                      case "billingflag": billingFlag = Result.getBoolean(i); break;
                      case "deliveryflag": deliveryFlag = Result.getBoolean(i); break;
                      case "mailingflag": mailingFlag = Result.getBoolean(i); break;              
                }
                
                // create addressRecord (only address table fields)
                if (ResultMD.getColumnName(i).toLowerCase().startsWith("adr_")) {
                    addressRecord.put(ResultMD.getColumnName(i), Result.getString(i));
                }
                  
            }
            
            if (!StringUtils.isEmpty(wkpExternalID) && !StringUtils.isEmpty(wkpName)) {
                //add workplace id to address record
                addressRecord.put("wkp_External_ID", wkpExternalID);
                //add unique id to address record
                addressRecord.put("wkpadr_External_Id", wkpadrExternalID);        
            } else {
                continue;
            }
            
            //Check Country
            if ( StringUtils.isEmpty(addressRecord.get("adr_Country")) && !StringUtils.isEmpty(Country) ) {
                addressRecord.put("adr_Country",Country);
            }
                
            //if any of the flags are true then it is active otherwise inactive
            activeFlag = (primaryFlag || secondaryFlag || billingFlag || mailingFlag || deliveryFlag);
            
            //add flags to address record
            addressRecord.put("activeFlag", Boolean.toString(activeFlag));
            addressRecord.put("primaryFlag", Boolean.toString(primaryFlag));
            addressRecord.put("secondaryFlag", Boolean.toString(secondaryFlag));
            addressRecord.put("mailingFlag", Boolean.toString(mailingFlag));
            addressRecord.put("deliveryFlag", Boolean.toString(deliveryFlag));
            addressRecord.put("billingFlag", Boolean.toString(billingFlag));
            
            //add to address records
            addressRecords.add(addressRecord);
            
            if (addressRecords.size()>=BatchSize) break;
        }
        
        if (addressRecords.isEmpty()) {
            return null;   
        } else return addressRecords;

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
    
    public static void main(String[] args) {
        
        try {
        
        ProcessWorkplaceAddress pwa = new ProcessWorkplaceAddress();
        
        pwa.start(null, null, null, null, null, "BIOGEN", "LoadFileToDB", "SE");
        
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

}
