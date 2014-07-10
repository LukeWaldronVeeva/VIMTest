 package vim;

/**
 *
 * @author isomogyi
 */
import java.util.*;

interface IBatch {
    
    public void start(String Host, String Database, String Port, String UserName, String Password, String project, String jobName, String Country) throws Exception;
    public void setSQLFetchSize(int fetchSize);
    public void setBatchSize(int batchSize);
    public List<HashMap<String,String>> getNextBatch() throws Exception;
    
}
