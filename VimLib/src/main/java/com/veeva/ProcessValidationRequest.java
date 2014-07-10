
package vim;

/**
 *
 * @author isomogyi
 */

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.protocol.BasicHttpContext;
import net.sf.json.*;


public class ProcessValidationRequest {
    
    String EndPoint;
    
    
    public ProcessValidationRequest() {
        
    }
    
    public void start(String endPoint) {
        try {
            getDCR(endPoint);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }
    
    void getDCR(String endPoint) throws Exception {
                
       CloseableHttpClient httpclient = HttpClients.createDefault();
          
       try {
           
           HttpPost httppost = new HttpPost("https://eudemo.veevanetwork.com/api/v2.0/auth?username=admin@integration.veevanetwork.com&password=Password3!");
           CloseableHttpResponse response = httpclient.execute(httppost);
           String authResult = EntityUtils.toString(response.getEntity());
            
           //get sessionID
           JSONObject authJsonObject = JSONObject.fromObject(authResult);
           String sessionId = authJsonObject.getString("sessionId");
          
           //dcr call
           HttpGet httpget = new HttpGet("https://eudemo.veevanetwork.com/api/v2.0/change_requests/422440872693466112");
           
           //set sessionId
           httpget.addHeader("Authorization", sessionId);
           response = httpclient.execute(httpget);
           
           //get result of dcr request
           String dcrResult = EntityUtils.toString(response.getEntity());
           JSONObject dcrJsonObject = JSONObject.fromObject(dcrResult); 
            
        } finally {
            httpclient.close();
        }


    }
    
//    public static void main(String[] args) {
//        
//        ProcessValidationRequest pvr = new ProcessValidationRequest();
//        
//        pvr.start(null);
//        
//    }
    
    
}
