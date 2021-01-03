package lambda;
import com.amazonaws.services.lambda.runtime.ClientContext;
import com.amazonaws.services.lambda.runtime.CognitoIdentity;
import com.amazonaws.services.lambda.runtime.Context; 
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.LinkedList;
import java.util.Properties;
import saaf.Inspector;
import java.util.HashMap;
import saaf.Response;

/**
 * lambda.S3Query_RDS::handleRequest
 *
 * @author Bharti Bansinge
 */
public class S3Query_RDS implements RequestHandler<Request, HashMap<String, Object>> {

    /**
     * Lambda Function Handler
     * 
     * @param request Request POJO with defined variables from Request.java
     * @param context 
     * @return HashMap that Lambda will automatically convert into JSON.
     */
    public HashMap<String, Object> handleRequest(Request request, Context context) {

        // Create logger
        LambdaLogger logger = context.getLogger();        

        //Collect inital data.
        Inspector inspector = new Inspector();
        inspector.inspectAll();
        
        //****************START FUNCTION IMPLEMENTATION*************************       
        
        //Create and populate a separate response object for function output. (OPTIONAL)
        Response r = new Response();

        try 
        {
            Properties properties = new Properties();
            properties.load(new FileInputStream("db.properties"));
            
            String url = properties.getProperty("url");
            String username = properties.getProperty("username");
            String password = properties.getProperty("password");
            String driver = properties.getProperty("driver");
          
            Connection con = DriverManager.getConnection(url,username,password);
            
            PreparedStatement ps = con.prepareStatement("SELECT SUM(Units_Sold) as sum FROM Sales_Data;");
            ResultSet rs1_sum = ps.executeQuery();
            
            LinkedList<String> ll1 = new LinkedList<String>();
            rs1_sum.next();
            logger.log("Sum(Units_Sold)=" + rs1_sum.getString("sum"));
            ll1.add(rs1_sum.getString("sum"));
            rs1_sum.close();
            
            ps = con.prepareStatement("SELECT COUNT(Units_Sold) as count FROM Sales_Data;");
            ResultSet rs2_count = ps.executeQuery();
            
            LinkedList<String> ll2 = new LinkedList<String>();
            rs2_count.next();
            logger.log("Count(Units_Sold)=" + rs2_count.getString("count"));
            ll2.add(rs2_count.getString("count"));
            rs2_count.close();
            
            ps = con.prepareStatement("SELECT AVG(Units_Sold) as Average FROM Sales_Data;");
            ResultSet rs3_avg = ps.executeQuery();
            
            LinkedList<String> ll3 = new LinkedList<String>();
            rs3_avg.next();
            logger.log("Average(Units_Sold)=" + rs3_avg.getString("Average"));
            ll3.add(rs3_avg.getString("Average"));
            rs3_avg.close();
            
            ps = con.prepareStatement("select MAX(Units_Sold) as Max from Sales_Data;");
            ResultSet rs4_max = ps.executeQuery();
            
            LinkedList<String> ll4 = new LinkedList<String>();
            rs4_max.next();
            logger.log("Maximum(Units_Sold)=" + rs4_max.getString("Max"));
            ll4.add(rs4_max.getString("Max"));
            rs4_max.close();
            
            ps = con.prepareStatement("select MIN(Units_Sold) as Min from Sales_Data;");
            ResultSet rs5_min = ps.executeQuery();
            
            LinkedList<String> ll5 = new LinkedList<String>();
            rs5_min.next();
            logger.log("Minimum(Units_Sold)=" + rs5_min.getString("Min"));
            ll5.add(rs5_min.getString("Min"));
            rs5_min.close();
            
            ps = con.prepareStatement("select Country as Country from Sales_Data where Gross_Margin between 0.0 and 0.5;");
            ResultSet rs6 = ps.executeQuery();
            
            LinkedList<String> ll6 = new LinkedList<String>();
            while (rs6.next())
            {
            logger.log("Output_with_where_clause=" + rs6.getString("Country"));
            ll6.add(rs6.getString("Country"));
            }
            rs6.close();
            
            ps = con.prepareStatement("select Region, count(Units_Sold) as SoldUnits from Sales_Data group by Region;");
            ResultSet rs7 = ps.executeQuery();
            
            LinkedList<String> ll7 = new LinkedList<String>();
            while(rs7.next())
            {
            logger.log("Groupby_Query1=" + rs7.getString("SoldUnits"));
            ll7.add(rs7.getString("SoldUnits"));
            }
            rs7.close();
            
            ps = con.prepareStatement("select Item_type, avg(Units_Sold) as AvgUnits from Sales_Data group by Region;");
            ResultSet rs8 = ps.executeQuery();
            
            LinkedList<String> ll8 = new LinkedList<String>();
            while(rs8.next())
            {
            logger.log("Groupby_Query2=" + rs8.getString("AvgUnits"));
            ll8.add(rs8.getString("AvgUnits"));
            }
            rs8.close();
            
            ps = con.prepareStatement("select sum(Units_Sold) as Totalsold from Sales_Data where Region = 'Europe' and Sales_channel = 'Offline' and Order_Priority ='Low' Group By ITEM_TYPE;");
            ResultSet rs9 = ps.executeQuery();
            
            LinkedList<String> ll9 = new LinkedList<String>();
            while (rs9.next()) {
                logger.log("Sum Unit Sold for Region: Europe, Sales_channel = 'Offline',Order_Priority ='Low' is " + rs9.getString("Totalsold"));
                ll9.add(rs9.getString("Totalsold"));
            }
            rs9.close();
            
                      
            ps = con.prepareStatement("select Region, sum(Total_Profit) as Profits from Sales_Data group by Region;");
            ResultSet query3 = ps.executeQuery();
            
            LinkedList<String> ll10 = new LinkedList<String>();
            while(query3.next())
            {
                logger.log("Query3=" + query3.getString("Profits"));
                ll10.add(query3.getString("Profits"));
            }
            query3.close();
            
            ps.close();           
            con.close();
            
            r.setSum(ll1);
            r.setCount(ll2);
            r.setAverage(ll3);
            r.setMaximum(ll4);
            r.setMinimum(ll5);
            r.setWhere(ll6);
            r.setGroupby(ll7);
            r.setGroupby(ll8);
            r.setGroupby(ll9);
            r.setGroupby(ll10);            
        } 
        catch (Exception e) 
        {
            logger.log("Got an exception working with MySQL! ");
            logger.log(e.getMessage());
        }

        inspector.consumeResponse(r);
        
        //****************END FUNCTION IMPLEMENTATION***************************
        
        //Collect final information such as total runtime and cpu deltas.
        inspector.inspectAllDeltas();
        return inspector.finish();
    }

    // int main enables testing function from cmd line
    public static void main (String[] args)
    {
        Context c = new Context() {
            @Override
            public String getAwsRequestId() {
                return "";
            }

            @Override
            public String getLogGroupName() {
                return "";
            }

            @Override
            public String getLogStreamName() {
                return "";
            }

            @Override
            public String getFunctionName() {
                return "";
            }

            @Override
            public String getFunctionVersion() {
                return "";
            }

            @Override
            public String getInvokedFunctionArn() {
                return "";
            }

            @Override
            public CognitoIdentity getIdentity() {
                return null;
            }

            @Override
            public ClientContext getClientContext() {
                return null;
            }

            @Override
            public int getRemainingTimeInMillis() {
                return 0;
            }

            @Override
            public int getMemoryLimitInMB() {
                return 0;
            }

            @Override
            public LambdaLogger getLogger() {
                return new LambdaLogger() {
                    @Override
                    public void log(String string) {
                        System.out.println("LOG:" + string);
                    }
                };
            }
        };

        // Create an instance of the class
        S3Query_RDS lt = new S3Query_RDS();

        // Create a request object
        Request req = new Request();


        // Test properties file creation
        Properties properties = new Properties();
        properties.setProperty("driver", "com.mysql.cj.jdbc.Driver");
        properties.setProperty("url","");
        properties.setProperty("username","");
        properties.setProperty("password","");
        try
        {
          properties.store(new FileOutputStream("test.properties"),"");
        }
        catch (IOException ioe)
        {
          System.out.println("error creating properties file.")   ;
        }

        // Run the function
        //Response resp = lt.handleRequest(req, c);
        System.out.println("The MySQL Serverless can't be called directly without running on the same VPC as the RDS cluster.");
        Response resp = new Response();

        // Print out function result
        System.out.println("function result:" + resp.toString());
    }

}
