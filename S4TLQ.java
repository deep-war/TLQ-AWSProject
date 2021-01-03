package lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import saaf.Inspector;
import saaf.Response;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * uwt.lambda_test::handleRequest
 *
 * @author Deepthi Warrier Edakunni
 * @date 14-Dec-2020
 * S4TLQ Implements the SwitchBoard Architecture.
 * Based on the service parameter pass in as input to the handleRequest method,
 * The code performs the corresponding services - T, L or Q.
 * Case1: Transform
 * Case2: Load
 * Case3: Query
 */
public class S4TLQ implements RequestHandler<Request, HashMap<String, Object>> {
    
    String bucketname = "";
    String filename = "";
    int service = 0;
    int numOfRecords = 0;
    
    //Create and populate a separate response object for function output. (OPTIONAL)
    Response r = new Response();
    
    public static final String TRANSFORMED_FILENAME = "TRANSFORMED_CSV.csv";
    public static final String LOGGER_CLASSNAME = "S4TLQ";
    public static final int ORDER_ID_IDX = 6;
    public static final int PROFIT_IDX = 13;
    public static final int REVENUE_IDX = 11;
    public static final int PRIORITY_IDX = 4;
    public static final int ORDERDATE_IDX = 5;
    public static final int SHIPDATE_IDX = 7;
    

    /**
     * Lambda Function Handler
     * 
     * @param request Request POJO with defined variables from Request.java
     * @param context 
     * @return HashMap that Lambda will automatically convert into JSON.
     */
    public HashMap<String, Object> handleRequest(Request request, Context context) {   
        
        //Collect inital data.
        Inspector inspector = new Inspector();        
        inspector.inspectAll();
        
        //****************START FUNCTION IMPLEMENTATION*************************   
        service = request.getService();
        
        switch (service) {
            case 1:
                bucketname = request.getBucketname();
                filename = request.getFilename();
                Transform(bucketname, filename);
                break;
            case 2:
                bucketname = request.getBucketname();
                filename = request.getFilename();
                Load(bucketname, filename);
                break;
            case 3:
                Query();  
                inspector.consumeResponse(r);
                break;            
            default:
                break;
        } 
                
        //****************END FUNCTION IMPLEMENTATION***************************
                
        //Collect final information such as total runtime and cpu deltas.
        inspector.inspectAllDeltas();
        return inspector.finish();
    }
    
    // Case1: Transform - Reads input CSV file from S3 and does 4 transformations
    // The transformed data is then written back to S3 as a new file TRANSFORMED_CSV.csv
    public void Transform(String bucketname, String filename){
        Logger.getLogger(LOGGER_CLASSNAME+ ": In Transform function");
        AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();
        //get object file using source bucket and srcKey name
        S3Object s3Object = s3Client.getObject(new GetObjectRequest(bucketname, filename));
        //get content of the file
        InputStream objectData = s3Object.getObjectContent();
        
        try {  
            //Do the transformations on the csv file and write as a new file to S3
            transformCSVData(objectData);
        } catch (IOException ex) {
            Logger.getLogger(S4TLQ.class.getName()).log(Level.SEVERE, null, ex);
        } 
    }
    
    //Case2: Loads the Transformed data to the database
    public void Load(String bucketname, String filename){
        Logger.getLogger(LOGGER_CLASSNAME+ ": In Load function");
        try 
        {
            Properties properties = new Properties();
            properties.load(new FileInputStream("db.properties"));
            
            String url = properties.getProperty("url");
            String username = properties.getProperty("username");
            String password = properties.getProperty("password");
            String driver = properties.getProperty("driver");
            
            Connection con = DriverManager.getConnection(url,username,password);
            
            // Detect if the table 'Sales_Data' exists in the database
            PreparedStatement ps = con.prepareStatement("SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'SALESDB' AND TABLE_NAME = 'Sales_Data'");            
            ResultSet rs = ps.executeQuery();
            if (!rs.next()) {
                Logger.getLogger("Create table 'Sales_Data'");
                ps = con.prepareStatement("CREATE TABLE Sales_Data ("
                        + " Region  VARCHAR(255),"
                        + " Country VARCHAR(255),"
                        + " Item_Type VARCHAR(255),"
                        + " Sales_Channel VARCHAR(255),"
                        + " Order_Priority VARCHAR(255),"
                        + " Order_Date DATE,"
                        + " Order_ID INT NOT NULL,"
                        + " Ship_Date DATE,"
                        + " Units_Sold INT,"
                        + " Unit_Price DOUBLE,"
                        + " Unit_Cost DOUBLE,"
                        + " Total_Revenue DOUBLE,"
                        + " Total_Cost DOUBLE,"
                        + " Total_Profit DOUBLE,"
                        + " Gross_Margin DOUBLE,"
                        + " Order_Processing_Time INT,"
                        + " PRIMARY KEY(Order_ID));");
                ps.execute();
            }
            rs.close();
            
            // Delete all rows from table - "Sales_Data" before inserting new rows.
            ps = con.prepareStatement("Delete from Sales_Data;");
            ps.execute(); 
            
            // Reading the csv file from the S3 Bucket
            AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();
            //get object file using source bucket and srcKey name
            S3Object s3Object = s3Client.getObject(new GetObjectRequest(bucketname, filename));
                        
            //get content of the file
            InputStream objectData = s3Object.getObjectContent();
            Scanner scanner = new Scanner(objectData);
            
            boolean skip=true; 
            String record = "";
            String[] values;
            String sqlQuery = "INSERT INTO Sales_Data VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
            
            while (scanner.hasNext()) {
                if(skip) {
                    skip = false; // Skip the first line - headers
                    record = scanner.nextLine();
                    continue;
                }
                
                record = scanner.nextLine();
                values = record.split(",");               
                
                ps = con.prepareStatement(sqlQuery);
                // Example Insert - Australia and Oceania,Tuvalu,Baby Food,Offline,High,
                //5/28/2010,669165933,6/27/2010,9925,255.28,159.42,
                //2533654,1582243.5,951410.5,0.38,30
                ps.setString(1, values[0]);
                ps.setString(2, values[1]);
                ps.setString(3, values[2]);
                ps.setString(4, values[3]);
                ps.setString(5, values[4]);
                ps.setDate(6, getDate(values[5]));
                ps.setInt(7, getInt(values[6]));
                ps.setDate(8, getDate(values[7]));
                ps.setInt(9, getInt(values[8]));
                ps.setDouble(10, getDouble(values[9]));
                ps.setDouble(11, getDouble(values[10]));
                ps.setDouble(12, getDouble(values[11]));
                ps.setDouble(13, getDouble(values[12]));
                ps.setDouble(14, getDouble(values[13]));
                ps.setDouble(15, getDouble(values[14]));
                ps.setInt(16, getInt(values[15]));
                
                ps.executeUpdate();
                ps.close();			          
            }
            scanner.close(); 
            
            con.close();                       
        } catch (SQLException sqlex) {
            Logger.getLogger("SQL Exception:" + sqlex.toString());
            Logger.getLogger(sqlex.getMessage());
        }catch (Exception ex) {
            Logger.getLogger("Got an exception working with MySQL!" + ex.toString());
            Logger.getLogger(ex.getMessage());
        }        
    }
    
    // Case 3: Queries the database
    public void Query(){
        Logger.getLogger(LOGGER_CLASSNAME+ ": In Query function");
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
            Logger.getLogger("Sum(Units_Sold)=" + rs1_sum.getString("sum"));
            ll1.add(rs1_sum.getString("sum"));
            rs1_sum.close();
            
            ps = con.prepareStatement("SELECT COUNT(Units_Sold) as count FROM Sales_Data;");
            ResultSet rs2_count = ps.executeQuery();
            
            LinkedList<String> ll2 = new LinkedList<String>();
            rs2_count.next();
            Logger.getLogger("Count(Units_Sold)=" + rs2_count.getString("count"));
            ll2.add(rs2_count.getString("count"));
            rs2_count.close();
            
            ps = con.prepareStatement("SELECT AVG(Units_Sold) as Average FROM Sales_Data;");
            ResultSet rs3_avg = ps.executeQuery();
            
            LinkedList<String> ll3 = new LinkedList<String>();
            rs3_avg.next();
            Logger.getLogger("Average(Units_Sold)=" + rs3_avg.getString("Average"));
            ll3.add(rs3_avg.getString("Average"));
            rs3_avg.close();
            
            ps = con.prepareStatement("select MAX(Units_Sold) as Max from Sales_Data;");
            ResultSet rs4_max = ps.executeQuery();
            
            LinkedList<String> ll4 = new LinkedList<String>();
            rs4_max.next();
            Logger.getLogger("Maximum(Units_Sold)=" + rs4_max.getString("Max"));
            ll4.add(rs4_max.getString("Max"));
            rs4_max.close();
            
            ps = con.prepareStatement("select MIN(Units_Sold) as Min from Sales_Data;");
            ResultSet rs5_min = ps.executeQuery();
            
            LinkedList<String> ll5 = new LinkedList<String>();
            rs5_min.next();
            Logger.getLogger("Minimum(Units_Sold)=" + rs5_min.getString("Min"));
            ll5.add(rs5_min.getString("Min"));
            rs5_min.close();
            
            ps = con.prepareStatement("select Country as Country from Sales_Data where Gross_Margin between 0.0 and 0.5;");
            ResultSet rs6 = ps.executeQuery();
            
            LinkedList<String> ll6 = new LinkedList<String>();
            while (rs6.next())
            {
            Logger.getLogger("Output_with_where_clause=" + rs6.getString("Country"));
            ll6.add(rs6.getString("Country"));
            }
            rs6.close();
            
            ps = con.prepareStatement("select Region, count(Units_Sold) as SoldUnits from Sales_Data group by Region;");
            ResultSet rs7 = ps.executeQuery();
            
            LinkedList<String> ll7 = new LinkedList<String>();
            while(rs7.next())
            {
            Logger.getLogger("Groupby_Query1=" + rs7.getString("SoldUnits"));
            ll7.add(rs7.getString("SoldUnits"));
            }
            rs7.close();
            
            ps = con.prepareStatement("select Item_type, avg(Units_Sold) as AvgUnits from Sales_Data group by Region;");
            ResultSet rs8 = ps.executeQuery();
            
            LinkedList<String> ll8 = new LinkedList<String>();
            while(rs8.next())
            {
            Logger.getLogger("Groupby_Query2=" + rs8.getString("AvgUnits"));
            ll8.add(rs8.getString("AvgUnits"));
            }
            rs8.close();
            
            ps = con.prepareStatement("select sum(Units_Sold) as Totalsold from Sales_Data where Region = 'Europe' and Sales_channel = 'Offline' and Order_Priority ='Low' Group By ITEM_TYPE;");
            ResultSet rs9 = ps.executeQuery();
            
            LinkedList<String> ll9 = new LinkedList<String>();
            while (rs9.next()) {
                Logger.getLogger("Sum Unit Sold for Region: Europe, Sales_channel = 'Offline',Order_Priority ='Low' is " + rs9.getString("Totalsold"));
                ll9.add(rs9.getString("Totalsold"));
            }
            rs9.close();
            
                      
            ps = con.prepareStatement("select Region, sum(Total_Profit) as Profits from Sales_Data group by Region;");
            ResultSet query3 = ps.executeQuery();
            
            LinkedList<String> ll10 = new LinkedList<String>();
            while(query3.next())
            {
                Logger.getLogger("Query3=" + query3.getString("Profits"));
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
            Logger.getLogger("Got an exception working with MySQL! ");
            Logger.getLogger(e.getMessage());
        }             
    }
    
    private Integer getInt(String integer) {
        return Integer.parseInt(integer);
    }

    private Double getDouble(String doubleVal) {
        return Double.parseDouble(doubleVal);
    }

    private java.sql.Date getDate(String date) throws Exception {
        SimpleDateFormat formatter = new SimpleDateFormat("MM/dd/yy");
        return new java.sql.Date(formatter.parse(date).getTime());
    }
    
    // Internally called from Transform method
    public void transformCSVData(InputStream ipStream) throws IOException{
        BufferedReader csvReader=null;
        final String lineSep=",";
        
        //ArrayList to hold each row of the csv file
        List<ArrayList<String>> csvRows = new ArrayList<>();
        
        csvReader = new BufferedReader(new InputStreamReader(ipStream));
        String row = null;
        
        while ((row = csvReader.readLine()) != null) {
            String[] data = row.split(lineSep);
            
            ArrayList<String> rowList = new ArrayList<>();
            
            //Add the each row of data to an Arraylist
            Collections.addAll(rowList, data);
            csvRows.add(rowList);            
        }
        csvReader.close();
        
        // Do the 4 transformations on the original data
        csvRows = transformData(csvRows);     
        
        //Write the Transfomred file to S3
        writeCSVToS3(csvRows);
    }  
        
    /*Do the transformations on the data read from csv
        Transformation1: Remove Duplicate Entries
        Transformation2: Change Order priority from 'L' to "Low", 'M' to "Medium etc
        Transformation3: Add a new column Gross Margin
        Transformation4: Add column [Order Processing Time]
    */
    public List<ArrayList<String>> transformData(List<ArrayList<String>> csvRows){
        // Transformation1 - Remove Duplicate Entries
        csvRows = removeDuplicates(csvRows);           
        
        // Transformation 2 - Change Order priority from 'L' to "Low", 'M' to "Medium etc
        csvRows = changePriority(csvRows);    
        
        // Transformation3 - Add a new column Gross Margin
        csvRows = addGrossMargin(csvRows);
        
        // Transformation4 - Add Order Processing Time        
        csvRows = addOrderProcTime(csvRows);
        
        return csvRows;
    }
    
    /* Transformation4: Add order Processing Time
    The Order Processing Time Column stores an integer value representing the number of days between the [Order Date] and [Ship Date]*/
    public List<ArrayList<String>> addOrderProcTime(List<ArrayList<String>> csvRows){
        SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy");

        Date orderDate = null;
        Date shipDate = null;

        for (int i= 0; i < csvRows.size(); i++){
            if(i==0){
                //Adding a header in the first row
                csvRows.get(i).add("Order Processing Time") ;             
            }
            else{
                try {
                    //Get the orderdate and ship date
                    orderDate = sdf.parse(csvRows.get(i).get(ORDERDATE_IDX));
                    shipDate = sdf.parse(csvRows.get(i).get(SHIPDATE_IDX));
                } catch (ParseException ex) {
                    Logger.getLogger(S4TLQ.class.getName()).log(Level.SEVERE, null, ex);
                }                
                
                //Calculate the number of days between order date and ship date
                long timeDifferenceInMilliSeconds = shipDate.getTime() - orderDate.getTime();
                long numOfDays = TimeUnit.MILLISECONDS.toDays(timeDifferenceInMilliSeconds);
                
                csvRows.get(i).add(String.valueOf(numOfDays));                
            }
        }        
        // return the Updated list
        return csvRows;
    }
    
    /* Transformation3: Add a [Gross Margin] column
    The Gross Margin Column is a percentage calculated using the formula: [Total Profit] / [Total Revenue]. 
    It is stored as a floating point value*/
    public List<ArrayList<String>> addGrossMargin(List<ArrayList<String>> csvRows){
        for (int i= 0; i < csvRows.size(); i++){
            if(i==0){
                //Adding a header in the first row
                csvRows.get(i).add("Gross Margin") ;             
            }
            else{
                //Get the revenue & Profit
                String revenue = csvRows.get(i).get(REVENUE_IDX);
                String profit = csvRows.get(i).get(PROFIT_IDX);
                
                //Calculate margin
                float margin = Float.parseFloat(profit) / Float.parseFloat(revenue);
                //Add the margin to the csv file
                csvRows.get(i).add(String.format("%.2f", margin));                
            }
        }        
        // return the Updated list
        return csvRows;
    }
    
    /*
        Transformation2: S1Transform [Order Priority] column:
        L to “Low”, M to “Medium”, H to “High”, C to “Critical”
    */
    public List<ArrayList<String>> changePriority(List<ArrayList<String>> csvRows){
        
        // Iterate through list - passed in csvRows
        csvRows.forEach((ArrayList<String> iterator) -> {
            //Get the priority
            String priority = iterator.get(PRIORITY_IDX);
            
            switch (priority) {
                case "L":
                    iterator.set(PRIORITY_IDX, "Low");
                    break;
                case "M":
                    iterator.set(PRIORITY_IDX, "Medium");
                    break;
                case "H":
                    iterator.set(PRIORITY_IDX, "High");
                    break;
                case "C":
                    iterator.set(PRIORITY_IDX, "Critical");
                    break;
                default:
                    break;
            }
        });
        // return the Updated list
        return csvRows;
    }
    
    /*
        Transformation1: Remove duplicate data identified by [Order ID]
    */
    public List<ArrayList<String>> removeDuplicates(List<ArrayList<String>> csvRows){
        // Create a new ArrayList for saving the duplicate removed list
        List<ArrayList<String>> updatedList = new ArrayList<>();

        // Iterate through the first list - passed in csvRows
        csvRows.forEach((iterator) -> {
            //Get the order Id
            String orderId = iterator.get(ORDER_ID_IDX);
            //Flag to see if the orderId already exists
            boolean isDuplicate = false;
            //Iterate through the newly created list to see if the same orderid is present
            for (List<String> newIterator : updatedList) {
                String newOrderId = newIterator.get(ORDER_ID_IDX);
                if(orderId.equals(newOrderId)) {
                    isDuplicate = true;
                }
            }
            // If the orderId doesnt exist in the newly created list,add the entire row to the new list
            if (!isDuplicate) {
                updatedList.add(iterator);
            }
        });
        // return the Updated list
        return updatedList;
    }
    
    /*
        Method to write the transformed data back to S3 as a new file TRANSFORMED_CSV.csv
    */    
    public void writeCSVToS3(List<ArrayList<String>> csvRows){
        
        //Creating values for csv file        
        StringWriter sw = new StringWriter();
  
        for(List<String> row: csvRows) {
            int i = 0;
            for (String value: row) {
                sw.append(value);
                if(i++ != row.size() - 1)
                    sw.append(',');
            }
            sw.append("\n");            
        }
                 
        byte[] bytes = sw.toString().getBytes(StandardCharsets.UTF_8);
        InputStream is = new ByteArrayInputStream(bytes);
        ObjectMetadata meta = new ObjectMetadata();
        meta.setContentLength(bytes.length);
        meta.setContentType("text/plain");
        
        // Create new file on S3
        AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();
        s3Client.putObject(bucketname, TRANSFORMED_FILENAME, is, meta);
    }
}
