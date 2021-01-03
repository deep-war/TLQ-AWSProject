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
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import saaf.Inspector;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * uwt.lambda_test::handleRequest
 *
 * @author Deepthi Warrier Edakunni
 * @date 4-Dec-2020
 * S1Transform - accesses the CSV file from S3 and does 4 transformations on the input raw data.
 * This transformed data is then written back to S3 as a new file TRANSFORMED_CSV.csv
 */
public class S1Transform implements RequestHandler<Request, HashMap<String, Object>> {
    
    String bucketname = "";
    String filename = "";
    public static final String TRANSFORMED_FILENAME = "TRANSFORMED_CSV.csv";
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
        bucketname = request.getBucketname();
        filename = request.getFilename();
        
        AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();
        //get object file using source bucket and srcKey name
        S3Object s3Object = s3Client.getObject(new GetObjectRequest(bucketname, filename));
        //get content of the file
        InputStream objectData = s3Object.getObjectContent();
        
        try {  
            //Do the transformations on the csv file and write as a new file to S3
            transformCSVData(objectData);
        } catch (IOException ex) {
            Logger.getLogger(S1Transform.class.getName()).log(Level.SEVERE, null, ex);
        }         
                
        //****************END FUNCTION IMPLEMENTATION***************************
                
        //Collect final information such as total runtime and cpu deltas.
        inspector.inspectAllDeltas();
        return inspector.finish();
    }
    
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
        
        //Write the Transformed file to S3
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
                    Logger.getLogger(S1Transform.class.getName()).log(Level.SEVERE, null, ex);
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
