package lambda;

/**
 *
 * @author Wes Lloyd
 * Deepthi - Edited for including service as part of switch board architecture
 */
public class Request {

    String bucketname;
    String filename;
    //For Switch Board Architecture
    int service;

    public int getService() {
        return service;
    }

    public void setService(int service) {
        this.service = service;
    }    

    public String getBucketname() {
        return bucketname;
    }

    public String getFilename() {
        return filename;
    }   
    
     public void setBucketname(String bucketname) {
        this.bucketname = bucketname;
    }

    public void setFilename(String filename) {
        this.filename = filename;
    }    

    public Request() {

    }
}
