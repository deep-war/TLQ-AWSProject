package saaf;

import java.util.List;

/**
 * A basic Response object that can be consumed by FaaS Inspector
 * to be used as additional output.
 * 
 * @author Bharti Bansinge
 */
public class Response {
    // Return value
    private List<String> sum;
    private List<String> count;
    private List<String> average;
    private List<String> maximum;
    private List<String> minimum;
    private List<String> where;
    private List<String> groupby;
    
    @Override
    public String toString() {
        return "value=" + super.toString();
    }   

    public List<String> getSum()
    {
        return this.sum;
    }
    public void setSum(List<String> sum)
    {
        this.sum = sum;
    }    
    
    public List<String> getCount()
    {
        return this.count;
    }
    public void setCount(List<String> count)
    {
        this.count = count;
    }
    
    public List<String> getAverage()
    {
        return this.average;
    }
    public void setAverage(List<String> average)
    {
        this.average = average;
    }
    
    public List<String> getMaximum()
    {
        return this.maximum;
    }
     
    public void setMaximum(List<String> maximum)
    {
        this.maximum = maximum;
    }
    
    public List<String> getMinimum()
    {
        return this.minimum;
    }
    
    public void setMinimum(List<String> minimum)
    {
        this.minimum = minimum;
    }      
    
    public List<String> getWhere()
    {
        return this.where;
    }
    
    public void setWhere(List<String> where)
    {
        this.where = where;
    }       
    
    public List<String> getGroupby()
    {
        return this.groupby;
    }
    public void setGroupby(List<String> groupby)
    {
        this.groupby = groupby;
    }    
}





