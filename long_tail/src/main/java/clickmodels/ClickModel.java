package clickmodels;

import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public interface ClickModel {
    public void map(Mapper.Context context, SERPLog log) throws IOException, InterruptedException;
}

