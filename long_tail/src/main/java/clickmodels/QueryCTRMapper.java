package clickmodels;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class QueryCTRMapper {
    private final String DELIMETER = "::::";
    private final String MARKER = "QUERYCTR" + DELIMETER;

    public QueryCTRMapper(){}
    @SuppressWarnings("unchecked")
    public void map(Mapper.Context context, SERPLog log) throws IOException, InterruptedException {
        StringBuilder sb = new StringBuilder();
        sb.append((double)log.clickedPositions.length).append(DELIMETER);
        sb.append((double)log.clickedPositions.length/log.shownLinks.length).append(DELIMETER);
        sb.append((double)(log.clickedPositions[0])).append(DELIMETER);
        sb.append((double)(log.clickedPositions[log.clickedPositions.length -1]));
        context.write(new Text(MARKER + log.query), new Text(sb.toString()));
    }
}
