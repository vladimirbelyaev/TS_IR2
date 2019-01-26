package clickmodels;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class RankCTRPosMapper {
    private final String DELIMETER = "::::";
    private final String MARKER = "RANKCTR" + DELIMETER;

    public RankCTRPosMapper(){}
    @SuppressWarnings("unchecked")
    public void map(Mapper.Context context, SERPLog log) throws IOException, InterruptedException {
        for (int i=0; i < log.shownLinks.length; i++){
            if (log.markedLinks[i]) {
                String link = log.shownLinks[i];
                context.write(new Text(MARKER + link), new Text(Integer.toString(i) + DELIMETER + Double.toString((double) 0)));
            }
        }
        for (int pos: log.clickedPositions){
            if (log.markedLinks[pos]){
                String link = log.shownLinks[pos];
                context.write(new Text(MARKER + link), new Text(Integer.toString(pos) + DELIMETER + Double.toString((double)1)));
            }
        }
    }
}
