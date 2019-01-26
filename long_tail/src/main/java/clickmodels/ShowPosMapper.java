package clickmodels;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;

public class ShowPosMapper {
    private final String DELIMETER = "::::";
    private final String MARKER = "SHOWPOS" + DELIMETER;

    public ShowPosMapper() {
    }

    @SuppressWarnings("unchecked")
    public void map(Mapper.Context context, SERPLog log) throws IOException, InterruptedException {
        for (int i = 0; i < log.shownLinks.length; i++){
            if (log.markedLinks[i]) {
                String link = log.shownLinks[i];
                context.write(new Text(MARKER + log.query + DELIMETER + link), new Text(Double.toString((double) i)));
            }
        }
    }
}
