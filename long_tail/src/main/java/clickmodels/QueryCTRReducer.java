package clickmodels;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

public class QueryCTRReducer {
    public static final String MARKER = "QUERYCTR";
    @SuppressWarnings("unchecked")
    public static void reduce(String query,
                              Iterable<Text> values,
                              MultipleOutputs<Text, Text> out) throws IOException, InterruptedException {;
        final String DELIMETER = "::::";
        Double meanCTR = 0.0;
        Double meanNormalisedCTR = 0.0;
        Double firstClick = 0.0;
        Double lastClick = 0.0;
        int counter = 0;
        for (Text i : values) {
            String[] vals = i.toString().split(DELIMETER);
            counter += 1;
            meanCTR += Double.parseDouble(vals[0]);
            meanNormalisedCTR += Double.parseDouble(vals[1]);
            firstClick += Double.parseDouble(vals[2]);
            lastClick = Double.parseDouble(vals[3]);
        }
        meanCTR /= counter;
        meanNormalisedCTR /= counter;
        firstClick /= counter;
        lastClick /= counter;
        StringBuilder sb = new StringBuilder();
        sb.append(meanCTR).append("\t").append(meanNormalisedCTR).append("\t");
        sb.append(firstClick).append("\t").append(lastClick);
        out.write("xgb", new Text(query), new Text(sb.toString()), "QueryCTR");

    }
}
