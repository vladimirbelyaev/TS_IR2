import org.apache.hadoop.io.Text;
import java.io.IOException;
import java.util.ArrayList;

public class PageRankNode{
    double weight;
    ArrayList<String> linksOut = new ArrayList<>();

    public static PageRankNode read(String in) throws IOException {
        PageRankNode out = new PageRankNode();
        String[] data = in.split(" ");
        out.weight = Double.parseDouble(data[0]);
        if (data.length > 1) {
            for (int i = 1; i < data.length; i++) {
                out.linksOut.add(data[i]);
            }
        }
        return out;
    }

    public Text toText() throws IOException{
        String out = "";
        out += Double.toString(weight);
        for (String link : linksOut) {
            out += " " + link;
        }
        return new Text(out);
    }
}
