import org.apache.hadoop.io.Text;
import java.io.IOException;
import java.util.ArrayList;

public class PageRankNode{
    double weightOut; // Hubness для HITS
    double weightIn; // Authority для HITS
    ArrayList<String> linksOut = new ArrayList<>();
    ArrayList<String> linksIn = new ArrayList<>();

    public static PageRankNode read(String in) throws IOException {
        PageRankNode out = new PageRankNode();
        String[] data = in.split(" ");
        out.weightOut = Double.parseDouble(data[0]);
        if (data.length > 1) {
            for (int i = 1; i < data.length; i++) {
                out.linksOut.add(data[i]);
            }
        }
        return out;
    }

    public static PageRankNode readHITS(String in) throws IOException {
        PageRankNode out = new PageRankNode();
        String splitter = "<SPLITTER>";
        String[] data = in.split(splitter);
        assert (data.length == 4);
        try {
            out.weightOut = Double.parseDouble(data[0]);
            out.weightIn = Double.parseDouble(data[1]);
        }catch(Exception ex){
            System.out.println("Full string:[" + in + "]");
            for (int i = 0; i < data.length; i++){
                System.out.println("data" + Integer.toString(i) + ":[" + data[i] + "]");
            }
        }
        String[] linksInArray;
        String[] linksOutArray;
        if (data[2].equals(" ")){
            linksOutArray = new String[0];
        }else{
            linksOutArray = data[2].split(" ");
        }
        try {
            if (data[3].equals(" ")) {
                linksInArray = new String[0];
            } else {
                linksInArray = data[3].split(" ");
            }
        }catch (Exception ex){
            linksInArray = new String[0];
            System.out.println("Full string:" + in);
            for (int i = 0; i < data.length; i++){
                System.out.println("data" + Integer.toString(i) + ":" + data[i]);
            }
        }
        for (int i = 0; i < linksInArray.length; i++) {
            if (!linksInArray[i].equals(" ") & !linksInArray[i].equals("")) {
                out.linksIn.add(linksInArray[i]);
            }
        }
        for (int i = 0; i < linksOutArray.length; i++) {
            if (!linksOutArray[i].equals(" ") & !linksOutArray[i].equals("")) {
                out.linksOut.add(linksOutArray[i]);
            }
        }
        return out;
    }

    public Text toHITS() throws IOException{
        StringBuilder outBuilder = new StringBuilder();
        String splitter = "<SPLITTER>";
        outBuilder.append(Double.toString(weightOut)).append(splitter).append(Double.toString(weightIn)).append(splitter);
        StringBuilder linksOutBuilder = new StringBuilder();
        StringBuilder linksInBuilder = new StringBuilder();
        for (String link : linksOut) {
            linksOutBuilder.append(link).append(" ");
        }
        for (String link : linksIn) {
            linksInBuilder.append(link).append(" ");
        }
        String linksInStr = linksInBuilder.toString().trim();
        String linksOutStr = linksOutBuilder.toString().trim();
        if (linksInStr.equals("")){
            linksInStr = " ";
        }
        if (linksOutStr.equals("")){
            linksOutStr = " ";
        }
        outBuilder.append(linksOutStr).append(splitter).append(linksInStr);
        return new Text(outBuilder.toString());
    }

    

    public Text toText() throws IOException{
        String out = "";
        out += Double.toString(weightOut);
        for (String link : linksOut) {
            out += " " + link;
        }
        return new Text(out);
    }
}
