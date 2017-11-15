package coflowsim.datastructures;


/**
 * Created by lee on 2017/11/15.
 */
public class ODPair implements Comparable<ODPair> {

    private String source;
    private String destination;


    public ODPair(String source, String destination) {
        this.source = source;
        this.destination = destination;
    }


    public String getSource() {
        return this.source;
    }

    public String getDestination() {
        return this.destination;
    }



    public int compareTo(ODPair o) {
        if (source.equals(o.source) && destination.equals(o.destination)) {
            return 0;
        } else {
            return 1;
        }
    }
}
