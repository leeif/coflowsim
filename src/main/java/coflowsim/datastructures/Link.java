package coflowsim.datastructures;


/**
 * Created by lee on 2017/11/15.
 */
public class Link implements Comparable<Link> {

    private String start;
    private String end;

    public Link(String start, String end) {
        this.start = start;
        this.end = end;
    }

    public String getStart() {
        return this.start;
    }

    public String getEnd() {
        return this.end;
    }

    public int compareTo(Link o) {
        if (this.getStart().equals(o.getStart()) &&
                this.getEnd().equals(o.getEnd())) {
            return 0;
        } else {
            return 1;
        }
    }

    public Link getReverseLink() {
        return new Link(this.end, this.start);
    }

    @Override
    public String toString() {
        return "Link: [" + start + "---->" + end + "]";
    }
}
