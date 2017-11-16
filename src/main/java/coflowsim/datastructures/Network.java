package coflowsim.datastructures;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Network Data Structure used to support network routing in coflow sim
 * In our research, we assume our network in a topology of Fat-tree
 * K-ary fat-tree : three layer topology (edge, aggregation and core)
 * K Pods, (K/2)^2 servers each Pod, each edge switch connects to k/2 servers & k/2 aggregation switch
 * each aggregation switch connects to k/2 edge & k/2 core switches.
 * (k/2)^2 core switches: each connects to k pods.
 */
public class Network {

	private int k_ary;
	private int total_bandwidth;
	private ArrayList<String> servers;
	private ArrayList<ArrayList<String>> pods;
	private ArrayList<String> edge;
	private ArrayList<String> aggregation;
	private ArrayList<String> core;
	private ArrayList<Link> links;
	private ArrayList<ODPair> ods;
	private Map<ODPair, ArrayList<ArrayList<Link>>> paths;
	private Map<Link, Integer> linkBandwidth;


	/**
	 * @param k_ary
	 */
	public Network(int k_ary, int bandwidth) {
		this.k_ary = k_ary;
		this.total_bandwidth = bandwidth;
		this.createTopology();
		this.initBandWidth();
		this.createPaths();
	}


	public void createTopology() {

		servers = new ArrayList<String>();
		edge = new ArrayList<String>();
		aggregation = new ArrayList<String>();
		core = new ArrayList<String>();
		pods = new ArrayList<ArrayList<String>>();
		links = new ArrayList<Link>();

		//create nodes
		for (int i = 0; i < (k_ary / 2) * (k_ary / 2) * k_ary; i++) {
			servers.add("server_" + i);
		}
		for (int i = 0; i < (k_ary / 2) * k_ary; i++) {
			edge.add("edge_" + i);
			aggregation.add("aggregation_" + i);
		}
		for (int i = 0; i < (k_ary / 2) * (k_ary / 2); i++) {
			core.add("core_" + i);
		}

		//create pods
		for (int i = 0; i < k_ary; i++) {
			ArrayList<String> s = new ArrayList<String>();
			for (int j = i; j < i + (k_ary / 2) * (k_ary / 2); j++) {
				s.add(servers.get(j));
			}
			pods.add(s);
		}

		// create links
		for (int i = 0; i < servers.size(); i++) {
			Link link1 = new Link(servers.get(i), edge.get(i / (k_ary / 2)));
			Link link2 = link1.getReverseLink();
			links.add(link1);
			links.add(link2);
		}

		for (int i = 0; i < aggregation.size(); i += (k_ary / 2)) {
			for (int k = 0; k < (k_ary / 2); k++) {
				for (int j = i; j < i + (k_ary / 2); j++) {
					Link link1 = new Link(aggregation.get(i + k), edge.get(j));
					Link link2 = link1.getReverseLink();
					links.add(link1);
					links.add(link2);
				}
			}
		}

		for (int i = 0; i < k_ary; i++) {
			for (int j = 0; j < k_ary / 2; j++) {
				for (int k = 0; k < k_ary / 2; k++) {
					Link link1 = new Link(aggregation.get(j + i * (k_ary / 2)), core.get(k + j * (k_ary / 2)));
					Link link2 = link1.getReverseLink();
					links.add(link1);
					links.add(link2);
				}
			}
		}
	}

	public void initBandWidth() {
		linkBandwidth = new HashMap<Link, Integer>();
		for (int i = 0; i < links.size(); i++) {
			linkBandwidth.put(links.get(i), total_bandwidth);
		}
	}


	//assume that source and destination are in different pod
	public void createPaths() {
		paths = new HashMap<ODPair, ArrayList<ArrayList<Link>>>();
		ods = new ArrayList<ODPair>();
		for (int i = 0; i < pods.size(); i++) {
			for (int j = 0; j < pods.size(); j++) {
				if (i != j) {
					for (int m = 0; m < pods.get(i).size(); m++) {
						for (int n = 0; n < pods.get(j).size(); n++) {
							ODPair o = new ODPair(pods.get(i).get(m),
											pods.get(j).get(n));
							ods.add(o);
							createPaths(o, i, j, m / (k_ary / 2), n / (k_ary / 2));
						}
					}
				}
			}
		}
	}

	private void createPaths(ODPair o, int sourcePod, int desPod,
													 int sourceEdge, int desEdge) {

		ArrayList<ArrayList<Link>> ps = new ArrayList<ArrayList<Link>>();

		for (int i = 0; i < core.size(); i++) {
			ArrayList<Link> p = new ArrayList<Link>();
			String c = core.get(i);
			int aggreUp = i / (k_ary / 2) + sourcePod * (k_ary / 2);
			int aggreDown = i / (k_ary / 2) + desPod * (k_ary / 2);
			int edgeUp = sourcePod * (k_ary / 2) + sourceEdge;
			int edgeDown = desPod * (k_ary / 2) + desEdge;
//			System.out.println(edgeUp + "-" + aggreUp + "-" + aggreDown + "-" + edgeDown);
			Link eu = links.get(links.indexOf(new Link("edge_" + edgeUp, "aggregation_" + aggreUp)));
			Link au = links.get(links.indexOf(new Link("aggregation_" + aggreUp, c)));
			Link ad = links.get(links.indexOf(new Link(c, "aggregation_" + aggreDown)));
			Link ed = links.get(links.indexOf(new Link("aggregation_" + aggreDown, "edge_" + edgeDown)));
			p.add(eu);
			p.add(au);
			p.add(ad);
			p.add(ed);
			ps.add(p);
		}

		paths.put(o, ps);
	}


	public Map<ODPair, ArrayList<ArrayList<Link>>> getPaths() {
		return this.paths;
	}

	public ArrayList<ODPair> getOds() {
		return this.ods;
	}

	public int getBandwidth(Link link) {
		return this.linkBandwidth.get(link);
	}

	public ArrayList<String> getServers() {
		return this.servers;
	}

	public ArrayList<Link> getLinks() {
		return this.links;
	}

}
