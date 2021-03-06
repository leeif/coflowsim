package coflowsim.datastructures;

import java.awt.image.AreaAveragingScaleFilter;
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

	public int k_ary;
	public Map<Link, Double> linkFreeBandwidth;

	private Double total_bandwidth;
	private ArrayList<String> servers;
	private ArrayList<ArrayList<String>> pods;
	private ArrayList<String> edge;
	private ArrayList<String> aggregation;
	private ArrayList<String> core;
	private ArrayList<Link> links;
	private Map<String, ArrayList<ArrayList<Link>>> paths = new HashMap<String, ArrayList<ArrayList<Link>>>();


	/**
	 * @param k_ary
	 */
	public Network(int k_ary, Double bandwidth) {
		this.k_ary = k_ary;
		this.total_bandwidth = bandwidth;
		this.createTopology();
		this.resetBandWidthForLink();
//		this.createPaths();
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

	public void resetBandWidthForLink() {
		linkFreeBandwidth = new HashMap<Link, Double>();
		for (int i = 0; i < links.size(); i++) {
			linkFreeBandwidth.put(links.get(i), total_bandwidth);
		}
	}


	//assume that source and destination are in different pod
//	public void createPaths() {
//		paths = new HashMap<ODPair, ArrayList<ArrayList<Link>>>();
//		ods = new ArrayList<ODPair>();
//		for (int i = 0; i < pods.size(); i++) {
//			for (int j = 0; j < pods.size(); j++) {
//				if (i != j) {
//					for (int m = 0; m < pods.get(i).size(); m++) {
//						for (int n = 0; n < pods.get(j).size(); n++) {
//							ODPair o = new ODPair(pods.get(i).get(m),
//											pods.get(j).get(n));
//							ods.add(o);
//							createPaths(o, i, j, m / (k_ary / 2), n / (k_ary / 2));
//						}
//					}
//				}
//			}
//		}
//	}

	public ArrayList<ArrayList<Link>> createPaths(int source, int des) {

		ArrayList<ArrayList<Link>> ps = new ArrayList<ArrayList<Link>>();

		String od = source + "-" + des;

		for (int i = 0; i < core.size(); i++) {
			ArrayList<Link> p = new ArrayList<Link>();
			String c = core.get(i);
			int aggreUp = i / (k_ary / 2) + source * (k_ary / 2);
			int aggreDown = i / (k_ary / 2) + des * (k_ary / 2);
//			System.out.println(edgeUp + "-" + aggreUp + "-" + aggreDown + "-" + edgeDown);
			Link au = links.get(links.indexOf(new Link("aggregation_" + aggreUp, c)));
			Link ad = links.get(links.indexOf(new Link(c, "aggregation_" + aggreDown)));
			p.add(au);
			p.add(ad);
			ps.add(p);
		}

		paths.put(od, ps);
		return ps;
	}


	public ArrayList<ArrayList<Link>> getPaths(int source, int des) {
		String od = source + "-" + des;
		if (paths.containsKey(od)) {
			return paths.get(od);
		} else {
			return createPaths(source, des);
		}
	}

	public Map<String, ArrayList<ArrayList<Link>>> getAllPaths() {
		return this.paths;
	}

	public double getPathMaxAvailableBandwidth(ArrayList<Link> path) {
		double min = total_bandwidth;
		for(int i = 0; i < path.size(); i++){
			Link link = path.get(i);
			if(min > linkFreeBandwidth.get(link)) {
				min =  linkFreeBandwidth.get(link);
			}
		}
		return min;
	}

	public void setLinkBandwidth(Link link, double bandwidth) {
		linkFreeBandwidth.put(link, bandwidth);
	}

	public ArrayList<ArrayList<String>> getPods() {
		return this.pods;
	}

	public ArrayList<String> getEdge() {
		return this.edge;
	}

	public ArrayList<String> getCore() {
		return this.core;
	}

	public Double getFreeBandwidth(Link link) {
		return this.linkFreeBandwidth.get(link);
	}

	public ArrayList<String> getServers() {
		return this.servers;
	}

	public ArrayList<Link> getLinks() {
		return this.links;
	}

}
