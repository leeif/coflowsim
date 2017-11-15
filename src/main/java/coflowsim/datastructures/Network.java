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
	private ArrayList<String> edge;
	private ArrayList<String> aggregation;
	private ArrayList<String> core;
	private ArrayList<Link> links;
	private Map<Link, Integer> residualCapacity;
	private Map<ODPair, ArrayList<Link>> paths;
	private Map<Link, Integer> linkBandwidth;


	/**
	 * @param k_ary
	 */
	public Network(int k_ary, int bandwidth) {
		this.k_ary = k_ary;
		this.total_bandwidth = bandwidth;
		this.createTopology();
		this.initBandWidth();
	}


	public void createTopology() {

		servers = new ArrayList<String>();
		edge = new ArrayList<String>();
		aggregation = new ArrayList<String>();
		core = new ArrayList<String>();
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

		System.out.println(servers.size());

		// create links
		for (int i = 0; i < servers.size(); i++) {
			Link link1 = new Link(servers.get(i), edge.get(i / 2));
			Link link2 = link1.getReverseLink();
			links.add(link1);
			links.add(link2);
		}

		for (int i = 0; i < aggregation.size(); i += (k_ary / 2)) {
			for (int j = i; j < i + (k_ary / 2); j++) {
				Link link1 = new Link(aggregation.get(i), edge.get(j));
				Link link2 = link1.getReverseLink();
				links.add(link1);
				links.add(link2);
			}
			for (int j = i; j < i + (k_ary / 2); j++) {
				Link link1 = new Link(aggregation.get(i + 1), edge.get(j));
				Link link2 = link1.getReverseLink();
				links.add(link1);
				links.add(link2);
			}
		}

		for (int i = 0; i < (k_ary / 2) * (k_ary / 2); i++) {
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
		for(int i = 0; i < links.size(); i++) {
			linkBandwidth.put(links.get(i), total_bandwidth);
		}
	}

	public void createPaths(ODPair o) {

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
