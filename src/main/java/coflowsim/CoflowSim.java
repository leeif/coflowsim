package coflowsim;

import coflowsim.datastructures.*;
import coflowsim.simulators.*;
import coflowsim.traceproducers.CoflowBenchmarkTraceProducer;
import coflowsim.traceproducers.CustomTraceProducer;
import coflowsim.traceproducers.JobClassDescription;
import coflowsim.traceproducers.TraceProducer;
import coflowsim.utils.Constants;
import coflowsim.utils.Constants.SHARING_ALGO;
import coflowsim.utils.Constants.ROUTING_ALGO;

public class CoflowSim {

	public static void main(String[] args) {
		int curArg = 0;

		SHARING_ALGO sharingAlgo = SHARING_ALGO.FAIR;
		ROUTING_ALGO routingAlgo = ROUTING_ALGO.HEDERA;
		if (args.length > curArg) {
			String UPPER_ARG = args[curArg++].toUpperCase();

			if (UPPER_ARG.contains("FAIR")) {
				sharingAlgo = SHARING_ALGO.FAIR;
			} else if (UPPER_ARG.contains("PFP")) {
				sharingAlgo = SHARING_ALGO.PFP;
			} else if (UPPER_ARG.contains("FIFO")) {
				sharingAlgo = SHARING_ALGO.FIFO;
			} else if (UPPER_ARG.contains("SCF") || UPPER_ARG.contains("SJF")) {
				sharingAlgo = SHARING_ALGO.SCF;
			} else if (UPPER_ARG.contains("NCF") || UPPER_ARG.contains("NJF")) {
				sharingAlgo = SHARING_ALGO.NCF;
			} else if (UPPER_ARG.contains("LCF") || UPPER_ARG.contains("LJF")) {
				sharingAlgo = SHARING_ALGO.LCF;
			} else if (UPPER_ARG.contains("SEBF")) {
				sharingAlgo = SHARING_ALGO.SEBF;
			} else if (UPPER_ARG.contains("DARK")) {
				sharingAlgo = SHARING_ALGO.DARK;
			} else {
				System.err.println("Unsupported or Wrong Sharing Algorithm");
				System.exit(1);
			}
		}

		boolean isOffline = false;
		int simulationTimestep = 10 * Constants.SIMULATION_SECOND_MILLIS;
		if (isOffline) {
			simulationTimestep = Constants.SIMULATION_ENDTIME_MILLIS;
		}

		boolean considerDeadline = false;
		double deadlineMultRandomFactor = 1;
		if (considerDeadline && args.length > curArg) {
			deadlineMultRandomFactor = Double.parseDouble(args[curArg++]);
		}

		//Create Network Topology
		Network network = new Network(10, Constants.RACK_BITS_PER_SEC);

		// Create TraceProducer
		TraceProducer traceProducer = null;

		int numRacks = network.getPods().size();
		int numJobs = 30;
		// random seed
		int randomSeed = (int) System.currentTimeMillis() / 1000;
		// constant seed
//		int randomSeed = 13;
		JobClassDescription[] jobClassDescs = new JobClassDescription[]{
						new JobClassDescription(1, 5, 1, 10),
						new JobClassDescription(1, 5, 10, 1000),
						new JobClassDescription(5, numRacks, 1, 10),
						new JobClassDescription(5, numRacks, 10, 1000)};
		double[] fracsOfClasses = new double[]{
						41,
						29,
						9,
						21};

		traceProducer = new CustomTraceProducer(numRacks, numJobs, jobClassDescs, fracsOfClasses,
						randomSeed, network);

		if (args.length > curArg) {
			String UPPER_ARG = args[curArg++].toUpperCase();

			if (UPPER_ARG.equals("CUSTOM")) {
				int numClasses = Integer.parseInt(args[curArg++]);

				jobClassDescs = new JobClassDescription[numClasses];
				for (int i = 0; i < numClasses; i++) {
					int minW = Integer.parseInt(args[curArg++]);
					int maxW = Integer.parseInt(args[curArg++]);
					int minL = Integer.parseInt(args[curArg++]);
					int maxL = Integer.parseInt(args[curArg++]);

					jobClassDescs[i] = new JobClassDescription(minW, maxW, minL, maxL);
				}

				fracsOfClasses = new double[numClasses];
				for (int i = 0; i < numClasses; i++) {
					fracsOfClasses[i] = Integer.parseInt(args[curArg++]);
				}

				numRacks = Integer.parseInt(args[curArg++]);
				numJobs = Integer.parseInt(args[curArg++]);
				randomSeed = Integer.parseInt(args[curArg++]);

				traceProducer = new CustomTraceProducer(numRacks, numJobs, jobClassDescs, fracsOfClasses,
								randomSeed, network);
			} else if (UPPER_ARG.equals("COFLOW-BENCHMARK")) {
				String pathToCoflowBenchmarkTraceFile = args[curArg++];
				traceProducer = new CoflowBenchmarkTraceProducer(pathToCoflowBenchmarkTraceFile);
			}
		}
		traceProducer.prepareTrace();

		Simulator nlpl = null;
		if (routingAlgo == ROUTING_ALGO.ECMP) {
			nlpl = new ECMPSimulator(null, traceProducer, isOffline, considerDeadline,
							deadlineMultRandomFactor, network);
		} else if (routingAlgo == ROUTING_ALGO.HEDERA) {
			nlpl = new HederaSimulator(null, traceProducer, isOffline, considerDeadline,
							deadlineMultRandomFactor, network);
		} else if (routingAlgo == ROUTING_ALGO.RAPIER) {
			nlpl = new RapierSimulator(null, traceProducer, isOffline, considerDeadline,
							deadlineMultRandomFactor, network);
		} else if (sharingAlgo == SHARING_ALGO.FAIR || sharingAlgo == SHARING_ALGO.PFP) {
			nlpl = new FlowSimulator(sharingAlgo, traceProducer, isOffline, considerDeadline,
							deadlineMultRandomFactor);
		} else if (sharingAlgo == SHARING_ALGO.DARK) {
			nlpl = new CoflowSimulatorDark(sharingAlgo, traceProducer);
		} else {
			nlpl = new CoflowSimulator(sharingAlgo, traceProducer, isOffline, considerDeadline,
							deadlineMultRandomFactor);
		}

		nlpl.simulate(simulationTimestep);
		nlpl.printStats(true);

//		ArrayList<ArrayList<String>> pods = network.getPods();
//		ODPair o = new ODPair(0, 1);
//		ArrayList<ArrayList<Link>> paths = network.getPaths(o);
//		for (int i = 0; i < paths.size(); i++) {
//			System.out.println(paths.get(i));
//		}
//
//		for(int i = 0; i < traceProducer.jobs.size(); i++) {
//			Job j = traceProducer.jobs.elementAt(i);
//			System.out.println("-----------" + j.jobName + "------------");
//			for(Task task : j.tasks) {
//				if(task.taskType == Task.TaskType.MAPPER) {
//					System.out.println("Mapper : " + task.getPlacement());
//				} else {
//					System.out.println("Reduce : " + task.getPlacement());
//				}
//			}
//			System.out.println("-----------------------");
//		}
	}
}
