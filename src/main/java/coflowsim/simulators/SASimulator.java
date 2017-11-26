package coflowsim.simulators;

import coflowsim.datastructures.*;
import coflowsim.traceproducers.TraceProducer;
import coflowsim.utils.Constants;
import coflowsim.utils.Utils;

import java.util.*;

/**
 * Simulated Annealing Simulator based on Simulated Annealing Methods
 */
public class SASimulator extends Simulator {

	Vector<Job> sortedJobs;

	private Network network;

	private Map<Integer, ArrayList<Link>> pathForFlow;
	private Map<Integer, Integer> pathIndexForFlow;

	private Vector<Vector<Flow>> flowsInPathOfRacks[];

	/**
	 * {@inheritDoc}
	 */
	public SASimulator(
					Constants.ROUTING_ALGO routingAlgo,
					TraceProducer traceProducer,
					boolean offline,
					boolean considerDeadline,
					double deadlineMultRandomFactor,
					Network network) {

		super(null, traceProducer, offline, considerDeadline, deadlineMultRandomFactor);
		this.network = network;

		// initialize the flow in each path of racks
		this.flowsInPathOfRacks = (Vector<Vector<Flow>>[]) new Vector[NUM_RACKS];
		for (int i = 0; i < this.flowsInPathOfRacks.length; i++) {
			flowsInPathOfRacks[i] = new Vector<Vector<Flow>>();
			for (int j = 0; j < network.getCore().size(); j++) {
				flowsInPathOfRacks[i].add(new Vector<Flow>());
			}
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void initialize(TraceProducer traceProducer) {
		super.initialize(traceProducer);

		this.sortedJobs = new Vector<Job>();
		// Initialize Path For Flow
		this.pathForFlow = new HashMap<Integer, ArrayList<Link>>();
		// Initialize Path Index For Flow
		this.pathIndexForFlow = new HashMap<Integer, Integer>();
	}

	/**
	 * Admission control for the deadline-sensitive case.
	 */
	@Override
	protected boolean admitThisJob(Job j) {
		return true;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void uponJobAdmission(Job j) {
		for (Task t : j.tasks) {
			if (t.taskType == Task.TaskType.REDUCER) {
				ReduceTask rt = (ReduceTask) t;

				// Update start stats for the task and its parent job
				rt.startTask(CURRENT_TIME);

				// Add the parent job to the collection of active jobs
				if (!activeJobs.containsKey(rt.parentJob.jobName)) {
					activeJobs.put(rt.parentJob.jobName, rt.parentJob);
					addToSortedJobs(j);
				}
				incNumActiveTasks();
			}
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void afterJobAdmission(long curTime) {
		layoutFlowsInJobOrder();
		updateRatesDynamicAlpha(curTime, false);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void onSchedule(long curTime) {
		proceedFlowsInAllRacks(curTime, Constants.SIMULATION_QUANTA);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void afterJobDeparture(long curTime) {
		updateRatesDynamicAlpha(curTime, false);
	}

	/**
	 * @param temperature: Iteration of Simulated Annealing
	 */
	private State SA(int temperature, final long curTime, boolean trialRun) {
		State s = initState();
		double e = energy(s, curTime, trialRun);
		State sb = s;
		double eb = e;
		for (int i = 0; i < temperature; i++) {
			State sn = neighbour(s);
			double en = energy(sn, curTime, trialRun);
			if (en < eb) {
				sb = sn;
				eb = en;
			}
			Random ranGen = new Random();
			if (P(e, en, i, temperature) > ranGen.nextDouble()) {
				s = sn;
				e = en;
			}
		}

		return sb;
	}

	private double P(double EN, double E, int T, int temperature) {
		int c = 1000 * temperature;
		if (EN < E) {
			return 1.0;
		} else {
			return Math.pow(Math.E, c * (E - EN) / T);
		}
	}

	/**
	 * init state of Simulated Annealing
	 */
	public State initState() {
		Map<Integer, Integer> coreMapping = new HashMap<Integer, Integer>();
		for (int i = 0; i < network.getServers().size(); i++) {
			coreMapping.put(i, i % network.getCore().size());
		}
		return new State(coreMapping, -1, -1, -1.0, -1.0);
	}

	/**
	 * Energy Function of Simulated Annealing
	 *
	 * @return
	 */
	private double energy(State s, final long curTime, boolean trialRun) {
		double a = 0.0;
		if (s.swap1 == -1 || s.swap2 == -1) {
			// For keeping track of jobs with invalid currentAlpha
			Vector<Job> skippedJobs = new Vector<Job>();
			for (Job sj : jobs) {
				double onlineAphla = calcAlphaOnline(sj, s.coreMapping);
				if (onlineAphla == Constants.VALUE_UNKNOWN) {
					skippedJobs.add(sj);
					continue;
				} else {
					sj.alpha = onlineAphla;
					a += onlineAphla;
//					System.out.println(onlineAphla);
				}
				// Use deadline instead of alpha when considering deadlines
				if (considerDeadline) {
					onlineAphla = sj.deadlineDuration / 1000;
				}
				updateRatesDynamicAlphaOneJob(sj, onlineAphla, trialRun);
			}
		} else {

		}
		return a;
	}

	/**
	 * Neighbour Function
	 */
	private State neighbour(State s) {
		Random rangen = new Random();
		int swap1 = rangen.nextInt(network.getCore().size());
		int swap2;
		while ((swap2 = rangen.nextInt(network.getCore().size())) == swap1) ;
		Map<Integer, Integer> coreMapping = s.coreMapping;
		int temp = coreMapping.get(swap1);
		coreMapping.put(swap1, coreMapping.get(swap2));
		coreMapping.put(swap2, temp);
		return new State(coreMapping, swap1, swap2, -1.0, s.currentEnergy);
	}


	/**
	 * Update rates of flows for each coflow using a dynamic alpha, calculated based on remaining
	 * bytes of the coflow and current condition of the network
	 *
	 * @param curTime  current time
	 * @param trialRun if true, do NOT change any jobs allocations
	 */
	private void updateRatesDynamicAlpha(final long curTime, boolean trialRun) {
		// Reset sendBpsFree and recvBpsFree
		network.resetBandWidthForLink();

		State sb = SA(100, curTime, trialRun);

		// Reset sendBpsFree and recvBpsFree
		network.resetBandWidthForLink();

		// For keeping track of jobs with invalid currentAlpha
		Vector<Job> skippedJobs = new Vector<Job>();

		// Recalculate rates
		for (Job sj : sortedJobs) {
			// Reset ALL rates first
			for (Task t : sj.tasks) {
				if (t.taskType == Task.TaskType.REDUCER) {
					ReduceTask rt = (ReduceTask) t;
					for (Flow f : rt.flows) {
						f.currentBps = 0;
					}
				}
			}

			double currentAlpha = calcAlphaOnline(sj, sb.coreMapping);
			if (currentAlpha == Constants.VALUE_UNKNOWN) {
				skippedJobs.add(sj);
				continue;
			}
			// Use deadline instead of alpha when considering deadlines
			if (considerDeadline) {
				currentAlpha = sj.deadlineDuration / 1000;
			}

			updateRatesDynamicAlphaOneJob(sj, currentAlpha, trialRun);
		}

//		// Work conservation
//		if (!trialRun) {
//			updateRatesFairShare(skippedJobs);
//
//			// Heuristic: Sort coflows by EDF and then refill
//			// If there is no deadline, this simply sorts them by arrival time
//			Vector<Job> sortedByEDF = new Vector<Job>(sortedJobs);
//			Collections.sort(sortedByEDF, new Comparator<Job>() {
//				public int compare(Job o1, Job o2) {
//					int timeLeft1 = (int) (o1.simulatedStartTime + o1.deadlineDuration - curTime);
//					int timeLeft2 = (int) (o2.simulatedStartTime + o2.deadlineDuration - curTime);
//					return timeLeft1 - timeLeft2;
//				}
//			});
//
//			for (Job sj : sortedByEDF) {
//				for (Task t : sj.tasks) {
//					if (t.taskType != Task.TaskType.REDUCER) {
//						continue;
//					}
//
//					ReduceTask rt = (ReduceTask) t;
//					int dst = rt.taskID;
//					boolean flag = false;
//					for (Flow f : rt.flows) {
//						ArrayList<Link> path = pathForFlow.get(f.id);
//						if (network.getFreeBandwidth(path.get(1)) <= Constants.ZERO) {
//							flag = true;
//							break;
//						}
//					}
//
//					if (flag) {
//						continue;
//					}
//
//					for (Flow f : rt.flows) {
//						int src = f.mapper.taskID;
//						ArrayList<Link> path = pathForFlow.get(f.id);
//						double sendBpsFree = network.getFreeBandwidth(path.get(0));
//						double recvBpsFree = network.getFreeBandwidth(path.get(1));
//						double minFree = Math.min(sendBpsFree, recvBpsFree);
//						if (minFree <= Constants.ZERO) minFree = 0.0;
//
//						f.currentBps += minFree;
//						// Remove how much capacity was allocated from link
//						network.setLinkBandwidth(path.get(0), sendBpsFree - minFree);
//						network.setLinkBandwidth(path.get(1), recvBpsFree - minFree);
//					}
//				}
//			}
//		}
	}


	private double calcAlphaOnline(Job job, Map<Integer, Integer> coreMapping) {
		double[][] sendBytes = new double[NUM_RACKS][network.getCore().size()];
		double[][] recvBytes = new double[NUM_RACKS][network.getCore().size()];

		for (Task t : job.tasks) {
			if (t.taskType == Task.TaskType.REDUCER) {
				ReduceTask rt = (ReduceTask) t;
				for (Flow f : rt.flows) {
					int machineID = f.mapper.assignedMachine.machineID;
					int pathIndex = coreMapping.get(machineID);
					pathForFlow.put(f.id, network.getPaths(f.mapper.taskID, rt.taskID).get(pathIndex));
					pathIndexForFlow.put(f.id, pathIndex);
					sendBytes[f.mapper.taskID][pathIndex] += f.bytesRemaining;
					recvBytes[rt.taskID][pathIndex] += f.bytesRemaining;
				}
			}
		}

		// Scale by available capacity
		for (int i = 0; i < NUM_RACKS; i++) {
			ArrayList<ArrayList<Link>> sendPaths = network.getPaths(i, (i + 1) % NUM_RACKS);
			ArrayList<ArrayList<Link>> recvPaths = network.getPaths((i + 1) % NUM_RACKS, i);
			for (int j = 0; j < network.getCore().size(); j++) {
				double sFree = network.getFreeBandwidth(sendPaths.get(j).get(0));
				double rFree = network.getFreeBandwidth(recvPaths.get(j).get(1));
				if ((sendBytes[i][j] > 0 && sFree <= Constants.ZERO)
								|| (recvBytes[i][j] > 0 && rFree <= Constants.ZERO)) {
					return Constants.VALUE_UNKNOWN;
				}

				sendBytes[i][j] = sendBytes[i][j] * 8 / sFree;
				recvBytes[i][j] = recvBytes[i][j] * 8 / rFree;
			}
		}

		return Math.max(Utils.max(sendBytes), Utils.max(recvBytes));
	}

	/**
	 * Update rates of individual flows from a collection of coflows while considering them as a
	 * single coflow.
	 * <p>
	 * Modifies sendBpsFree and recvBpsFree
	 *
	 * @param jobsToConsider Collection of {@link coflowsim.datastructures.Job} under consideration.
	 * @return
	 */
	private double updateRatesFairShare(
					Vector<Job> jobsToConsider) {
		double totalAlloc = 0.0;

		// Calculate the number of mappers and reducers in each port
		int[][] numMapSideFlows = new int[NUM_RACKS][network.getCore().size()];
		for (int i = 0; i < numMapSideFlows.length; i++) {
			for (int j = 0; j < numMapSideFlows[i].length; j++) {
				numMapSideFlows[i][j] = 0;
			}
		}
		int[][] numReduceSideFlows = new int[NUM_RACKS][network.getCore().size()];
		for (int i = 0; i < numMapSideFlows.length; i++) {
			for (int j = 0; j < numMapSideFlows[i].length; j++) {
				numReduceSideFlows[i][j] = 0;
			}
		}

		for (Job sj : jobsToConsider) {
			for (Task t : sj.tasks) {
				if (t.taskType != Task.TaskType.REDUCER) {
					continue;
				}

				ReduceTask rt = (ReduceTask) t;
				int dst = rt.taskID;
				boolean flag = false;
				for (Flow f : rt.flows) {
					ArrayList<Link> path = pathForFlow.get(f.id);
					if (network.getFreeBandwidth(path.get(1)) <= Constants.ZERO) {
						flag = true;
						break;
					}
				}

				if (flag) {
					continue;
				}

				for (Flow f : rt.flows) {
					int src = f.mapper.taskID;
					ArrayList<Link> path = pathForFlow.get(f.id);
					double sendBpsFree = network.getFreeBandwidth(path.get(0));
					if (sendBpsFree <= Constants.ZERO) {
						continue;
					}
					numMapSideFlows[src][pathIndexForFlow.get(f.id)]++;
					numReduceSideFlows[dst][pathIndexForFlow.get(f.id)]++;
				}
			}
		}

		for (Job sj : jobsToConsider) {
			for (Task t : sj.tasks) {
				if (t.taskType != Task.TaskType.REDUCER) {
					continue;
				}

				ReduceTask rt = (ReduceTask) t;
				int dst = rt.taskID;
				boolean flag = false;
				for (Flow f : rt.flows) {
					int pathIndex = pathIndexForFlow.get(f.id);
					ArrayList<Link> path = pathForFlow.get(f.id);
					if (network.getFreeBandwidth(path.get(1)) <= Constants.ZERO || numReduceSideFlows[dst][pathIndex] == 0) {
						flag = true;
						break;
					}
				}
				if (flag) {
					continue;
				}

				for (Flow f : rt.flows) {
					int src = f.mapper.taskID;
					ArrayList<Link> path = pathForFlow.get(f.id);
					int pathIndex = pathIndexForFlow.get(f.id);
					double sendBpsFree = network.getFreeBandwidth(path.get(0));
					double recvBpsFree = network.getFreeBandwidth(path.get(1));

					if (sendBpsFree <= Constants.ZERO || numMapSideFlows[src][pathIndex] == 0) {
						continue;
					}

					// Determine rate based only on this job and available bandwidth
					double minFree = Math.min(sendBpsFree / numMapSideFlows[src][pathIndex],
									recvBpsFree / numReduceSideFlows[dst][pathIndex]);
					if (minFree <= Constants.ZERO) {
						minFree = 0.0;
					}

					f.currentBps = minFree;

					// Remove how much capacity was allocated from link
					network.setLinkBandwidth(path.get(0), sendBpsFree - minFree);
					network.setLinkBandwidth(path.get(1), recvBpsFree - minFree);
					totalAlloc += f.currentBps;
				}
			}
		}

		return totalAlloc;
	}

	/**
	 * Update rates of individual flows of a given coflow.
	 *
	 * @param sj           {@link coflowsim.datastructures.Job} under consideration.
	 * @param currentAlpha dynamic alpha is in Seconds (Normally, alpha is in Bytes).
	 * @param trialRun     if true, do NOT change any jobs allocations.
	 * @return
	 */
	private double updateRatesDynamicAlphaOneJob(
					Job sj,
					double currentAlpha,
					boolean trialRun) {

		double jobAlloc = 0.0;

		for (Task t : sj.tasks) {
			if (t.taskType != Task.TaskType.REDUCER) {
				continue;
			}

			ReduceTask rt = (ReduceTask) t;
			int dst = rt.taskID;
			boolean flag = false;
			for (Flow f : rt.flows) {
				ArrayList<Link> path = pathForFlow.get(f.id);
				if (network.getFreeBandwidth(path.get(1)) <= Constants.ZERO) {
					flag = true;
					break;
				}
			}
			if (flag) {
				continue;
			}

			for (Flow f : rt.flows) {

				ArrayList<Link> path = pathForFlow.get(f.id);
				double sendBpsFree = network.getFreeBandwidth(path.get(0));
				double recvBpsFree = network.getFreeBandwidth(path.get(1));

				int src = f.mapper.taskID;

				double curBps = f.bytesRemaining * 8 / currentAlpha;
				if (curBps > sendBpsFree || curBps > recvBpsFree) {
					curBps = Math.min(sendBpsFree, recvBpsFree);
				}

				// Remove capacity was allocated from links
				network.setLinkBandwidth(path.get(0), sendBpsFree - curBps);
				network.setLinkBandwidth(path.get(1), recvBpsFree - curBps);
				jobAlloc += curBps;

				// Update f.currentBps if required
				if (!trialRun) {
					f.currentBps = curBps;
				}
			}
		}
		return jobAlloc;
	}

	protected void addToSortedJobs(Job j) {
		if (considerDeadline) {
			sortedJobs.add(j);
			return;
		}

		int index = 0;
		for (Job sj : sortedJobs) {
			if (SEBFComparator.compare(j, sj) < 0) {
				break;
			}
			index++;
		}
		sortedJobs.insertElementAt(j, index);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void removeDeadJob(Job j) {
		activeJobs.remove(j.jobName);
		sortedJobs.remove(j);
	}

	private void proceedFlowsInAllRacks(long curTime, long quantaSize) {
		for (int i = 0; i < NUM_RACKS; i++) {
			double totalBytesMoved = 0;
			Vector<Flow> flowsToRemove = new Vector<Flow>();
			for (Flow f : flowsInRacks[i]) {
				ReduceTask rt = f.reducer;

				if (totalBytesMoved >= Constants.RACK_BYTES_PER_SEC) {
					break;
				}

				double bytesPerTask = (f.currentBps / 8)
								* (1.0 * quantaSize / Constants.SIMULATION_SECOND_MILLIS);
				bytesPerTask = Math.min(bytesPerTask, f.bytesRemaining);

				f.bytesRemaining -= bytesPerTask;
				if (f.bytesRemaining <= Constants.ZERO) {
					rt.flows.remove(f);
					flowsToRemove.add(f);

					// Give back to src and dst links
					// Remove how much capacity was allocated from link
					ArrayList<Link> path = pathForFlow.get(f.id);
					network.setLinkBandwidth(path.get(0), network.getFreeBandwidth(path.get(0)) + f.currentBps);
					network.setLinkBandwidth(path.get(1), network.getFreeBandwidth(path.get(1)) + f.currentBps);
				}

				totalBytesMoved += bytesPerTask;
				rt.shuffleBytesLeft -= bytesPerTask;
				rt.parentJob.decreaseShuffleBytesPerRack(rt.taskID, bytesPerTask);
				rt.parentJob.shuffleBytesCompleted += bytesPerTask;

				// If no bytes remaining, mark end and mark for removal
				if ((rt.shuffleBytesLeft <= Constants.ZERO || rt.flows.size() == 0) && !rt.isCompleted()) {
					rt.cleanupTask(curTime + quantaSize);
					if (!rt.parentJob.jobActive) {
						removeDeadJob(rt.parentJob);
					}
					decNumActiveTasks();
				}
			}
			flowsInRacks[i].removeAll(flowsToRemove);
		}
	}

	protected void layoutFlowsInJobOrder() {
		for (int i = 0; i < NUM_RACKS; i++) {
			flowsInRacks[i].clear();
		}

		for (Job j : sortedJobs) {
			for (Task r : j.tasks) {
				if (r.taskType != Task.TaskType.REDUCER) {
					continue;
				}

				ReduceTask rt = (ReduceTask) r;
				if (rt.isCompleted()) {
					continue;
				}

				flowsInRacks[r.taskID].addAll(rt.flows);
			}
		}
	}

	/**
	 * Comparator used by {@link CoflowSimulator#addToSortedJobs(Job)} to add new job in list sorted
	 * by coflow length.
	 */
	private static Comparator<Job> SCFComparator = new Comparator<Job>() {
		public int compare(Job o1, Job o2) {
			if (o1.maxShuffleBytes / o1.numMappers == o2.maxShuffleBytes / o2.numMappers)
				return 0;
			return o1.maxShuffleBytes / o1.numMappers < o2.maxShuffleBytes / o2.numMappers ? -1 : 1;
		}
	};

	/**
	 * Comparator used by {@link CoflowSimulator#addToSortedJobs(Job)} to add new job in list sorted
	 * by coflow size.
	 */
	private static Comparator<Job> LCFComparator = new Comparator<Job>() {
		public int compare(Job o1, Job o2) {
			double n1 = o1.calcShuffleBytesLeft();
			double n2 = o2.calcShuffleBytesLeft();
			if (n1 == n2) return 0;
			return n1 < n2 ? -1 : 1;
		}
	};

	/**
	 * Comparator used by {@link CoflowSimulator#addToSortedJobs(Job)} to add new job in list sorted
	 * by the minimum number of endpoints of a coflow.
	 */
	private static Comparator<Job> NCFComparator = new Comparator<Job>() {
		public int compare(Job o1, Job o2) {
			int n1 = (o1.numMappers < o1.numReducers) ? o1.numMappers : o1.numReducers;
			int n2 = (o2.numMappers < o2.numReducers) ? o2.numMappers : o2.numReducers;
			return n1 - n2;
		}
	};

	/**
	 * Comparator used by {@link CoflowSimulator#addToSortedJobs(Job)} to add new job in list sorted
	 * by static coflow skew.
	 */
	private static Comparator<Job> SEBFComparator = new Comparator<Job>() {
		public int compare(Job o1, Job o2) {
			if (o1.alpha == o2.alpha) return 0;
			return o1.alpha < o2.alpha ? -1 : 1;
		}
	};

	/**
	 * State Object For Simulated Annealing
	 */
	private class State {

		/**
		 * First Integer : Machine Server Id
		 * Second Integer : Core Switch Id
		 */
		public Map<Integer, Integer> coreMapping;
		public int swap1;
		public int swap2;
		public double currentEnergy;
		public double lastEnergy;


		public State(Map<Integer, Integer> cm, int swap1, int swap2,
								 double currentEnergy, double lastEnergy) {
			this.coreMapping = new HashMap<Integer, Integer>();
			for(Integer key : cm.keySet()) {
				this.coreMapping.put(key, cm.get(key));
			}
			this.swap1 = swap1;
			this.swap2 = swap2;
			this.currentEnergy = currentEnergy;
			this.lastEnergy = lastEnergy;
		}
	}
}
