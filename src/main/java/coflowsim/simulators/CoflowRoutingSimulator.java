package coflowsim.simulators;

import coflowsim.datastructures.*;
import coflowsim.traceproducers.TraceProducer;
import coflowsim.utils.Constants;

import java.util.*;

/**
 * Created by lee on 2017/11/20.
 */
public class CoflowRoutingSimulator extends Simulator {

	Vector<Job> sortedJobs;

	Network network;

	Map<Integer, ArrayList<Link>> pathForFlow;
	Map<String, ArrayList<Flow>> flowsInPath;

	Constants.ROUTING_ALGO routingAlgo;

	/**
	 * {@inheritDoc}
	 */
	public CoflowRoutingSimulator(
					Constants.SHARING_ALGO sharingAlgo,
					Constants.ROUTING_ALGO routingAlgo,
					TraceProducer traceProducer,
					boolean offline,
					boolean considerDeadline,
					double deadlineMultRandomFactor,
					Network network) {

		super(sharingAlgo, traceProducer, offline, considerDeadline, deadlineMultRandomFactor);
		this.network = network;
		this.routingAlgo = routingAlgo;
		assert (sharingAlgo == Constants.SHARING_ALGO.FIFO || sharingAlgo == Constants.SHARING_ALGO.SCF
						|| sharingAlgo == Constants.SHARING_ALGO.NCF || sharingAlgo == Constants.SHARING_ALGO.LCF
						|| sharingAlgo == Constants.SHARING_ALGO.SEBF);
	}

	@Override
	protected void initialize(TraceProducer traceProducer) {
		super.initialize(traceProducer);
		this.sortedJobs = new Vector<Job>();
		this.pathForFlow = new HashMap<Integer, ArrayList<Link>>();
		//Initialize Link Bandwidth
		network.resetBandWidthForLink();
	}

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

	@Override
	protected void afterJobAdmission(long curTime) {
		super.afterJobAdmission(curTime);
		updateFlowRate();
	}

	private void updateFlowRate() {
		// For keeping track of jobs with invalid rate
		Vector<Job> skippedJobs = new Vector<Job>();

		//Recalculate the rate for flows
		for (Job sj : sortedJobs) {

			// 1. Reset ALL rates first
			for (Task t : sj.tasks) {
				if (t.taskType == Task.TaskType.REDUCER) {
					ReduceTask rt = (ReduceTask) t;
					for (Flow f : rt.flows) {
						f.currentBps = 0;
					}
				}
			}
		}
	}



	private void allocateBandwidth(ArrayList<Link> selectedPath, Flow flow) {
		// 1. Get the sendFreeBps and RevFreeBps
		double sendFreeBps = network.getFreeBandwidth(selectedPath.get(0));
		double revFreeBps = network.getFreeBandwidth(selectedPath.get(1));

		// 2. Get the flow rate of the min of sendFreeBps and revFreeBps
		double rate = Math.min(sendFreeBps, revFreeBps);

		if (rate <= Constants.ZERO) {
			rate = 0.0;
		}
		flow.currentBps = rate;


		// 3. Update Link Free Bps
		network.setLinkBandwidth(selectedPath.get(0), sendFreeBps - rate);
		network.setLinkBandwidth(selectedPath.get(1), revFreeBps - rate);
	}

	protected void onSchedule(long curTime) {
		proceedFlowsInAllRacks(curTime, Constants.SIMULATION_QUANTA);
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
					Link send = pathForFlow.get(f.id).get(0);
					Link rev = pathForFlow.get(f.id).get(1);
					network.setLinkBandwidth(send, network.getFreeBandwidth(send) + f.currentBps);
					network.setLinkBandwidth(rev, network.getFreeBandwidth(rev) + f.currentBps);
					onFlowCompleted(f);
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

	/**
	 * When flow completed, update rate for flows in the same path
	 */
	private void onFlowCompleted(Flow flow) {
		ArrayList<Flow> flows = flowsInPath.get(flow.path);
		flows.remove(flow);
		for(Flow f : flows) {
			ArrayList<Link> path = pathForFlow.get(f.id);
			allocateBandwidth(path, f);
		}
	}

	protected void removeDeadJob(Job j) {
		activeJobs.remove(j.jobName);
		sortedJobs.remove(j);
	}

	@Override
	protected void afterJobDeparture(long curTime) {
		super.afterJobDeparture(curTime);
	}

	protected void addToSortedJobs(Job j) {
		if (considerDeadline) {
			sortedJobs.add(j);
			return;
		}

		if (routingAlgo == Constants.ROUTING_ALGO.ECMP ||
						routingAlgo == Constants.ROUTING_ALGO.RAPIER ||
						sharingAlgo == Constants.SHARING_ALGO.FIFO) {
			sortedJobs.add(j);
		} else {
			int index = 0;
			for (Job sj : sortedJobs) {
				if (sharingAlgo == Constants.SHARING_ALGO.SCF && SCFComparator.compare(j, sj) < 0) {
					break;
				} else if (sharingAlgo == Constants.SHARING_ALGO.NCF && NCFComparator.compare(j, sj) < 0) {
					break;
				} else if (sharingAlgo == Constants.SHARING_ALGO.LCF && LCFComparator.compare(j, sj) < 0) {
					break;
				} else if (sharingAlgo == Constants.SHARING_ALGO.SEBF && SEBFComparator.compare(j, sj) < 0) {
					break;
				}
				index++;
			}
			sortedJobs.insertElementAt(j, index);
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

}
