package coflowsim.simulators;

import coflowsim.datastructures.*;
import coflowsim.traceproducers.TraceProducer;
import coflowsim.utils.Constants;

import java.util.*;

/**
 * Created by lee on 2017/11/21.
 */
public class HederaSimulator extends Simulator {
	private Network network;
	private Vector<Vector<Flow>> flowsInPathOfRacks[];

	/**
	 * {@inheritDoc}
	 */
	public HederaSimulator(
					Constants.ROUTING_ALGO routingAlgo,
					TraceProducer traceProducer,
					boolean offline,
					boolean considerDeadline,
					double deadlineMultRandomFactor,
					Network network) {

		super(null, traceProducer, offline, considerDeadline, deadlineMultRandomFactor);

		assert (routingAlgo == Constants.ROUTING_ALGO.ECMP);

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

	private void addAscending(Vector<Flow> coll, Vector<Flow> flows) {
		for (Flow f : flows) {
			addAscending(coll, f);
		}
	}

	/**
	 * Add flow in FIFO Order
	 *
	 * @param coll
	 * @param flow
	 */
	private void addAscending(Vector<Flow> coll, Flow flow) {
		flow.consideredAlready = true;
		coll.add(flow);
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
				}

				incNumActiveTasks();
			}
		}

	}

	@Override
	protected void afterJobAdmission(long curTime) {
		super.afterJobAdmission(curTime);
		estimateDemand();
		for (Job j : jobs) {
			for (Task r : j.tasks) {
				if (r.taskType != Task.TaskType.REDUCER) {
					continue;
				}
				ReduceTask rt = (ReduceTask) r;

				// Add at most Constants.MAX_CONCURRENT_FLOWS flows for FAIR sharing
				int numFlowsToAdd = rt.flows.size();
				if (sharingAlgo == Constants.SHARING_ALGO.FAIR) {
					numFlowsToAdd = Constants.MAX_CONCURRENT_FLOWS;
				}
				numFlowsToAdd = rt.flows.size();

				int added = 0;
				for (Flow f : rt.flows) {
					int toRack = rt.taskID;
					int pathIndex = Hedera(f);

					addAscending(flowsInPathOfRacks[toRack].elementAt(pathIndex), f);
					added++;
					if (added >= numFlowsToAdd) {
						break;
					}
				}
			}
		}
		for(Vector<Flow> flows : flowsInPathOfRacks[0]) {
			System.out.println(flows.size());
		}
	}

	/**
	 * Select path for flow using Hedera Global Fit Algorithm
	 */
	private int Hedera(Flow flow) {
		int pathIndex = -1;
		// 1. Choose path based on estimate rate
		ArrayList<ArrayList<Link>> paths = network.getPaths(flow.mapper.taskID, flow.reducer.taskID);

		double max = 0.0;
		for (int i = 0; i < paths.size(); i++) {
			ArrayList<Link> path = paths.get(i);
			double freeBandwidth = Math.min(network.getFreeBandwidth(path.get(0)),
							network.getFreeBandwidth(path.get(1)));
			if (freeBandwidth >= flow.demand * Constants.RACK_BITS_PER_SEC) {
				if((freeBandwidth - flow.demand * Constants.RACK_BITS_PER_SEC) > max) {
					pathIndex = i;
				}
			}
		}

		if (pathIndex == -1) {
			Random random = new Random();
			pathIndex = random.nextInt(network.getCore().size());
		} else {
			// 2. Update link free bandwidth
			ArrayList<Link> path = paths.get(pathIndex);
			for (Link link : path) {
				network.setLinkBandwidth(link,
								network.getFreeBandwidth(link) - flow.demand * Constants.RACK_BITS_PER_SEC);
			}
		}

		return pathIndex;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void onSchedule(long curTime) {
		fairShare(curTime, Constants.SIMULATION_QUANTA);
//		proceedFlowsInAllRacksInSortedOrder(curTime, Constants.SIMULATION_QUANTA);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void removeDeadJob(Job j) {
		activeJobs.remove(j.jobName);
	}

	/**
	 * Flow-level fair sharing
	 *
	 * @param curTime    current time
	 * @param quantaSize size of each simulation time step
	 */
	private void fairShare(long curTime, long quantaSize) {
		// Calculate the number of outgoing flows
		Vector<Integer>[] numMapSideFlows = (Vector<Integer>[]) new Vector[NUM_RACKS];
		for (int i = 0; i < NUM_RACKS; i++) {
			numMapSideFlows[i] = new Vector<Integer>();
			for (int j = 0; j < network.getCore().size(); j++) {
				numMapSideFlows[i].add(0);
			}
		}

		for (int i = 0; i < NUM_RACKS; i++) {
			Vector<Vector<Flow>> flowInPaths = flowsInPathOfRacks[i];
			for (int j = 0; j < flowInPaths.size(); j++) {
				for (Flow f : flowInPaths.get(j)) {
					numMapSideFlows[f.mapper.taskID].set(j, numMapSideFlows[f.mapper.taskID].get(j) + 1);
				}
			}
		}

		for (int i = 0; i < NUM_RACKS; i++) {
			Vector<Flow> flowsToRemove = new Vector<Flow>();
			Vector<Flow> flowsToAdd = new Vector<Flow>();
			Vector<Vector<Flow>> flowsInPath = flowsInPathOfRacks[i];
			for (int j = 0; j < flowsInPath.size(); j++) {
				Vector<Flow> flows = flowsInPath.get(j);
				int numFlows = flows.size();
				if (numFlows == 0) {
					continue;
				}
				for (Flow f : flows) {
					ReduceTask rt = f.reducer;
					double bytesPerTask = Math.min(
									Constants.RACK_BYTES_PER_SEC * (1.0 * quantaSize / Constants.SIMULATION_SECOND_MILLIS)
													/ numFlows,
									Constants.RACK_BYTES_PER_SEC * (1.0 * quantaSize / Constants.SIMULATION_SECOND_MILLIS)
													/ numMapSideFlows[f.mapper.taskID].get(j));

					bytesPerTask = Math.min(bytesPerTask, f.bytesRemaining);
					f.bytesRemaining -= bytesPerTask;
					if (f.bytesRemaining <= Constants.ZERO) {
						// Remove the one that has finished right now
						rt.flows.remove(f);
						flowsToRemove.add(f);

						// Remember flows to add, if available
						for (Flow ff : rt.flows) {
							if (!ff.consideredAlready) {
								flowsToAdd.add(ff);
								break;
							}
						}
					}

					rt.shuffleBytesLeft -= bytesPerTask;

					// If no bytes remaining, mark end and mark for removal
					if ((rt.shuffleBytesLeft <= Constants.ZERO || rt.flows.size() == 0) && !rt.isCompleted()) {
						rt.cleanupTask(curTime + quantaSize);
						if (!rt.parentJob.jobActive) {
							removeDeadJob(rt.parentJob);
						}
						decNumActiveTasks();
					}
				}
				flowsInPathOfRacks[i].get(j).removeAll(flowsToRemove);
				addAscending(flowsInPathOfRacks[i].get(j), flowsToAdd);
			}
		}
	}

	//Hedera Estimate Demand
	private void estimateDemand() {

		Map<String, ArrayList<Flow>> flowsInEachOD = new HashMap<String, ArrayList<Flow>>();

		for (Job jb : jobs) {
			for (Task task : jb.tasks) {
				if (task.taskType != Task.TaskType.REDUCER) {
					continue;
				}
				ReduceTask rt = (ReduceTask) task;
				for (Flow flow : rt.flows) {
					String od = flow.mapper.taskID + "-" + rt.taskID;
					if (flowsInEachOD.get(od) == null) {
						ArrayList<Flow> flows = new ArrayList<Flow>();
						flows.add(flow);
						flowsInEachOD.put(od, flows);
					} else {
						flowsInEachOD.get(od).add(flow);
					}
				}
			}
		}


		boolean flag = true;
		while (flag) {
			flag = false;
			for (int i = 0; i < NUM_RACKS; i++) {
				for (int j = 0; j < NUM_RACKS; j++) {
					if (i != j) {
						if (estimateSource(i, j, flowsInEachOD)) {
							flag = true;
						}
					}
				}
			}

			for (int i = 0; i < NUM_RACKS; i++) {
				for (int j = 0; j < NUM_RACKS; j++) {
					if (i != j) {
						if (estimateDestination(i, j, flowsInEachOD)) {
							flag = true;
						}
					}
				}
			}
		}
	}

	private boolean estimateSource(int source, int destination,
																 Map<String, ArrayList<Flow>> flowsInEachOD) {
		double df = 0.0;
		double nu = 0.0;
		boolean res = false;
		String od = source + "-" + destination;
		if (!flowsInEachOD.containsKey(od)) {
			return res;
		}
		for (Flow flow : flowsInEachOD.get(od)) {
			if (flow.converged) {
				df += flow.demand;
			} else {
				nu += 1;
			}
		}
		double es = (1.0 - df) / nu;
		for (Flow flow : flowsInEachOD.get(od)) {
			if (!flow.converged) {
				flow.demand = es;
				res = true;
			}
		}
		return res;
	}

	private boolean estimateDestination(int source, int destination,
																			Map<String, ArrayList<Flow>> flowsInEachOD) {
		double dt = 0.0;
		double ds = 0.0;
		double nr = 0.0;
		double es;
		boolean res = false;
		String od = source + "-" + destination;
		if (!flowsInEachOD.containsKey(od)) {
			return res;
		}
		for (Flow flow : flowsInEachOD.get(od)) {
			flow.rl = true;
			dt += flow.demand;
			nr += 1;
		}
		if(dt < 0.9) {
			return false;
		} else {
			es = 1.0 / nr;
		}
		while (someRLisFalseOrNot(flowsInEachOD.get(od))) {
			nr = 0;
			for (Flow flow : flowsInEachOD.get(od)) {
				if (flow.rl) {
					if (flow.demand < es) {
						ds += flow.demand;
					} else {
						nr += 1;
					}
				}
			}
			es = (1.0 - ds) / nr;
		}
		for (Flow flow : flowsInEachOD.get(od)) {
			if (flow.rl) {
				flow.demand = es;
				flow.converged = true;
			}
		}
		return res;
	}

	private boolean someRLisFalseOrNot(ArrayList<Flow> flows) {
		for (Flow flow : flows) {
			if (!flow.rl) {
				return true;
			}
		}
		return false;
	}

}
