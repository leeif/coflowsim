package coflowsim.simulators;

import coflowsim.datastructures.*;
import coflowsim.traceproducers.TraceProducer;
import coflowsim.utils.Constants;

import java.util.Arrays;
import java.util.Random;
import java.util.Vector;

/**
 * Flow-level Routing using randomly-choose path
 */
public class ECMPSimulator extends Simulator {

	private Network network;
	private Vector<Vector<Flow>> flowsInPathOfRacks[];

	/**
	 * {@inheritDoc}
	 */
	public ECMPSimulator(
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
				int pathIndex = ECMP();
				addAscending(flowsInPathOfRacks[toRack].elementAt(pathIndex), f);
				added++;
				if (added >= numFlowsToAdd) {
					break;
				}
			}
		}
	}

	@Override
	protected void afterJobAdmission(long curTime) {
		super.afterJobAdmission(curTime);
	}

	/**
	 * Select path for flow using ECMP Algorithm
	 */
	private int ECMP() {
		Random random = new Random();
		int pathIndex = random.nextInt(network.getCore().size() / 4);
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

	/**
	 * Proceed flows in each rack in the already-determined order; e.g., shortest-first of PFP or
	 * earliest-deadline-first in the deadline-sensitive scenario.
	 *
	 * @param curTime    current time
	 * @param quantaSize size of each simulation time step
	 */
	private void proceedFlowsInAllRacksInSortedOrder(long curTime, long quantaSize) {
		boolean[] mapSideBusy = new boolean[NUM_RACKS];
		Arrays.fill(mapSideBusy, false);

		for (int i = 0; i < NUM_RACKS; i++) {
			Vector<Flow> flowsToRemove = new Vector<Flow>();
			for (Flow f : flowsInRacks[i]) {
				if (!mapSideBusy[f.mapper.taskID]) {
					mapSideBusy[f.mapper.taskID] = true;

					ReduceTask rt = f.reducer;

					double bytesPerTask = Constants.RACK_BYTES_PER_SEC
									* (1.0 * quantaSize / Constants.SIMULATION_SECOND_MILLIS);
					bytesPerTask = Math.min(bytesPerTask, f.bytesRemaining);

					f.bytesRemaining -= bytesPerTask;
					if (f.bytesRemaining <= Constants.ZERO) {
						rt.flows.remove(f);
						flowsToRemove.add(f);
					}

					rt.shuffleBytesLeft -= bytesPerTask;

					rt.parentJob.decreaseShuffleBytesPerRack(rt.taskID, bytesPerTask);

					// If no bytes remaining, mark end and mark for removal
					if ((rt.shuffleBytesLeft <= Constants.ZERO || rt.flows.size() == 0)
									&& !rt.isCompleted()) {

						rt.cleanupTask(curTime + quantaSize);
						if (!rt.parentJob.jobActive) {
							removeDeadJob(rt.parentJob);
						}
						decNumActiveTasks();
					}

					break;
				} else {
					System.out.print("");
				}
			}
			flowsInRacks[i].removeAll(flowsToRemove);
		}
	}
}
