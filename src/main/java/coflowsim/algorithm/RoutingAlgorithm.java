package coflowsim.algorithm;

import coflowsim.datastructures.Job;
import coflowsim.datastructures.JobCollection;
import coflowsim.datastructures.Network;
import coflowsim.datastructures.Task;

import java.util.Vector;

/**
 * Routing Algorithm which choose path between multi pahts for flow
 */
public class RoutingAlgorithm {

	private Network network;

	public RoutingAlgorithm(Network network) {
		this.network = network;
	}


	public void ECMP(JobCollection jobs) {
		for(int i = 0; i < jobs.size(); i++) {
			Job job = jobs.elementAt(i);
			Vector<Task> tasks = job.tasks;
			for(Task task : tasks) {
				if(task.taskType == Task.TaskType.REDUCER) {
					int sourceServer = task.getPlacement();
				}
			}
		}
	}

	public void Hedera() {

	}

	public void Rapier() {

	}

	public void SA() {

	}
}
