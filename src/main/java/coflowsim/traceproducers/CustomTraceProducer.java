package coflowsim.traceproducers;

import java.util.Arrays;
import java.util.Random;

import coflowsim.datastructures.*;
import coflowsim.utils.Constants;
import coflowsim.utils.Utils;

/**
 * Creates a random trace based on the given parameters.
 * <p>
 * Characteristics of the generated trace:
 * <ul>
 * <li>Each rack has at most one mapper and at most one reducer. Historically, this was because
 * production clusters at Facebook and Microsoft are oversubscribed in core-rack links; essentially,
 * simulating rack-level was enough for them. For full-bisection bandwidth networks, setting to the
 * number of machines should result in desired outcome.
 * <li>All tasks of a phase are known when that phase starts, meaning all mappers start together and
 * all reducers do the same.
 * <li>Mapper arrival times are ignored because they are assumed to be over before reducers start;
 * i.e., shuffle start time is reducers' start time.
 * <li>Assuming all reducers to arrive together arrive at time zero. This should be replaced by an
 * appropriate arrival function like Poisson arrival.
 * <li>All times are in milliseconds.
 * </ul>
 */
public class CustomTraceProducer extends TraceProducer {

	private final int NUM_RACKS;
	private final int MACHINES_PER_RACK;

	private final int REDUCER_ARRIVAL_TIME = 0;

	public int numJobs;

	private final int numJobClasses;
	private final JobClassDescription[] jobClass;

	private final double sumFracs;
	private final double[] fracsOfClasses;

	private Network network;

	private final Random ranGen;

	private Constants.ROUTING_ALGO routingAlgo;

	/**
	 * Constructor and input validator.
	 *
	 * @param numRacks       Number of racks in the trace.
	 * @param numJobs        Number of jobs to create.
	 * @param jobClassDescs  Description of job classes ({@link coflowsim.traceproducers.JobClassDescription}).
	 * @param fracsOfClasses Fractions of jobs from each job class.
	 * @param randomSeed     Random seed to use for all randomness inside.
	 * @param network 			 Network Topology Instance
	 */
	public CustomTraceProducer(
					int numRacks,
					int numJobs,
					JobClassDescription[] jobClassDescs,
					double[] fracsOfClasses,
					int randomSeed,
					Constants.ROUTING_ALGO routingAlgo,
					Network network) {

		ranGen = new Random(randomSeed);

		this.numJobs = numJobs;

		this.numJobClasses = jobClassDescs.length;
		this.jobClass = jobClassDescs;
		this.fracsOfClasses = fracsOfClasses;
		this.sumFracs = Utils.sum(fracsOfClasses);
		this.network = network;
		this.NUM_RACKS = numRacks;
		this.MACHINES_PER_RACK = network.getPods().get(0).size();
		System.out.println(NUM_RACKS + "  " + MACHINES_PER_RACK);
		this.routingAlgo = routingAlgo;

		// Check input validity
		assert (jobClassDescs.length == numJobClasses);
		assert (fracsOfClasses.length == numJobClasses);
	}

	/**
	 * Actually generates the random trace.
	 */
	@Override
	public void prepareTrace() {

		// Create the tasks
		int jID = 0;
		for (int i = 0; i < numJobClasses; i++) {

			int numJobsInClass = (int) (1.0 * numJobs * fracsOfClasses[i] / sumFracs);

			while (numJobsInClass-- > 0) {
				// Find corresponding job
				String jobName = "JOB-" + jID;
				jID++;
				Job job = jobs.getOrAddJob(jobName, routingAlgo);

				// #region: Create mappers
				int numMappers = ranGen.nextInt(jobClass[i].maxWidth - jobClass[i].minWidth + 1)
								+ jobClass[i].minWidth;

				// choose pod
				boolean[] rackChosen = new boolean[NUM_RACKS];
				Arrays.fill(rackChosen, false);
				for (int mID = 0; mID < numMappers; mID++) {
					String taskName = "MAPPER-" + mID;
					int taskID = mID;

					// Create map task
					// map machine index
					int mmIndex = ranGen.nextInt(MACHINES_PER_RACK);
					Task task = new MapTask(taskName, taskID, job, Constants.VALUE_IGNORED,
									Constants.VALUE_IGNORED, new Machine(selectMachine(rackChosen, mmIndex)));

					// Add task to corresponding job
					job.addTask(task);
				}
				// #endregion

				// #region: Create reducers
				int numReducers = ranGen.nextInt(jobClass[i].maxWidth - jobClass[i].minWidth + 1)
								+ jobClass[i].minWidth;

				// Mark racks so that there is at most one reducer per rack
				rackChosen = new boolean[NUM_RACKS];
				Arrays.fill(rackChosen, false);
				for (int rID = 0; rID < numReducers; rID++) {
					int numMB = ranGen.nextInt(jobClass[i].maxLength - jobClass[i].minLength + 1)
									+ jobClass[i].minLength;

					double shuffleBytes = numMB * 1048576.0;

					// shuffleBytes for each mapper
					shuffleBytes *= numMappers;

					String taskName = "REDUCER-" + rID;
					int taskID = rID;

					// Create reduce task
					// Machine index in rack (pod)
					int rmIndex = ranGen.nextInt(MACHINES_PER_RACK);
					Task task = new ReduceTask(taskName, taskID, job, REDUCER_ARRIVAL_TIME,
									Constants.VALUE_IGNORED, new Machine(selectMachine(rackChosen, rmIndex)), shuffleBytes,
									Constants.VALUE_IGNORED);

					// Add task to corresponding job
					job.addTask(task);
				}
				// #endregion
			}
		}
	}

	/**
	 * Selects a rack that has no tasks, returns its index, and updates bookkeeping.
	 * <p>
	 * Because CustomTraceProducer essentially has one machine per rack, selecting rack is equivalent
	 * to selecting a machine.
	 *
	 * @param racksAlreadyChosen keeps track of racks that have already been used.
	 * @return the selected rack's index
	 */
	private int selectMachine(boolean[] racksAlreadyChosen, int mIndex) {
		int rackIndex = -1;
		while (rackIndex == -1) {
			rackIndex = ranGen.nextInt(NUM_RACKS);
			if (racksAlreadyChosen[rackIndex]) {
				rackIndex = -1;
			}
		}
		racksAlreadyChosen[rackIndex] = true;
		// 0 <= rackIndex < NUM_RACKS
//		System.out.println(rackIndex * MACHINES_PER_RACK +  mIndex);
		return rackIndex * MACHINES_PER_RACK +  mIndex;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int getNumRacks() {
		return NUM_RACKS;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int getMachinesPerRack() {
		return MACHINES_PER_RACK;
	}
}
