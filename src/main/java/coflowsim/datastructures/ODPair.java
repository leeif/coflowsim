package coflowsim.datastructures;


/**
 * ODPair in level of Pod
 */
public class ODPair implements Comparable<ODPair> {

	private int sourcePod;
	private int desPod;


	/**
	 *
	 * @param sourcePod
	 * @param desPod
	 */
	public ODPair(int sourcePod, int desPod) {
		this.sourcePod = sourcePod;
		this.desPod = desPod;
	}

	public int getSourcePod() {
		return sourcePod;
	}

	public int getDesPod() {
		return desPod;
	}

	@Override
	public boolean equals(Object obj) {
		ODPair od = (ODPair) obj;
		return od.getSourcePod() == this.sourcePod &&
						od.getDesPod() == this.desPod;
	}

	public int compareTo(ODPair o) {
		if (sourcePod == o.getSourcePod() && desPod == o.getDesPod()) {
			return 0;
		} else {
			return 1;
		}
	}
}
