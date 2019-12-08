use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::net::Ipv4Addr;
use std::rc::Rc;

use rand::Rng;

use crate::rdd::*;

/*
 * Class that captures a coalesced RDD by essentially keeping track of parent partitions
 *
 * @param index of this coalesced partition
 * @param rdd which it belongs to
 * @param parentsIndices list of indices in the parent that have been coalesced into this partition
 * @param preferredLocation the preferred location for this partition
private[spark] case class CoalescedRDDPartition(
    index: Int,
    @transient rdd: RDD[_],
    parentsIndices: Array[Int],
    @transient preferredLocation: Option[String] = None) extends Partition {
  var parents: Seq[Partition] = parentsIndices.map(rdd.partitions(_))

  @throws(classOf[IOException])
  private def writeObject(oos: ObjectOutputStream): Unit = Utils.tryOrIOException {
    // Update the reference to parent partition at the time of task serialization
    parents = parentsIndices.map(rdd.partitions(_))
    oos.defaultWriteObject()
  }
}
*/

#[derive(Serialize, Deserialize, Clone)]
struct CoalescedRddSplit {
  index: usize,
}

impl CoalescedRddSplit {
  fn new(index: usize) -> Self {
    unimplemented!()
  }

  /// Computes the fraction of the parents' partitions containing preferred_location within
  /// their preferred_locs.
  ///
  /// Returns locality of this coalesced partition between 0 and 1.
  fn local_fraction(&self) -> f64 {
    /*
    val loc = parents.count { p =>
      val parentPreferredLocations = rdd.context.getPreferredLocs(rdd, p.index).map(_.host)
      preferredLocation.exists(parentPreferredLocations.contains)
    }
    if (parents.isEmpty) 0.0 else loc.toDouble / parents.size.toDouble
    */
    unimplemented!()
  }
}

impl Split for CoalescedRddSplit {
  fn get_index(&self) -> usize {
    self.index
  }
}

/// Represents a coalesced RDD that has fewer partitions than its parent RDD
///
/// This type uses the PartitionCoalescer type to find a good partitioning of the parent RDD
/// so that each new partition has roughly the same number of parent partitions and that
/// the preferred location of each new partition overlaps with as many preferred locations of its
/// parent partitions
///
/// @param prev RDD to be coalesced
/// @param maxPartitions number of desired partitions in the coalesced RDD (must be positive)
/// @param partitionCoalescer [[PartitionCoalescer]] implementation to use for coalescing
#[derive(Serialize, Deserialize)]
pub struct CoalescedRdd<T: Data> {
  #[serde(with = "serde_traitobject")]
  parent: Arc<dyn Rdd<Item = T>>,
  max_partitions: usize,
  partition_coalescer: usize,
}

impl<T: Data> CoalescedRdd<T> {
  pub(crate) fn new(
    parent: Arc<dyn Rdd<Item = T>>,
    max_partitions: usize,
    partition_coalescer: usize,
  ) {
    assert!(max_partitions > 0);
  }
}

type SplitIdx = usize;

/// A PartitionCoalescer defines how to coalesce the partitions of a given RDD.
pub trait PartitionCoalescer<T: Data> {
  /// Coalesce the partitions of the given RDD.
  ///
  /// ## Arguments
  ///
  /// * max_partitions: the maximum number of partitions to have after coalescing
  /// * parent: the parent RDD whose partitions to coalesce
  ///
  /// ## Return
  /// A vec of `PartitionGroup`s, where each element is itself a vector of
  /// `Partition`s and represents a partition after coalescing is performed.
  fn coalesce(max_partitions: usize, parent: Box<dyn Rdd<Item = T>>) -> Vec<PartitionGroup>;
}

/// A group of `Partition`s
pub struct PartitionGroup {
  /// preferred location for the partition group
  pref_loc: Option<Ipv4Addr>,
  partitions: Vec<SplitIdx>,
}

impl PartitionGroup {
  fn new(pref_loc: Option<Ipv4Addr>) -> Self {
    PartitionGroup {
      pref_loc,
      partitions: vec![],
    }
  }

  fn num_partitions(&self) -> usize {
    self.partitions.len()
  }
}

impl PartialEq for PartitionGroup {
  fn eq(&self, other: &Self) -> bool {
    unimplemented!()
  }
}

impl Eq for PartitionGroup {}

impl PartialOrd for PartitionGroup {
  fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
    Some(self.cmp(other))
  }
}

impl Ord for PartitionGroup {
  fn cmp(&self, other: &Self) -> Ordering {
    unimplemented!()
  }
}

// Coalesce the partitions of a parent RDD (`prev`) into fewer partitions, so that each partition of
// this RDD computes one or more of the parent ones. It will produce exactly `max_partitions` if the
// parent had more than max_partitions, or fewer if the parent had fewer.
//
// This transformation is useful when an RDD with many partitions gets filtered into a smaller one,
// or to avoid having a large number of small tasks when processing a directory with many files.
//
// If there is no locality information (no preferred_locations) in the parent, then the coalescing
// is very simple: chunk parents that are close in the array in chunks.
// If there is locality information, it proceeds to pack them with the following four goals:
//
// (1) Balance the groups so they roughly have the same number of parent partitions
// (2) Achieve locality per partition, i.e. find one machine which most parent partitions prefer
// (3) Be efficient, i.e. O(n) algorithm for n parent partitions (problem is likely NP-hard)
// (4) Balance preferred machines, i.e. avoid as much as possible picking the same preferred machine
//
// Furthermore, it is assumed that the parent RDD may have many partitions, e.g. 100 000.
// We assume the final number of desired partitions is small, e.g. less than 1000.
//
// The algorithm tries to assign unique preferred machines to each partition. If the number of
// desired partitions is greater than the number of preferred machines (can happen), it needs to
// start picking duplicate preferred machines. This is determined using coupon collector estimation
// (2n log(n)). The load balancing is done using power-of-two randomized bins-balls with one twist:
// it tries to also achieve locality. This is done by allowing a slack (balanceSlack, where
// 1.0 is all locality, 0 is all balance) between two bins. If two bins are within the slack
// in terms of balance, the algorithm will assign partitions according to locality.

struct PartitionLocations {
  /// contains all the partitions from the previous RDD that don't have preferred locations
  parts_without_locs: Vec<SplitIdx>,
  /// contains all the partitions from the previous RDD that have preferred locations
  parts_with_locs: Vec<(Ipv4Addr, SplitIdx)>,
}

impl PartitionLocations {
  fn new<T: Data>(prev: Box<dyn Rdd<Item = T>>) -> Self {
    // Gets all the preferred locations of the previous RDD and splits them into partitions
    // with preferred locations and ones without
    let mut tmp_parts_with_loc: HashMap<SplitIdx, Vec<Ipv4Addr>> = HashMap::new();
    let mut parts_without_locs = vec![];
    let mut parts_with_locs = vec![];

    // first get the locations for each partition, only do this once since it can be expensive
    prev.splits().into_iter().for_each(|p| {
      let locs = Self::current_pref_locs(p.clone(), &*prev);
      if !locs.is_empty() {
        tmp_parts_with_loc.insert(p.get_index(), locs);
      } else {
        parts_without_locs.push(p.get_index());
      }
    });
    // convert it into an array of host to partition
    for x in 0..=2 {
      for (part_idx, locs) in tmp_parts_with_loc.iter() {
        if locs.len() > x {
          parts_with_locs.push((locs[x], *part_idx))
        }
      }
    }

    PartitionLocations {
      parts_without_locs,
      parts_with_locs,
    }
  }

  /// Gets the *current* preferred locations from the DAGScheduler (as opposed to the static ones).
  fn current_pref_locs<T: Data>(part: Box<dyn Split>, prev: &dyn Rdd<Item = T>) -> Vec<Ipv4Addr> {
    //TODO: this is inefficient and likely to happen in more places,
    //we should add a preferred_locs method that takes split by ref (&dyn Split) not by value
    prev.preferred_locations(part)
  }
}

struct DefaultPartitionCoalescer {
  balance_slace: f64,
  /// keep it deterministic
  rnd: rand_pcg::Pcg64,
  /// each element of group arr represents one coalesced partition
  group_arr: Vec<Rc<RefCell<PartitionGroup>>>,
  /// hash used to check whether some machine is already in group_arr
  group_hash: HashMap<Ipv4Addr, Vec<Rc<RefCell<PartitionGroup>>>>,
  /// hash used for the first max_partitions (to avoid duplicates)
  initial_hash: HashSet<SplitIdx>,
  /// true if no preferred_locations exists for parent RDD
  no_locality: bool,
}

impl DefaultPartitionCoalescer {
  fn new() -> Self {
    DefaultPartitionCoalescer {
      balance_slace: 0.10,
      rnd: utils::random::get_default_rng(),
      group_arr: Vec::new(),
      group_hash: HashMap::new(),
      initial_hash: HashSet::new(),
      no_locality: true,
    }
  }

  fn add_part_to_pgroup(&mut self, part: SplitIdx, pgroup: &mut PartitionGroup) -> bool {
    if !self.initial_hash.contains(&part) {
      pgroup.partitions.push(part); // already assign this element
      self.initial_hash.insert(part); // needed to avoid assigning partitions to multiple buckets
      true
    } else {
      false
    }
  }

  /// Gets the least element of the list associated with key in group_hash
  /// The returned PartitionGroup is the least loaded of all groups that represent the machine "key"
  fn get_least_group_hash(&self, key: Ipv4Addr) -> Option<Rc<RefCell<PartitionGroup>>> {
    let mut current_min: Option<Rc<RefCell<PartitionGroup>>> = None;
    if let Some(group) = self.group_hash.get(&key) {
      for g in group.as_slice() {
        if let Some(ref cmin) = current_min {
          if *cmin.borrow() > *g.borrow() {
            current_min = Some((*g).clone());
          }
        }
      }
    }
    current_min
  }

  /// Initializes targetLen partition groups. If there are preferred locations, each group
  /// is assigned a preferredLocation. This uses coupon collector to estimate how many
  /// preferredLocations it must rotate through until it has seen most of the preferred
  /// locations (2 * n log(n))
  ///
  /// # Arguments
  ///
  /// * target_len - the number of desired partition groups
  #[allow(clippy::map_entry)]
  fn setup_groups(&mut self, target_len: usize, partition_locs: PartitionLocations) {
    // deal with empty case, just create target_len partition groups with no preferred location
    if partition_locs.parts_with_locs.is_empty() {
      for i in 1..=target_len {
        self
          .group_arr
          .push(Rc::new(RefCell::new(PartitionGroup::new(None))))
      }
      return;
    }

    self.no_locality = false;

    // number of iterations needed to be certain that we've seen most preferred locations
    let expected_coupons_2 = {
      let target_len = target_len as f64;
      2 * (target_len.ln() * target_len + target_len + 0.5f64) as u64
    };

    let mut num_created = 0;
    let mut tries = 0;

    // rotate through until either target_len unique/distinct preferred locations have been created
    // OR (we have went through either all partitions OR we've rotated expected_coupons_2 - in
    // which case we have likely seen all preferred locations)
    let num_parts_to_look_at = expected_coupons_2.min(partition_locs.parts_with_locs.len() as u64);
    while num_created < target_len as u64 && tries < num_parts_to_look_at {
      let (nxt_replica, nxt_part) = partition_locs.parts_with_locs[tries as usize];
      tries += 1;

      if !self.group_hash.contains_key(&nxt_replica) {
        let mut pgroup = PartitionGroup::new(Some(nxt_replica));
        self.add_part_to_pgroup(nxt_part, &mut pgroup);
        self
          .group_hash
          .insert(nxt_replica, vec![Rc::new(RefCell::new(pgroup))]); // list in case we have multiple
        num_created += 1;
      }
    }

    // if we don't have enough partition groups, create duplicates
    while num_created < target_len as u64 {
      // Copy the preferred location from a random input partition.
      // This helps in avoiding skew when the input partitions are clustered by preferred location.
      let (nxt_replica, nxt_part) =
        partition_locs.parts_with_locs[self.rnd.gen_range(0, 0) as usize];
      let mut pgroup = Rc::new(RefCell::new(PartitionGroup::new(Some(nxt_replica))));
      self.add_part_to_pgroup(nxt_part, &mut *pgroup.borrow_mut());
      self
        .group_hash
        .entry(nxt_replica)
        .or_insert_with(Vec::new)
        .push(pgroup.clone());
      self.group_arr.push(pgroup);
      num_created += 1;
    }
  }

  fn pick_bin(
    p: &dyn Split,
    prev: &dyn RddBase,
    balance_slace: f64,
    partition_locs: PartitionLocations,
  ) -> PartitionGroup {
    /**
     * Takes a parent RDD partition and decides which of the partition groups to put it in
     * Takes locality into account, but also uses power of 2 choices to load balance
     * It strikes a balance between the two using the balanceSlack variable
     * @param p partition (ball to be thrown)
     * @param balanceSlack determines the trade-off between load-balancing the partitions sizes and
     *                     their locality. e.g., balanceSlack=0.10 means that it allows up to 10%
     *                     imbalance in favor of locality
     * @return partition group (bin to be put in)
    def pickBin(
        p: Partition,
        prev: RDD[_],
        balanceSlack: Double,
        partitionLocs: PartitionLocations): PartitionGroup = {
      val slack = (balanceSlack * prev.partitions.length).toInt
      // least loaded pref locs
      val pref = currPrefLocs(p, prev).flatMap(getLeastGroupHash)
      val prefPart = if (pref.isEmpty) None else Some(pref.min)
      val r1 = rnd.nextInt(groupArr.size)
      val r2 = rnd.nextInt(groupArr.size)
      val minPowerOfTwo = {
        if (groupArr(r1).numPartitions < groupArr(r2).numPartitions) {
          groupArr(r1)
        }
        else {
          groupArr(r2)
        }
      }
      if (prefPart.isEmpty) {
        // if no preferred locations, just use basic power of two
        return minPowerOfTwo
      }

      val prefPartActual = prefPart.get

      // more imbalance than the slack allows
      if (minPowerOfTwo.numPartitions + slack <= prefPartActual.numPartitions) {
        minPowerOfTwo  // prefer balance over locality
      } else {
        prefPartActual // prefer locality over balance
      }
    }
    */
    unimplemented!()
  }
}

/*
private class DefaultPartitionCoalescer(val balanceSlack: Double = 0.10)
  extends PartitionCoalescer {
  def throwBalls(
      maxPartitions: Int,
      prev: RDD[_],
      balanceSlack: Double, partitionLocs: PartitionLocations): Unit = {
    if (noLocality) {  // no preferredLocations in parent RDD, no randomization needed
      if (maxPartitions > groupArr.size) { // just return prev.partitions
        for ((p, i) <- prev.partitions.zipWithIndex) {
          groupArr(i).partitions += p
        }
      } else { // no locality available, then simply split partitions based on positions in array
        for (i <- 0 until maxPartitions) {
          val rangeStart = ((i.toLong * prev.partitions.length) / maxPartitions).toInt
          val rangeEnd = (((i.toLong + 1) * prev.partitions.length) / maxPartitions).toInt
          (rangeStart until rangeEnd).foreach{ j => groupArr(i).partitions += prev.partitions(j) }
        }
      }
    } else {
      // It is possible to have unionRDD where one rdd has preferred locations and another rdd
      // that doesn't. To make sure we end up with the requested number of partitions,
      // make sure to put a partition in every group.

      // if we don't have a partition assigned to every group first try to fill them
      // with the partitions with preferred locations
      val partIter = partitionLocs.partsWithLocs.iterator
      groupArr.filter(pg => pg.numPartitions == 0).foreach { pg =>
        while (partIter.hasNext && pg.numPartitions == 0) {
          var (_, nxt_part) = partIter.next()
          if (!initialHash.contains(nxt_part)) {
            pg.partitions += nxt_part
            initialHash += nxt_part
          }
        }
      }

      // if we didn't get one partitions per group from partitions with preferred locations
      // use partitions without preferred locations
      val partNoLocIter = partitionLocs.partsWithoutLocs.iterator
      groupArr.filter(pg => pg.numPartitions == 0).foreach { pg =>
        while (partNoLocIter.hasNext && pg.numPartitions == 0) {
          val nxt_part = partNoLocIter.next()
          if (!initialHash.contains(nxt_part)) {
            pg.partitions += nxt_part
            initialHash += nxt_part
          }
        }
      }

      // finally pick bin for the rest
      for (p <- prev.partitions if (!initialHash.contains(p))) { // throw every partition into group
        pickBin(p, prev, balanceSlack, partitionLocs).partitions += p
      }
    }
  }

  def getPartitions: Array[PartitionGroup] = groupArr.filter( pg => pg.numPartitions > 0).toArray

  /**
   * Runs the packing algorithm and returns an array of PartitionGroups that if possible are
   * load balanced and grouped by locality
    *
    * @return array of partition groups
   */
  def coalesce(maxPartitions: Int, prev: RDD[_]): Array[PartitionGroup] = {
    val partitionLocs = new PartitionLocations(prev)
    // setup the groups (bins)
    setupGroups(math.min(prev.partitions.length, maxPartitions), partitionLocs)
    // assign partitions (balls) to each group (bins)
    throwBalls(maxPartitions, prev, balanceSlack, partitionLocs)
    getPartitions
  }
}
*/

/*
private[spark] class CoalescedRDD[T: ClassTag](
    @transient var prev: RDD[T],
    maxPartitions: Int,
    partitionCoalescer: Option[PartitionCoalescer] = None)
  extends RDD[T](prev.context, Nil) {  // Nil since we implement getDependencies

  require(maxPartitions > 0 || maxPartitions == prev.partitions.length,
    s"Number of partitions ($maxPartitions) must be positive.")
  if (partitionCoalescer.isDefined) {
    require(partitionCoalescer.get.isInstanceOf[Serializable],
      "The partition coalescer passed in must be serializable.")
  }

  override def getPartitions: Array[Partition] = {
    val pc = partitionCoalescer.getOrElse(new DefaultPartitionCoalescer())

    pc.coalesce(maxPartitions, prev).zipWithIndex.map {
      case (pg, i) =>
        val ids = pg.partitions.map(_.index).toArray
        CoalescedRDDPartition(i, prev, ids, pg.prefLoc)
    }
  }

  override def compute(partition: Partition, context: TaskContext): Iterator[T] = {
    partition.asInstanceOf[CoalescedRDDPartition].parents.iterator.flatMap { parentPartition =>
      firstParent[T].iterator(parentPartition, context)
    }
  }

  override def getDependencies: Seq[Dependency[_]] = {
    Seq(new NarrowDependency(prev) {
      def getParents(id: Int): Seq[Int] =
        partitions(id).asInstanceOf[CoalescedRDDPartition].parentsIndices
    })
  }

  override def clearDependencies(): Unit = {
    super.clearDependencies()
    prev = null
  }

  /**
   * Returns the preferred machine for the partition. If split is of type CoalescedRDDPartition,
   * then the preferred machine will be one which most parent splits prefer too.
   * @param partition the partition for which to retrieve the preferred machine, if exists
   * @return the machine most preferred by split
   */
  override def getPreferredLocations(partition: Partition): Seq[String] = {
    partition.asInstanceOf[CoalescedRDDPartition].preferredLocation.toSeq
  }
}
*/

impl<T: Data> PartitionCoalescer<T> for DefaultPartitionCoalescer {
  fn coalesce(max_partitions: usize, parent: Box<dyn Rdd<Item = T>>) -> Vec<PartitionGroup> {
    unimplemented!()
  }
}
