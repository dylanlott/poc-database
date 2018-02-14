use std::collections::HashMap;

fn main() {

}

#[derive(Debug, Clone)]
enum Message {
    MessageShard {
        source_id: String,
        destination_id: String,
        payload: Shard,
    },
    MessageMetaShard {
        source_id: String,
        destination_id: String,
        payload: MetaShard,
    },
    MessageUserData {
        source_id: String,
        destination_id: String,
        payload: UserData
    },
    MessageAccumulation {
        source_id: String,
        destination_id: String,
        payload: LatticeAccumulator,
    },
    MessageReplicationState {
        source_id: String,
        destination_id: String,
        payload: LatticeReplication,
    },
}

#[derive(Debug, Clone)]
enum PeerTag {
    CDN,
    Grid,
    S3,
    Glaicer,
    NoTag,
}

#[derive(Debug, Clone, PartialEq)]
enum Partition {
    hex_0_7,
    hex_8_f,
    NoPartition,
}

// WHERE or filer
#[derive(Debug, Clone)]
enum DatumType {
    Shard,
    MetaShard,
    User,
    All,
}

// projection
#[derive(Debug, Clone)]
enum Select {
    Star,
    Datum { datum_id: String }, 
} 
    
// this is the key that is submitted to the network to store a datum/value
#[derive(Debug, Clone)]
struct MetaKey {
    // pub key AND with WHERE
    user_id: String,
    //meta data for data partition
    datum_type: DatumType,
    //data that is the value to this key, gen by user
    datum_id: Select,
}

// Peer type
#[derive(Debug, Clone)]
struct Prefix {
    partition0: Partition,
    partition1: Partition,
    peer_tag: PeerTag,
}

// this is the result of storing a Meta key stored on the user
#[derive(Debug, Clone)]
struct PreFixMetaKey {
    // uuid
    meta_key_id: String,
    // FROM ?node id vs prefix vs prefix subset?
    prefix: Prefix,
    meta_key: MetaKey,
}



//users perspective of shards
#[derive(Debug, Clone)]
struct File {
    // user
    node_id: String,
    file_name: String,
    contents: String,
}

#[derive(Debug, Clone)]
struct Shard {
    // uuid
    shard_id: String,
    shard_data: String,
    shard_accumulator: u32,
    shard_accumulator_limit: u32,
    shard_replication_level: u32,
}

//data about group of shards (file)
#[derive(Debug, Clone)]
struct MetaShard {
    // uuid
    meta_shard_id: String,
    // lists of shards that make a file
    shard_ids: Vec<String>,
    // for updates/ cache invalidation
    meta_shard_version: u32,
    // crdt counter
    shard_accumulator_max: u32,
    // for ttl delete
    shard_accumulator_limit: u32,
    // state based crdt
    shard_replication_level: u32,
    // state of replication
    shard_replication_level_state: ReplicaState,
}

// meta data about the user
#[derive(Debug, Clone)]
struct UserData {
    //pub key
    user_id: String,
    files: HashMap<String, String>,
}

#[derive(Debug, Clone)]
enum ReplicaState {
    ReplicaInPartition,
    ReplicaNotInPartition,
    ReplicationMeet,
    ReplicationNeeded,
    ReplicationExcess,
    ReplicationUnknown,
}

#[derive(Debug, Clone)]
enum AccumlationState {
    count { num: u32 },
    max_exceeded,
}




trait user {
    fn upload(file: File) -> ReplicaState;
    fn mkfile(name: String, data: String) -> File;
}

trait farmer {
    fn replicate(shard: Shard) -> ReplicaState;
    fn add_peer(&mut self) -> ();
    fn update_peer(&mut self, peer_id: String) -> ();
    fn find_shard(&self, shard_id: String) -> Shard;
    fn find_meta_shard(&self, meta_shard_id: String) -> MetaShard;
    fn find_user_data(&self, pub_key: String) -> UserData;
}

#[derive(Debug, Clone)]
struct Node {
    node_msgs: Vec<Message>,
    node_id: String,
    node_address: String,
    node_port: String,
    node_space_available: String,
    node_partition: (Partition, Partition),
    node_peers: HashMap<String, Peer>,

    user_data: UserData,
    user_keys: Vec<PreFixMetaKey>,

    farmer_shards: HashMap<String, Shard>,
}

impl Node {
    fn eval_key(shard_id: String) -> (Partition, Partition) {
        let first_dimension = shard_id.get(0..1).unwrap();
        let second_dimension = shard_id.get(1..2).unwrap();

        //hexadecimal 2 dimesional partition
        let zero_seven = vec![
            "0", "1", "2", "3", 
            "4", "5", "6", "7", 
        ];

        let eight_f = vec![
            "8", "9", "a", "b",
            "c", "d", "e", "f",
        ];

        match first_dimension {
            test0 if zero_seven.contains(&test0) => {
                match second_dimension {
                    test1 if eight_f.contains(&test1) => {
                        (Partition::hex_0_7, Partition::hex_8_f)
                    },
                    _ => {
                        (Partition::hex_0_7, Partition::hex_0_7)
                    }
                }
            },
            test0 if eight_f.contains(&test0) => {
                match second_dimension {
                    test1 if eight_f.contains(&test1) => {
                        (Partition::hex_8_f, Partition::hex_8_f)
                    },
                    _ => {
                        (Partition::hex_8_f, Partition::hex_0_7)
                    }
                }
            },
            _ => {
                (Partition::NoPartition, Partition::NoPartition)
            },
        }
    }
}

#[derive(Debug, Clone)]
struct Peer {
    peer_id: String,
    peer_ip: String,
    peer_port: String,
    peer_meta: Prefix,
}




#[derive(Debug, Clone)]
struct LatticeReplication {
    // id of shard or meta shard
    datum_id: String,
    // min level for replication to be satisfied
    replication_level: u32,
    // current state of replication
    atom: HashMap<String, ReplicaState>,
}

impl LatticeReplication {
    fn apply(node_id: String, datum_id: String, replication_level: u32, node_partition: (Partition, Partition)) -> LatticeReplication {
        let mut new_atom: HashMap<String, ReplicaState> = HashMap::new();

        let partition_eval = Node::eval_key(datum_id.clone());

        let state: ReplicaState = if (partition_eval.0 == node_partition.0) && (partition_eval.1 == node_partition.1) {
            ReplicaState::ReplicaInPartition
        } else {
            ReplicaState::ReplicaNotInPartition
        };

        new_atom.insert(node_id, state);

        LatticeReplication {
            datum_id: datum_id,
            replication_level: replication_level,
            atom: new_atom,
        }
    }
}

#[derive(Debug, Clone)]
struct LatticeAccumulator { 
    // id of shard or meta shard
    datum_id: String,
    // max count for data
    accumulator_limit: u32,
    // current state of the count
    atom: HashMap<String, AccumlationState>,
}

impl LatticeAccumulator {
    fn apply(node_id: String, datum_id: String, accumulator_limit: u32) -> LatticeAccumulator {
        let mut new_atom: HashMap<String, AccumlationState> = HashMap::new();
        new_atom.insert(node_id, AccumlationState::count{ num: 0 });

        LatticeAccumulator {
            datum_id: datum_id,
            accumulator_limit: accumulator_limit,
            atom: new_atom,
        } 
    }
}

trait CvRDT<T, U> {
    fn join(self, other_atom: HashMap<String, T>) -> U;
    fn compare(&self, delta: (&String, &T)) -> (String, T);
    fn state(&self) -> T;
}

impl CvRDT<ReplicaState, LatticeReplication> for LatticeReplication {
    fn join(self, other_atom: HashMap<String, ReplicaState>) -> LatticeReplication {
        let mut update_atom = self.atom.clone();

        for (node_id, state) in other_atom.iter() {
            let (new_node, new_state) = self.compare((&node_id, &state));
            update_atom.insert(new_node, new_state);
        }

        LatticeReplication {
            datum_id: self.datum_id,
            replication_level: self.replication_level,
            atom: update_atom,
        }
    }

    fn compare(&self, delta: (&String, &ReplicaState)) -> (String, ReplicaState) {
        let node_id: String = delta.0.clone();
        let other_state = delta.1.clone();

        let res: (String, ReplicaState) = match self.atom.get(&node_id) {
            Some(state) => {
                let old_state: ReplicaState = state.clone();
                let new_state: (String, ReplicaState) = match old_state {
                    ReplicaState::ReplicaInPartition | ReplicaState::ReplicaNotInPartition => {
                        (node_id, old_state)
                    },
                    _ => {
                        (node_id, other_state)
                    },
                };

                new_state
            },
            None => {
                (node_id, other_state)
            },
        };

        res
    }

    fn state(&self) -> ReplicaState {
        let lattice = &self.atom;
        let mut local_replication_level = 0;

        for (node_id, state) in lattice.iter() {
            let local_state = state.clone();
            match local_state {
                ReplicaState::ReplicaInPartition | ReplicaState::ReplicaNotInPartition => {
                    local_replication_level = local_replication_level + 1;
                },
                _ => {
                    ()
                },
            }
        }

        match local_replication_level {
            level if level == self.replication_level => ReplicaState::ReplicationMeet,
            level if level > self.replication_level => ReplicaState::ReplicationExcess,
            _ => ReplicaState::ReplicationNeeded,
        }
    }
}


impl CvRDT<AccumlationState, LatticeAccumulator> for LatticeAccumulator {
    fn join(self, other_atom: HashMap<String, AccumlationState>) -> LatticeAccumulator {
        let mut update_atom = self.atom.clone();

        for (node_id, state) in other_atom.iter() {
            let (new_node, new_state) = self.compare((&node_id, &state));
            update_atom.insert(new_node, new_state);
        }

        LatticeAccumulator {
            datum_id: self.datum_id,
            accumulator_limit: self.accumulator_limit,
            atom: update_atom,
        }
    }

    fn compare(&self, delta: (&String, &AccumlationState)) -> (String, AccumlationState) {
        let node_id: String = delta.0.clone();
        let other_state: AccumlationState = delta.1.clone();

        let res: (String, AccumlationState) = match self.atom.get(&node_id) {
            Some(state) => {
                let old_state: AccumlationState = state.clone();
                let new_state: (String, AccumlationState) = match old_state {
                    AccumlationState::count{num} => {
                        match other_state {
                            AccumlationState::count{num} => {
                                let new_num: u32 = num.max(num);
                                (node_id, AccumlationState::count{num: new_num})
                            },
                            _ => {
                                (node_id, AccumlationState::max_exceeded)
                            }
                        }
                    },
                    _ => {
                        (node_id, AccumlationState::max_exceeded)
                    },
                };

                new_state
            },
            _ => {
                (node_id, AccumlationState::max_exceeded)
            },
        };
        
        ("".to_string(), AccumlationState::max_exceeded)
    }

    fn state(&self) -> AccumlationState {
        let lattice = &self.atom;
        let mut max_met = false;
        let mut local_accumlator = 0;

        for v in lattice.values() {
            match v {
                &AccumlationState::max_exceeded => {
                    max_met = true;
                },
                _ => (),
            }
        }

        if max_met {
            AccumlationState::max_exceeded
        } else {
            for (node_id, acc) in lattice.iter() {
                let local_acc = acc.clone();
                match local_acc {
                    AccumlationState::count{num} => {
                        local_accumlator = local_accumlator + num;
                    },
                    _ => { () },
                }
            }
            
            AccumlationState::count{ num: local_accumlator }
        }
    }
}
