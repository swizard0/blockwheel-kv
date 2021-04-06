use std::{
    collections::{
        hash_map,
        HashMap,
        HashSet,
    },
};

pub struct BinMerger<B> {
    powers: HashMap<usize, Vec<B>>,
    need_merge: HashSet<usize>,
}

impl<B> BinMerger<B> {
    pub fn new() -> BinMerger<B> {
        BinMerger {
            powers: HashMap::new(),
            need_merge: HashSet::new(),
        }
    }

    pub fn push(&mut self, bucket: B, items_count: usize) {
        let power_of_2 = items_count.next_power_of_two();
        match self.powers.entry(power_of_2) {
            hash_map::Entry::Vacant(ve) => {
                ve.insert(vec![bucket]);
            },
            hash_map::Entry::Occupied(mut oe) => {
                let powers = oe.get_mut();
                powers.push(bucket);
                if powers.len() >= 2 {
                    self.need_merge.insert(power_of_2);
                }
            },
        }
    }

    pub fn pop(&mut self) -> Option<(B, B)> {
        let power_of_2 = self.need_merge.iter().next().cloned()?;
        let powers = self.powers.get_mut(&power_of_2).unwrap();
        let bucket_a = powers.pop().unwrap();
        let bucket_b = powers.pop().unwrap();
        if powers.len() < 2 {
            self.need_merge.remove(&power_of_2);
        }
        Some((bucket_a, bucket_b))
    }
}

#[cfg(test)]
mod tests {
    use rand::Rng;

    use super::BinMerger;

    struct Bucket {
        count: usize,
    }

    #[test]
    fn fill() {
        let mut bin_merger = BinMerger::new();
        let mut rng = rand::thread_rng();

        let mut total_count = 0;
        for _ in 0 .. 65536 {
            let count = rng.gen_range(0 .. 1024);
            let bucket = Bucket { count, };

            bin_merger.push(bucket, count);
            total_count += count;
        }

        while let Some((bucket_a, bucket_b)) = bin_merger.pop() {
            let count = bucket_a.count + bucket_b.count;
            let bucket = Bucket { count, };
            bin_merger.push(bucket, count);
        }

        let mut total_count_check = 0;
        for buckets in bin_merger.powers.values() {
            if buckets.is_empty() {
                continue;
            }
            assert_eq!(buckets.len(), 1);
            total_count_check += buckets[0].count;
        }
        assert_eq!(total_count_check, total_count);
        assert!(bin_merger.need_merge.is_empty());
    }
}
