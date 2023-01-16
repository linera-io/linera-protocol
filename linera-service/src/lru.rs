use lru_time_cache::LruCache;
use std::time::Duration;

pub struct LruSet<T> {
    inner: LruCache<T, ()>,
}

impl<T: Ord + Clone> LruSet<T> {
    /// Constructor for capacity based `LruSet`.
    pub fn with_capacity(capacity: usize) -> LruSet<T> {
        Self {
            inner: LruCache::with_capacity(capacity),
        }
    }

    /// Constructor for time based `LruSet`.
    pub fn with_expiry_duration(time_to_live: Duration) -> LruSet<T> {
        Self {
            inner: LruCache::with_expiry_duration(time_to_live),
        }
    }

    /// Constructor for dual-feature capacity and time based `LruSet`.
    pub fn with_expiry_duration_and_capacity(time_to_live: Duration, capacity: usize) -> LruSet<T> {
        Self {
            inner: LruCache::with_expiry_duration_and_capacity(time_to_live, capacity),
        }
    }

    /// Adds a value to the set.
    ///
    /// If the set did not have this value present, `true` is returned.
    ///
    /// If the set did have this value present, `false` is returned.
    pub fn insert(&mut self, t: T) -> bool {
        self.inner.insert(t, ()).is_none()
    }

    /// If the set did not have this value present, `false` is returned.
    ///
    /// If the set did have this value present, `true` is returned.
    pub fn contains(&self, t: &T) -> bool {
        self.inner.contains_key(t)
    }
}

#[cfg(test)]
pub mod tests {
    use std::time::Duration;
    use crate::lru::LruSet;

    #[test]
    fn test_insert() {
        let mut lru = LruSet::with_capacity(3);

        assert!(lru.insert(1));
        assert!(!lru.insert(1));

        assert!(lru.insert(2));
        assert!(!lru.insert(2));
        assert!(!lru.insert(1));

        assert!(lru.insert(3));
        assert!(!lru.insert(3));
        assert!(!lru.insert(2));
        assert!(!lru.insert(1));

        assert!(lru.insert(4));

        // 1 should be evicted
        assert!(!lru.insert(1));
    }

    #[test]
    fn test_contains() {
        let mut lru = LruSet::with_capacity(3);

        lru.insert(1);
        assert!(lru.contains(&1));

        lru.insert(2);
        assert!(lru.contains(&1));
        assert!(lru.contains(&2));

        lru.insert(3);
        assert!(lru.contains(&1));
        assert!(lru.contains(&2));
        assert!(lru.contains(&3));

        lru.insert(4);
        assert!(lru.contains(&2));
        assert!(lru.contains(&3));
        assert!(lru.contains(&4));

        // 1 should be evicted.
        assert!(!lru.contains(&1));
    }

    #[test]
    fn test_duration() {
        let mut lru = LruSet::with_expiry_duration(Duration::from_millis(100));
        lru.insert(1);
        assert!(lru.contains(&1));
        std::thread::sleep(Duration::from_millis(110));
        assert!(!lru.contains(&1));
    }
}
