use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Weak};

/// One execution's cancellation request, shared with its result reader and
/// statement. A request must not be reused for a later execution.
#[derive(Default)]
pub(crate) struct QueryCancellation(AtomicBool);

impl QueryCancellation {
    pub(crate) fn request(&self) {
        self.0.store(true, Ordering::Release);
    }

    pub(crate) fn requested(&self) -> bool {
        self.0.load(Ordering::Acquire)
    }
}

/// Tracks executions belonging to one logical ADBC connection. Weak entries
/// do not keep finished queries or their results alive.
#[derive(Default)]
pub(crate) struct CancellationRegistry(Mutex<Vec<Weak<QueryCancellation>>>);

impl CancellationRegistry {
    pub(crate) fn register(&self, query: &Arc<QueryCancellation>) {
        let mut active = self.0.lock().unwrap_or_else(|error| error.into_inner());
        active.retain(|query| query.strong_count() > 0);
        active.push(Arc::downgrade(query));
    }

    pub(crate) fn cancel_all(&self) {
        let mut active = self.0.lock().unwrap_or_else(|error| error.into_inner());
        active.retain(|query| {
            if let Some(query) = query.upgrade() {
                query.request();
                true
            } else {
                false
            }
        });
    }
}
