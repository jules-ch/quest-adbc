use std::sync::atomic::{AtomicBool, Ordering};

/// Cancellation token for one execution, shared with its result reader, the
/// native connection, and the owning statement. A token must not be reused
/// for a later execution.
#[derive(Default)]
pub(crate) struct CancellationToken {
    inner: AtomicBool,
}

impl CancellationToken {
    pub(crate) fn cancel(&self) {
        self.inner.store(true, Ordering::Release);
    }

    pub(crate) fn is_cancelled(&self) -> bool {
        self.inner.load(Ordering::Acquire)
    }
}
