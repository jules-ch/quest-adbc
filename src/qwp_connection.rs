use std::sync::{Arc, Mutex, Weak};

use adbc_core::error::Result;
use arrow_array::RecordBatch;
use questdb::QuestDb;
// `OwnedReader` is in questdb-rs's FFI escape hatch: the stable API only
// exposes lifetime-bound reader borrows. Pin questdb-rs exactly in Cargo.toml
// so a minor release cannot change this surface unnoticed.
use questdb::ffi_support::borrow_reader_owned;

use crate::arrow_cursor::ArrowCursor;
use crate::cancellation::CancellationToken;
use crate::error;

/// The QWP pool owned by one logical ADBC connection. Both queries and
/// ingestion borrow from the same pool; `QuestDb` keeps its reader and sender
/// pools independent, so an open result set can never block an ingest flush.
pub(crate) struct QwpConnection {
    conf: String,
    /// Opened lazily on first use. `QuestDb` is `Send + Sync`, so the `Arc` is
    /// cloned out before any network I/O.
    pool: Mutex<Option<Arc<QuestDb>>>,
    /// Cancellation registers for queries currently checked out of the reader
    /// pool. A pooled connection may have several results in flight, so
    /// `Connection::cancel` marks all of them.
    in_flight: Mutex<Vec<Weak<CancellationToken>>>,
}

impl QwpConnection {
    pub(crate) fn new(conf: String) -> Self {
        Self {
            conf,
            pool: Mutex::new(None),
            in_flight: Mutex::new(Vec::new()),
        }
    }

    /// The shared QWP pool, created on first use from the connection's
    /// configuration. Cloning the `Arc` out before any borrow keeps network
    /// I/O out of the pool mutex.
    pub(crate) fn pool(&self) -> Result<Arc<QuestDb>> {
        let mut slot = self
            .pool
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(pool) = slot.as_ref() {
            return Ok(Arc::clone(pool));
        }
        let pool = Arc::new(QuestDb::connect(&self.conf).map_err(error::from_questdb)?);
        *slot = Some(Arc::clone(&pool));
        Ok(pool)
    }

    /// Borrow a reader from the pool and start one query on it. The returned
    /// cursor owns the reader lease, so it stays `Send + 'static` without
    /// holding a lock for the lifetime of the result stream.
    pub(crate) fn execute(
        &self,
        cancellation: Arc<CancellationToken>,
        sql: String,
    ) -> Result<ArrowCursor> {
        let pool = self.pool()?;
        let reader = borrow_reader_owned(&pool).map_err(error::from_questdb)?;
        self.register(&cancellation);
        ArrowCursor::execute(reader, sql)
    }

    /// Register a query for connection-level cancellation. Dead entries from
    /// finished statements are pruned here so the list stays bounded.
    fn register(&self, cancellation: &Arc<CancellationToken>) {
        let mut in_flight = self
            .in_flight
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        in_flight.retain(|query| query.strong_count() > 0);
        in_flight.push(Arc::downgrade(cancellation));
    }

    /// `Connection::cancel`: mark every query still in flight on this logical
    /// connection. Queries started after this call are unaffected.
    pub(crate) fn cancel_in_flight(&self) {
        let mut in_flight = self
            .in_flight
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        for query in in_flight.drain(..) {
            if let Some(query) = query.upgrade() {
                query.cancel();
            }
        }
    }

    /// Run one query to completion and return all of its Arrow batches. The
    /// metadata methods use this because they need the whole result in hand
    /// before they can build an ADBC response batch.
    pub(crate) fn query_batches(&self, sql: &str) -> Result<Vec<RecordBatch>> {
        let mut cursor = self.execute(Arc::new(CancellationToken::default()), sql.to_owned())?;
        let mut batches = Vec::new();
        loop {
            match cursor.next_batch()? {
                Some(batch) => batches.push(batch),
                None => return Ok(batches),
            }
        }
    }
}
