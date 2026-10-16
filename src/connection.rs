use std::ops::{Deref, DerefMut};
use std::sync::{Arc, Mutex};

use adbc_core::error::{Error, Result, Status};
use questdb::egress::Reader;

use crate::cancellation::CancellationToken;

/// The single native QWP connection owned by one logical ADBC connection —
/// the Rust analog of the PostgreSQL ADBC driver's `PGconn* conn_`. QWP,
/// like libpq, carries at most one query in flight, so the socket stays
/// healthy across queries and reconnects only after a transport failure.
pub(crate) struct NativeConnection {
    conf: String,
    state: Mutex<ConnectionState>,
}

#[derive(Default)]
struct ConnectionState {
    reader: Option<Reader>,
    busy: bool,
    /// The in-flight query's cancellation register. Because a single reader
    /// can carry at most one query, `Connection::cancel` only ever has to
    /// reach this one.
    in_flight: Option<Arc<CancellationToken>>,
}

impl NativeConnection {
    pub(crate) fn new(conf: String) -> Self {
        Self {
            conf,
            state: Mutex::new(ConnectionState::default()),
        }
    }

    /// `Connection::cancel`: marks the in-flight query of this connection,
    /// if any. A query started after this call replaces the slot and runs
    /// unharmed — the same semantics as marking a live-query list would
    /// have.
    pub(crate) fn cancel_in_flight(&self) {
        let state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        if let Some(query) = state.in_flight.as_ref() {
            query.cancel();
        }
    }

    /// Check out the connection's reader for one in-flight query (the analog
    /// of the PG driver's reader occupying `conn_` while its cursor is
    /// active). A second checkout is refused with `InvalidState` while the
    /// first result is still active. Dropping the guard returns a healthy
    /// reader to the slot; a torn-down transport is discarded so the next
    /// checkout reconnects, mirroring libpq's reset behavior. The socket is
    /// opened lazily on the first query.
    pub(crate) fn acquire(
        self: &Arc<Self>,
        cancellation: Arc<CancellationToken>,
    ) -> Result<ReaderGuard> {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        if state.busy {
            return Err(Error::with_message_and_status(
                "another result is active on this QuestDB connection",
                Status::InvalidState,
            ));
        }

        // The reconnect path performs network I/O, but only when no healthy
        // connection is stored; holding the mutex keeps the one-at-a-time
        // invariant panic-safe even if the connect attempt unwinds.
        let reused = state
            .reader
            .take()
            .filter(|reader| !reader.transport_torn_down());
        let reader = match reused {
            Some(reader) => reader,
            None => Reader::from_conf(&self.conf)
                .map_err(|error| Error::with_message_and_status(error.to_string(), Status::IO))?,
        };
        state.busy = true;
        state.in_flight = Some(cancellation);
        Ok(ReaderGuard {
            reader: Some(reader),
            connection: Arc::clone(self),
        })
    }
}

/// Exclusive checkout of the connection's native reader until one query
/// finishes or is abandoned. Guards the slot: dropping it (also on panic or
/// unwind) puts a healthy reader back, frees the busy flag, and forgets the
/// query it belonged to.
pub(crate) struct ReaderGuard {
    reader: Option<Reader>,
    /// Back-reference driving the return trip.
    connection: Arc<NativeConnection>,
}

impl Deref for ReaderGuard {
    type Target = Reader;

    fn deref(&self) -> &Self::Target {
        self.reader.as_ref().expect("reader checked out")
    }
}

impl DerefMut for ReaderGuard {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.reader.as_mut().expect("reader checked out")
    }
}

impl Drop for ReaderGuard {
    fn drop(&mut self) {
        let mut state = self
            .connection
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let reader = self.reader.take().expect("reader checked out");
        if !reader.transport_torn_down() {
            state.reader = Some(reader);
        }
        state.busy = false;
        state.in_flight = None;
    }
}
