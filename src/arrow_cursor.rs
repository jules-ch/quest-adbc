use arrow_array::RecordBatch;
use questdb::egress::{Cursor, Terminal};
use questdb::ffi_support::OwnedReader;
use self_cell::{MutBorrow, self_cell};

use crate::error;

// A QWP Cursor mutably borrows its Reader for the whole query, like the
// PostgreSQL ADBC driver's TupleReader holds its borrowed PGconn. The pool
// hands out an owned reader lease; keeping that lease and the borrowed cursor
// in one movable cell lets the cursor be driven from a `Send + 'static`
// result without a worker thread.
self_cell!(
    struct ActiveQuery {
        owner: MutBorrow<OwnedReader>,

        #[not_covariant]
        dependent: Cursor,
    }
);

/// Owned, movable handle to one in-flight QWP query. Wraps the self-referential
/// pair of a checked-out reader and its borrowed cursor so callers can hold it
/// without managing lifetimes themselves.
pub(crate) struct ArrowCursor {
    active: ActiveQuery,
}

impl ArrowCursor {
    pub(crate) fn execute(reader: OwnedReader, sql: String) -> adbc_core::error::Result<Self> {
        let active = ActiveQuery::try_new(MutBorrow::new(reader), |owner| {
            owner.borrow_mut().get_mut().execute(sql)
        })
        .map_err(error::from_questdb)?;
        Ok(Self { active })
    }

    pub(crate) fn next_batch(&mut self) -> adbc_core::error::Result<Option<RecordBatch>> {
        self.active
            .with_dependent_mut(|_, cursor| cursor.next_arrow_batch())
            .map_err(error::from_questdb)
    }

    pub(crate) fn cancel(&mut self) -> adbc_core::error::Result<()> {
        self.active
            .with_dependent_mut(|_, cursor| cursor.cancel())
            .map_err(error::from_questdb)
    }

    /// The terminal frame observed after the last batch was drained.
    pub(crate) fn terminal(&self) -> Option<Terminal> {
        self.active
            .with_dependent(|_, cursor| cursor.terminal().cloned())
    }
}
