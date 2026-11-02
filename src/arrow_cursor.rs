use std::ops::DerefMut;

use arrow_array::RecordBatch;
use questdb::egress::Cursor;
use self_cell::{MutBorrow, self_cell};

use crate::error;
use crate::qwp_connection::ReaderGuard;

// A QWP Cursor mutably borrows its Reader for the whole query, like the
// PostgreSQL ADBC driver's TupleReader holds its borrowed PGconn. Keeping the
// cursor and its checkout-guard in one owned, movable cell lets the cursor be
// driven from a `Send + 'static` result without a worker thread.
self_cell!(
    struct ActiveQuery {
        owner: MutBorrow<ReaderGuard>,

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
    pub(crate) fn execute(guard: ReaderGuard, sql: String) -> adbc_core::error::Result<Self> {
        let active = ActiveQuery::try_new(MutBorrow::new(guard), |owner| {
            owner.borrow_mut().deref_mut().execute(sql)
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
}
