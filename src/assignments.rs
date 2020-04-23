use crate::{client_type_error, RedisGraphResult, ResultSet};

/// Implemented by types that can be constructed from a [`ResultSet`](../result_set/struct.ResultSet.html).
pub trait FromTable: Sized {
    fn from_table(result_set: &ResultSet) -> RedisGraphResult<Self>;
}

/// Implemented by types that can be constructed from a row in a [`ResultSet`](../result_set/struct.ResultSet.html).
pub trait FromRow: Sized {
    fn from_row(result_set: &ResultSet, row_idx: usize) -> RedisGraphResult<Self>;
}

/// Implemented by types that can be constructed from a cell in a [`ResultSet`](../result_set/struct.ResultSet.html).
pub trait FromCell: Sized {
    fn from_cell(
        result_set: &ResultSet,
        row_idx: usize,
        column_idx: usize,
    ) -> RedisGraphResult<Self>;
}

impl FromTable for ResultSet {
    fn from_table(result_set: &ResultSet) -> RedisGraphResult<Self> {
        Ok(result_set.clone())
    }
}

impl<T: FromRow> FromTable for Vec<T> {
    fn from_table(result_set: &ResultSet) -> RedisGraphResult<Self> {
        let num_rows = result_set.num_rows();
        let mut ret = Self::with_capacity(num_rows);

        for i in 0..num_rows {
            ret.push(T::from_row(result_set, i)?);
        }

        Ok(ret)
    }
}

// Altered version of https://github.com/mitsuhiko/redis-rs/blob/master/src/types.rs#L1080
macro_rules! impl_row_for_tuple {
    () => ();
    ($($name:ident,)+) => (
        #[doc(hidden)]
        impl<$($name: FromCell),*> FromRow for ($($name,)*) {
            // we have local variables named T1 as dummies and those
            // variables are unused.
            #[allow(non_snake_case, unused_variables, clippy::eval_order_dependence)]
            fn from_row(result_set: &ResultSet, row_idx: usize) -> RedisGraphResult<($($name,)*)> {
                // hacky way to count the tuple size
                let mut n = 0;
                $(let $name = (); n += 1;)*
                if result_set.num_columns() != n {
                    return client_type_error!(
                        "failed to construct tuple: tuple has {:?} entries but result table has {:?} columns",
                        n,
                        result_set.num_columns()
                    );
                }

                // this is pretty ugly too. The { i += 1; i - 1 } is rust's
                // postfix increment :)
                let mut i = 0;
                Ok(($({let $name = (); $name::from_cell(result_set, row_idx, { i += 1; i - 1 })?},)*))
            }
        }
        impl_row_for_tuple_peel!($($name,)*);
    )
}

/// This chips of the leading one and recurses for the rest.  So if the first
/// iteration was T1, T2, T3 it will recurse to T2, T3.  It stops for tuples
/// of size 1 (does not implement down to unit).
macro_rules! impl_row_for_tuple_peel {
    ($name:ident, $($other:ident,)*) => (impl_row_for_tuple!($($other,)*);)
}

impl_row_for_tuple! { T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, }

// Row and column indices default to zero for lower-level values
impl<T: FromCell> FromRow for T {
    fn from_row(result_set: &ResultSet, row_idx: usize) -> RedisGraphResult<Self> {
        T::from_cell(result_set, row_idx, 0)
    }
}

impl<T: FromRow> FromTable for T {
    fn from_table(result_set: &ResultSet) -> RedisGraphResult<Self> {
        T::from_row(result_set, 0)
    }
}
