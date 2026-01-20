pub use crate::db::DBError as BoxKVError;
pub type BoxKVResult<T> = std::result::Result<T, BoxKVError>;
