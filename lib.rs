//! This module is used to streamline event based analysis common in nuclear physics. The plotting
//! routines are left to the user, but this crate can be used to generate the data to be plotted.
//! This crate focusses on performance and flexibility for large datasets (up to ~250 GB) on small
//! systems (like mid-level laptop). 
//!
//! This crate should be used as an analysis workflow.
//! This crate uses this `rkyv` crate heavily under the hood. One should look at the `rkyv`
//! documentation for more details, but it provides very fast binary serialization and
//! deserialization through its `Archived` types. The details of how this works are not important.
//! What is important is that any event type that seeks to be handled by this crate needs to derive
pub mod anl_module;
pub mod data_set;
