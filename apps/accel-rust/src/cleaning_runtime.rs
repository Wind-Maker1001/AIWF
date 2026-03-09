use crate::api_types::{
    CleanRow, CleaningOutputs, CleaningReq, CleaningResp, ComputeMetrics, ComputeReq, ComputeResp,
    FileOut, OfficeGenInfo, ProfileOut,
};
use parquet::{
    basic::{Compression, Repetition, Type as PhysicalType},
    column::writer::ColumnWriter,
    file::{properties::WriterProperties, writer::SerializedFileWriter},
    schema::types::Type,
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    env, fs,
    io::{Read, Write},
    path::{Path, PathBuf},
    process::Command,
    sync::Arc,
};

mod clean_rows;
mod compute;
mod office;
mod outputs;
mod runtime;

pub(crate) use clean_rows::load_and_clean_rows;
pub(crate) use compute::run_compute_metrics;
pub(crate) use outputs::write_cleaned_parquet;
pub(crate) use runtime::run_cleaning_operator;
