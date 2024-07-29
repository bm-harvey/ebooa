#![allow(dead_code)]
#![allow(unused_imports)]
use crate::data_set2;
use crate::data_set2::ArchivedDataCollectionIter;
//use crate::data_set2::DataCollectionIter;
//use crate::data_set2::LimitedArchivedDataCollectionIter;
use crate::data_set2::{DataFileCollection, DataSet, DataSetCollection};
use colored::Colorize;
use indicatif::ParallelProgressIterator;
//use indicatif::ProgressBar;
//use indicatif::ProgressIterator;
use memmap2::Mmap;
//use rayon::iter::ParallelBridge;
use rayon::iter::ParallelBridge;
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use rayon::prelude::*;
use rkyv::ser::serializers::{
    AlignedSerializer, AllocScratch, CompositeSerializer, FallbackScratch, HeapScratch,
    SharedSerializeMap,
};
use rkyv::{AlignedVec, Archive};
use rkyv::{Deserialize, Serialize};
use std::fs::create_dir;
use std::io::BufWriter;
use std::io::Write;
use std::sync::{Arc, Mutex, RwLock};
use std::{
    fs::File,
    path::{Path, PathBuf},
};
use thousands::Separable;

type A<E> = <E as Archive>::Archived;

/// The generic form of an event based analysis module. An `Analysis` or `MixedAnalysis` can take in one or more of
/// these modules and manage the calling of all of these functions for you in a systematic way, or
/// one could use `AnalysisModule`s on their own right for organizational purposes.
pub trait AnlModule<E: Archive, R>: Send + Sync {
    /// Required name of the module, can be used for naming outputs or keeping track of outputs
    fn name(&self) -> String;

    /// Runs before the event loop. The output directory is passed in case the module generates
    /// output that should be buffered and written during analysis rather than holding on to all of
    /// the data in memory.
    fn initialize(&mut self, _output_directory: &Path) {}

    /// Pre filter events before event is called. This work could be done in the begining of
    /// `analyze_event`, but this is sometimes cleaner, generally it is better to use an
    /// `EventFilter` though for broad analysis.
    fn filter_event(&self, _event: &<E as Archive>::Archived, _idx: usize) -> bool {
        true
    }

    /// Runs once per event
    fn analyze_event(&self, _event: &A<E>, _idx: usize) -> Option<R>;

    ///
    fn handle_result_chunk(&mut self, results: &mut [R]);

    /// Place to put periodic print statements every once in a while (interval determined by the
    /// user)
    fn report(&mut self) {}

    /// Runs after the event loop
    fn finalize(&mut self, _output_directory: &Path) {}
}

pub struct Anl<E: Archive, R> {
    /// The analysis scripts used to analyze the generated events
    anl_module: Option<Arc<RwLock<Box<dyn AnlModule<E, R>>>>>,
    /// Where to find the actual input data. This value needs to get set manually, otherwise
    /// `run_analysis` will panic.
    input_directory: Option<String>,
    /// Where to ouput data to. The data will actually be written to a subdirectory of this
    /// location, using the `self.mixer.name()` as the subdirectory name. This value needs to get
    /// set manually, otherwise `run_analysis` will panic.
    output_directory: Option<String>,
    //
    update_interval: usize,
}

impl<E: Archive, R> Default for Anl<E, R> {
    fn default() -> Self {
        Self::new()
    }
}

impl<E: Archive, R> Anl<E, R> {
    /// create a new `MixedAnalysis` from a boxed `EventMixer`
    pub fn new() -> Self {
        Anl::<E, R> {
            anl_module: None,
            input_directory: None,
            output_directory: None,
            update_interval: 10_000,
        }
    }

    /// Add an anlsysis module to use for the unmixed data. These analysis modules are not
    /// automatically applied to the mixed events
    pub fn with_anl_module(mut self, module: impl AnlModule<E, R> + 'static) -> Self {
        //pub fn with_anl_module<M:  AnlModule<E, R> + 'static>(mut self, module: M) -> Self {
        let module: Arc<RwLock<Box<dyn AnlModule<E, R>>>> = Arc::new(RwLock::new(Box::new(module)));
        self.anl_module = Some(module);
        self
    }

    /// A directory containing the raw `rkyv::Archived` data.
    pub fn with_input_directory(mut self, input: &str) -> Self {
        self.input_directory = Some(input.into());
        self
    }

    /// Directory where the output data should be written.
    /// Subdirectories will be generated within this directory.
    pub fn with_output_directory(mut self, input: &str) -> Self {
        self.output_directory = Some(input.into());
        self
    }

    /// The number of events between ouputs. The `report` function of analysis modules is called
    /// this often.
    pub fn with_update_interval(mut self, interval: usize) -> Self {
        self.update_interval = interval;
        self
    }

    pub fn input_directory(&self) -> Option<&str> {
        self.input_directory.as_deref()
    }

    pub fn output_directory(&self) -> Option<&str> {
        self.output_directory.as_deref()
    }
}

//DataSet<E>: Serialize<
//CompositeSerializer<
//AlignedSerializer<AlignedVec>,
//FallbackScratch<HeapScratch<256>, AllocScratch>,
//SharedSerializeMap,
//>,
//>,
impl<'a, E, R> Anl<E, R>
where
    E: Archive,
    <E as Archive>::Archived: Sync,
    R: Send,
    //R: Sync,
{
    fn run_real_analysis(&self, in_dir: PathBuf, out_dir: PathBuf) {
        let anl_module = self.anl_module.as_ref().expect("No module attatched");

        Anl::<E, R>::make_announcment("INITIALIZE");
        anl_module
            .write()
            .expect("Failed to get write lock")
            .initialize(&out_dir);

        Anl::<E, R>::make_announcment("EVENT LOOP");

        let mut result_chunk = {
            let anl = anl_module.read().expect("Failed to get read lock");

            let file_set = DataFileCollection::new_from_path(in_dir.as_path());

            {
                let data_set = file_set.datasets::<E>();
                data_set
                    .archived_iter()
                    .enumerate()
                    .par_bridge()
                    .filter(|(idx, event)| anl.filter_event(&event, *idx))
                    .map(|(idx, event)| anl.analyze_event(&event, idx))
                    .filter_map(|res| res)
                    .collect::<Vec<R>>()
            }
        };

        {
            // This gives
            let mut anl = anl_module.write().expect("Failed to get write lock");
            anl.handle_result_chunk(&mut result_chunk);
        }

        Anl::<E, R>::make_announcment("FINALIZE");
        anl_module
            .write()
            .expect("Failed to get write lock")
            .finalize(&out_dir);
    }

    pub fn run(&mut self) {
        // Manage directories

        let out_dir_parent = self.output_directory.clone();
        let (in_dir, out_dir) = self.manage_output_paths();

        //let mem_mapped_files = Anl::<E, R>::map_data(in_dir.as_path());

        //let data_set = DataSetCollection::new_from_path(in_dir.as_path());
        //let data_set: ArchivedData<E> = DataSetCollection::new_from_path(in_dir.as_path());
        //let data_set: ArchivedData<'b, E> = DataSetCollection::new_from_path(in_dir.as_path());

        //let data_set: ArchivedData<E> = DataSetCollection::new(mem_mapped_files);

        self.run_real_analysis(in_dir, out_dir.unwrap());

        Anl::<E, R>::make_announcment("DONE");
        println!("Output Directory : {}", out_dir_parent.unwrap());
    }

    fn should_run_real_analysis(&self) -> bool {
        self.anl_module.is_some()
    }

    #[allow(dead_code)]
    fn update_real_events(analyzed: usize) {
        println!("Analyzed : {}", analyzed.separate_with_underscores());
    }

    fn update_mixed_events(
        attempts: usize,
        analyzed: usize,
        secs_since_last: f64,
        overal_time_s: f64,
    ) {
        println!(
            "({:.1} | {:.2} s): {}{}{}{}{}{}",
            overal_time_s,
            secs_since_last,
            "Attempts: ".blue().bold(),
            attempts.to_string().separate_with_underscores(),
            " Analyzed: ".green().bold(),
            analyzed.to_string().separate_with_underscores(),
            " Rejected: ".red().bold(),
            (attempts - analyzed)
                .to_string()
                .separate_with_underscores()
        );
    }

    fn make_announcment(text: &str) {
        let s = format!("[[ {} ]]", text).blue().bold();
        println!("{}", s);
    }
    fn manage_output_paths(&self) -> (PathBuf, Option<PathBuf>) {
        let in_dir: &Path = Path::new(
            self.input_directory()
                .expect("Input data directory not set."),
        );

        let out_dir: &Path = Path::new(
            self.output_directory()
                .expect("Output data directory not set."),
        );

        if !out_dir.is_dir() {
            create_dir(out_dir).unwrap_or_else(|_| {
                panic!(
                    "Output {} directory could not be created",
                    out_dir.to_str().unwrap()
                )
            });
        }

        if !out_dir.is_dir() {
            create_dir(&out_dir).expect("Output directory could not be created");
        }

        let real_out_dir = None;

        (in_dir.into(), real_out_dir)
    }

    pub fn map_data(directory: &Path) -> Vec<Mmap> {
        let mut mmaps = Vec::new();

        let paths = std::fs::read_dir(directory).expect("problem opening input directory");
        for path in paths {
            let path = path.expect("Ivalid path").path();

            let path_name = path
                .to_str()
                .expect("Path could not be interpretted as str");

            let length = path_name.len();
            if !path_name[length - 5..].contains(".rkyv") {
                // if the file is not marked as an rkyv file, don't try to read it as one
                continue;
            }

            let input_file = File::open(path_name).expect("File could not be found");

            let memory_map =
                unsafe { Mmap::map(&input_file).expect("Input file could not be memory mapped") };
            mmaps.push(memory_map);
        }

        mmaps
    }
    pub fn map_idx_data(directory: &Path) -> Vec<Mmap> {
        let mut mmaps = Vec::new();

        let paths = std::fs::read_dir(directory).expect("problem opening input directory");
        for path in paths {
            let path = path.expect("Ivalid path").path();

            let path_name = path
                .to_str()
                .expect("Path could not be interpretted as str");

            let length = path_name.len();
            if !path_name[length - 4..].contains(".idx") {
                continue;
            }

            let input_file = File::open(path_name).expect("File could not be found");

            let memory_map =
                unsafe { Mmap::map(&input_file).expect("Input file could not be memory mapped") };
            mmaps.push(memory_map);
        }

        mmaps
    }
}

fn indices(file: &std::path::Path) -> Vec<usize>
where
    <Vec<usize> as Archive>::Archived: rkyv::Deserialize<Vec<usize>, rkyv::Infallible>,
{
    let bytes = std::fs::read(file).unwrap();

    let indices: &<Vec<usize> as Archive>::Archived =
        unsafe { rkyv::archived_root::<Vec<usize>>(&bytes) };

    let indices_2: Vec<usize> = indices
        .deserialize(&mut rkyv::Infallible)
        .expect("Deserializing filtered indces failed");

    indices_2
}
