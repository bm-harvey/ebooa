use crate::data_set::DataCollectionIter;
use crate::data_set::{ArchivedData, DataCollection, DataSet, FilteredArchivedData};
use colored::Colorize;
use indicatif::ParallelProgressIterator;
//use indicatif::ProgressBar;
//use indicatif::ProgressIterator;
use memmap2::Mmap;
//use rayon::iter::ParallelBridge;
use rayon::iter::ParallelBridge;
use rayon::iter::{IntoParallelIterator, ParallelIterator};
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

/// The generic form of an event based analysis module. An `Analysis` or `MixedAnalysis` can take in one or more of
/// these modules and manage the calling of all of these functions for you in a systematic way, or
/// one could use `AnalysisModule`s on their own right for organizational purposes.
pub trait AnlModule<E, R> {
    /// Required name of the module, can be used for naming outputs or keeping track of outputs
    fn name(&self) -> String;

    /// Runs before the event loop. The output directory is passed in case the module generates
    /// output that should be buffered and written during analysis rather than holding on to all of
    /// the data in memory.
    fn initialize(&mut self, _output_directory: &Path) {}

    /// Pre filter events before event is called. This work could be done in the begining of
    /// `analyze_event`, but this is sometimes cleaner, generally it is better to use an
    /// `EventFilter` though for broad analysis.
    fn filter_event(&self, _event: &E, _idx: usize) -> bool {
        true
    }

    /// Runs once per event
    fn analyze_event(&self, _event: &E, _idx: usize) -> Option<R>;

    ///
    fn handle_result_chunk(&mut self, results: &mut [R]);

    /// Place to put periodic print statements every once in a while (interval determined by the
    /// user)
    fn report(&mut self) {}

    /// Runs after the event loop
    fn finalize(&mut self, _output_directory: &Path) {}
}

pub trait EventFilter<E>: Send + Sync {
    ///
    fn name(&self) -> String;

    /// Whether or not to accept an event into the mixed analysis. By default, all events are
    /// accepted. Using this function before trying to generate events can be a massive efficiency
    /// gain or simply just necassary, dependng on the anaysis being done.
    fn filter_event(&self, _event: &E, _idx: usize) -> bool;
}

/// Mixed Analysis is very similar to `Analysis` in nature and in use. It takes in exactly one
/// `EventMixer` and any number of `AnalysisModule`s. The events in the dataset get read opened.
/// Events that pass the filter get written to th output directory, and then that dataset is opened
/// for analysis. Instead of looping over all of the events in the filtered data, the filtered data
/// set is used by the EventMixer to generate events, which are then passsed to the
/// `AnalysisModule`s.
pub struct Anl<'a, E: Archive, R> {
    /// The analysis scripts used to analyze the generated events
    anl_module: Option<Arc<RwLock<dyn 'a + AnlModule<E, R>>>>,
    /// What actually does the filtering
    filter: Option<Arc<RwLock<dyn EventFilter<E>>>>,
    /// Where to find the actual input data. This value needs to get set manually, otherwise
    /// `run_analysis` will panic.
    input_directory: Option<String>,
    /// Where to ouput data to. The data will actually be written to a subdirectory of this
    /// location, using the `self.mixer.name()` as the subdirectory name. This value needs to get
    /// set manually, otherwise `run_analysis` will panic.
    output_directory: Option<String>,
    ///
    max_raw: usize,
    ///
    update_interval: usize,
}

impl<'a, E: Archive, R> Default for Anl<'a, E, R> {
    fn default() -> Self {
        Self::new()
    }
}

impl<'a, E: Archive, R> Anl<'a, E, R> {
    /// create a new `MixedAnalysis` from a boxed `EventMixer`
    pub fn new() -> Self {
        Anl::<E, R> {
            filter: None,
            anl_module: None,
            input_directory: None,
            output_directory: None,
            max_raw: usize::MAX,
            update_interval: 10_000,
        }
    }

    /// Add an anlsysis module to use for the unmixed data. These analysis modules are not
    /// automatically applied to the mixed events
    pub fn with_anl_module<M: AnlModule<E, R> + 'a>(mut self, module: M) -> Self {
        self.anl_module = Some(Arc::new(RwLock::new(module)));
        self
    }

    pub fn with_filter<F: EventFilter<E> + 'static>(mut self, filter: F) -> Self {
        self.filter = Some(Arc::new(RwLock::new(filter)));
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

    pub fn with_max_real_events(mut self, real_events: usize) -> Self {
        self.max_raw = real_events;
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

impl<'a, E: Archive + 'a, R> Anl<'a, E, R>
where
    DataSet<E>: Serialize<
        CompositeSerializer<
            AlignedSerializer<AlignedVec>,
            FallbackScratch<HeapScratch<256>, AllocScratch>,
            SharedSerializeMap,
        >,
    >,
    <E as Archive>::Archived: Deserialize<E, rkyv::Infallible>,
    <DataSet<E> as Archive>::Archived: Sync + 'a,
    E: Sync + Send,
    //R: + Send,
    E: Archive,
    <E as Archive>::Archived: Deserialize<E, rkyv::Infallible>,
    DataCollectionIter<'a, E>: Send,
    R: std::marker::Send,
    dyn AnlModule<E, R> + 'a: Send + Sync,
    //DataCollectionIter<'a, E>: Iterator<Item = E>,
{
    fn generate_filtered_indices(&self, in_dir: &Path, out_dir: &Path) {
        println!("{}", out_dir.to_str().unwrap());
        let timer = std::time::Instant::now();
        if self.filter.is_none() {
            return;
        }

        // Create a data collection for the original data.
        let memory_maps = Anl::<E, R>::map_data(in_dir);
        let data_collection: DataCollection<<DataSet<E> as Archive>::Archived, E> =
            DataCollection::new(&memory_maps);
        //let data_collection = DataCollection::new(&memory_maps);

        let max_raw = usize::min(self.max_raw, data_collection.len());
        let data_collection = Arc::new(RwLock::new(data_collection));
        let data_set = Arc::new(Mutex::new(DataSet::<usize>::new()));
        //let output_size = self.filtered_output_size;

        let num_found = Arc::new(Mutex::new(0));

        let filter = self.filter.as_ref().unwrap().clone();

        //let update_interval = self.update_interval;
        (0..max_raw).into_par_iter().progress().for_each(|idx| {
            let event = data_collection.read().unwrap().event_by_idx(idx).unwrap();
            if filter.read().unwrap().filter_event(&event, 0) {
                let mut data_set = data_set.lock().unwrap();
                data_set.add_event(idx);
                let mut num_found = num_found.lock().unwrap();
                *num_found += 1;
            }
        });

        let mut data_set = data_set.lock().unwrap();

        println!("time to filter = {} s", timer.elapsed().as_secs());
        Anl::<E, R>::generate_filtered_idx_file(out_dir, &mut data_set)
    }
    fn generate_filtered_idx_file(out_dir: &Path, data_set: &mut DataSet<usize>) {
        let file_name: String = String::from("idx.rkyv");
        let out_file_name: String = out_dir.join(file_name).to_str().unwrap().into();
        println!("Generating file:{}", out_file_name);
        let out_file = File::create(out_file_name)
            .expect("A filtered rkyv'ed idx file could not be generated");
        let mut buffer = BufWriter::new(out_file);
        buffer
            .write_all(rkyv::to_bytes::<_, 256>(data_set).unwrap().as_slice())
            .unwrap();
        data_set.clear();
    }

    pub fn run(&mut self) {
        // Manage directories

        let out_dir_parent = self.output_directory.clone();
        let (in_dir, real_out_dir) = self.manage_output_paths();

        let mem_mapped_files = Anl::<E, R>::map_data(in_dir.as_path());

        let data_set: ArchivedData<'a, E> = DataCollection::new(&mem_mapped_files);

        self.run_real_analysis(&data_set, real_out_dir.unwrap());

        Anl::<E, R>::make_announcment("DONE");
        println!("Output Directory : {}", out_dir_parent.unwrap());
    }
    fn should_run_real_analysis(&self) -> bool {
        self.anl_module.is_some()
    }

    //fn run_real_analysis(&mut self, dataset: &ArchivedData<E>, out_dir: PathBuf) {
    fn run_real_analysis(&self, dataset: &'a ArchivedData<E>, out_dir: PathBuf) {
        let anl_module = self.anl_module.as_ref().expect("No module attatched");

        Anl::<E, R>::make_announcment("INITIALIZE");
        anl_module
            .write()
            .expect("Failed to get write lock")
            .initialize(&out_dir);

        Anl::<E, R>::make_announcment("EVENT LOOP");
        let time_in_event_s: f64 = 0.;
        let start_outer = std::time::Instant::now();
        //

        {
            let lock = anl_module.read().expect("Failed to get read lock");

            let results = dataset
                .iter()
                .enumerate()
                .par_bridge()
                .filter(|(idx, event)| lock.filter_event(&event, *idx))
                .map(|(idx, event)| lock.analyze_event(&event, idx))
                .filter_map(|res| res)
                .collect::<Vec<R>>();
        }
        //
        println!("Time in analyze_event functions: {} s", time_in_event_s);
        println!(
            "Time in event loop: {} s",
            start_outer.elapsed().as_secs_f64()
        );

        Anl::<E, R>::make_announcment("FINALIZE");
        anl_module
            .write()
            .expect("Failed to get write lock")
            .finalize(&out_dir);
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

        let out_dir = match &self.filter {
            None => out_dir.join("all_events"),
            Some(filter) => out_dir.join(filter.read().unwrap().name()),
        };

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
