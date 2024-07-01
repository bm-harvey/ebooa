use crate::data_set::{ArchivedData, DataCollection, DataSet, FilteredArchivedData};
use colored::Colorize;
use indicatif::ParallelProgressIterator;
use indicatif::ProgressBar;
use indicatif::ProgressIterator;
use memmap2::Mmap;
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
pub trait AnlModule<E> {
    /// Required name of the module, can be used for naming outputs or keeping track of outputs
    fn name(&self) -> String;

    /// Runs before the event loop. The output directory is passed in case the module generates
    /// output that should be buffered and written during analysis rather than holding on to all of
    /// the data in memory.
    fn initialize(&mut self, _output_directory: &Path) {}

    /// Pre filter events before event is called. This work could be done in the begining of
    /// `analyze_event`, but this is sometimes cleaner, generally it is better to use an
    /// `EventFilter` though for broad analysis.
    fn filter_event(&mut self, _event: &E, _idx: usize) -> bool {
        true
    }

    /// Runs once per event
    fn analyze_event(&mut self, _event: &E, _idx: usize) {}

    /// Place to put periodic print statements every once in a while (interval determined by the
    /// user)
    fn report(&mut self) {}

    /// Runs after the event loop
    fn finalize(&mut self) {}

    /// Runs after finalize. As is, there is no generic way to output the results of the analysis,
    /// so it is up to the user to generate the output. If the module ends up storing a lot of
    /// data, it might be smart to actually generate the output as a buffered output one every
    /// event and ignore this function entirely. This depends entirely on the scale of the output,
    /// so user discretion is advised.
    fn generate_output(&mut self, _output_directory: &Path) {}
}

/// `EventMixer`s are used in conjunction with `AnalysisModule`s to generate a `MixedAnalysis`. The
/// goal of these modules is to sreamline the process of workng with "combinitoric mixing of
/// events" - the process of generating events based on the particles measured in many independent
/// events to generate events with certain correlations removed, often for background subracting.
/// While this framework is more flexible than that goal, that is the original intention behind
/// this trait.
pub trait EventMixer<E>: Send + Sync
where
    E: Archive,
{
    /// The name of the mixer will be used to generate a subdirectory under the output directory.
    /// At runtime, a filtered data set will be generated in a subfolder called `rkyv` (which can
    /// optionally be automatically removed at the end of the run time). The output directory that
    /// a `MixedAnalysis` will use is the one provided joined with the `name`.
    fn name(&self) -> String;

    /// Attempt to make a new event. This returns an `Option` because there are cases where an
    /// event will be generated and fail to pass some condtition. If one generates a mixed events
    /// which have 2 alpha particles, this can be done easily and deterministically. If one later
    /// wants to impose a total kinetic energy threshold, then that can't be known until after an
    /// event is made / proposed and then checked. Returning None is a way to say that a valid
    /// mixed event was not generated, and to try again next time. `MixedAnalysis` will keep track
    /// of both the number of successfully generated mixed events as well as the total number of
    /// attempts, both of which can be restricted.
    fn mix_events(&self, _data_collection: &FilteredArchivedData<E>, _idx: usize) -> Option<E>;
}

/// Often used for creating `EventMixer`s but not always.  
pub trait EventScrambler<E>: Send + Sync {
    fn name(&self) -> String;
    fn scramble_event(&self, _event: &E, _idx: usize) -> Option<E>;
}

pub enum EventGenerator<E> {
    Mixer(Box<dyn EventMixer<E>>),
    Scrambler(Box<dyn EventScrambler<E>>),
}

impl<E> EventGenerator<E> {
    pub fn generate_event(&self, data_collection: &FilteredArchivedData<E>, idx: usize) -> Option<E>
    where
        E: Archive,
        <E as Archive>::Archived: Deserialize<E, rkyv::Infallible>,
        E: Send + Sync,
    {
        match self {
            EventGenerator::Mixer(mixer) => mixer.mix_events(data_collection, idx),
            EventGenerator::Scrambler(scrambler) => {
                let event = data_collection.event_by_idx(idx % data_collection.len());
                scrambler.scramble_event(&event.unwrap(), idx)
                //scrambler.scramble_event(&data_collection.random_event(), idx)
            }
        }
    }
    pub fn name(&self) -> String
    where
        E: Archive,
    {
        match self {
            EventGenerator::Mixer(mixer) => mixer.name(),
            EventGenerator::Scrambler(scrambler) => scrambler.name(),
        }
    }
}

pub trait EventFilter<E>: Send + Sync {
    ///
    fn name(&self) -> String;

    /// Whether or not to accept an event into the mixed analysis. By default, all events are
    /// accepted. Using this function before trying to generate events can be a massive efficiency
    /// gain or simply just necassary, dependng on the anaysis being done.
    fn filter_event(&self, _event: &E, _idx: usize) -> bool;
}

pub enum MixedEventMaximum {
    Factor(f64),
    Absolute(usize),
}

/// Mixed Analysis is very similar to `Analysis` in nature and in use. It takes in exactly one
/// `EventMixer` and any number of `AnalysisModule`s. The events in the dataset get read opened.
/// Events that pass the filter get written to th output directory, and then that dataset is opened
/// for analysis. Instead of looping over all of the events in the filtered data, the filtered data
/// set is used by the EventMixer to generate events, which are then passsed to the
/// `AnalysisModule`s.
pub struct Anl<'a, E: Archive> {
    /// This needs to be set to actually run the script.  
    //mixer: Option<Box<dyn EventMixer<E> + 'a>>,
    event_generator: Option<EventGenerator<E>>,
    /// The analysis scripts used to analyze the generated events
    real_modules: Vec<Box<dyn AnlModule<E> + 'a>>,
    /// The analysis scripts used to analyze the generated mixed events
    mixed_modules: Vec<Box<dyn AnlModule<E> + 'a>>,
    /// What actually does the filtering
    filter: Option<Arc<RwLock<dyn EventFilter<E>>>>,
    ///Keep track of whether or not a filter has been manually set. This prevents copying the
    ///entire data set when using he default transparent filter.
    filter_manually_set: bool,
    /// Where to find the actual input data. This value needs to get set manually, otherwise
    /// `run_analysis` will panic.
    input_directory: Option<String>,
    /// Where to ouput data to. The data will actually be written to a subdirectory of this
    /// location, using the `self.mixer.name()` as the subdirectory name. This value needs to get
    /// set manually, otherwise `run_analysis` will panic.
    output_directory: Option<String>,
    /// The maximum number of generated events to analyze.
    max_mixed_events: MixedEventMaximum,
    /// The maximum number of real events to analyze.
    max_real_events: Option<usize>,
    /// Because mixed events can "fail" or be rejected, this is a cap to make sure a deterministic
    /// maximum attempts at generating mixed events
    max_attempts: usize,
    /// The maximum number of "real" events to use to generate mixed events.
    max_raw: usize,
    /// The maximum number of filtered events to use to generate mixed events.  
    max_filtered: usize,
    /// How often to call `report` on the `AnalysisModule`s. Based on attempted generated events,
    /// not sucessfully generated.  
    update_interval: usize,
    /// Number of events stored in each file in the filtered event output. Larger number requires
    /// more RAM depending on the size of the type stored, but might be faster when reading back in
    filtered_output_size: usize,
    /// Whether or not to delete the generated data after the analysis is done running
    delete_filtered_data_dir: bool,
    /// Analysis to be run on the filtered events without mixing them.
    /// real_analysis: Analysis<E>,
    /// Use the existing filtered events if they exist. This can be a massive time save if there has
    /// been no changes to the raw data or the filtering algorithm.
    use_existing: bool,
}

impl<'a, E: Archive> Default for Anl<'a, E> {
    fn default() -> Self {
        Self::new()
    }
}

impl<'a, E: Archive> Anl<'a, E> {
    /// create a new `MixedAnalysis` from a boxed `EventMixer`
    pub fn new() -> Self {
        Anl::<E> {
            event_generator: None,
            filter: None,
            filter_manually_set: false,
            mixed_modules: Vec::new(),
            real_modules: Vec::new(),
            input_directory: None,
            output_directory: None,
            max_mixed_events: MixedEventMaximum::Factor(1.),
            max_real_events: None,
            max_attempts: usize::MAX,
            max_filtered: usize::MAX,
            max_raw: usize::MAX,
            update_interval: 1_000_000,
            delete_filtered_data_dir: false,
            filtered_output_size: 1_000_000,
            use_existing: true,
        }
    }

    /// Add an anlysis module to run the mixed events through. These are not automatically applied
    /// to the unmixed events
    pub fn with_mixed_module<M: AnlModule<E> + 'a>(mut self, module: M) -> Self {
        self.mixed_modules.push(Box::new(module));
        self
    }

    /// Add an analysis module to use for the unmixed data. These analysis modules are not
    /// automatically applied to the mixed events
    pub fn with_module<M: AnlModule<E> + Clone + 'a>(mut self, module: M) -> Self {
        self.real_modules.push(Box::new(module.clone()));
        self.mixed_modules.push(Box::new(module));
        self
    }
    /// Add an anlsysis module to use for the unmixed data. These analysis modules are not
    /// automatically applied to the mixed events
    pub fn with_real_module<M: AnlModule<E> + 'a>(mut self, module: M) -> Self {
        self.real_modules.push(Box::new(module));
        self
    }

    pub fn with_filter<F: EventFilter<E> + 'static>(mut self, filter: F) -> Self {
        self.filter = Some(Arc::new(RwLock::new(filter)));
        self.filter_manually_set = true;
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
    /// The number of mixed events to be generated. If this value is not explicitly set, the number
    /// of events generated will be a 1:1 scale of the filtered data. This number of mixed events
    /// may not get reached if mixed events fail at a high rate, and max attampts gets reached
    /// first.
    pub fn with_max_mixed_events(mut self, max_events: MixedEventMaximum) -> Self {
        self.max_mixed_events = max_events;
        self
    }

    pub fn with_max_real_events(mut self, real_events: usize) -> Self {
        self.max_real_events = Some(real_events);
        self
    }

    /// Maximum number of times to attempt to generate a mixed event. Unless you are rejecting
    /// mixed events at a very high rate, this usually does not need to come into play.
    pub fn with_max_mixed_attempts(mut self, max_attempts: usize) -> Self {
        self.max_attempts = max_attempts;
        self
    }

    /// The maximum number of raw events to be filtered. As a consequence of this setting, this
    /// also ends up being the maximum number of events to analysis in the real data. This value
    /// may not get reached if the 'maximum filtered' setting is set
    pub fn with_max_raw(mut self, max_raw: usize) -> Self {
        self.max_raw = max_raw;
        self
    }

    /// The maximum number of events allowed into the filtered data. This number may not be reached
    /// if the 'max raw' setting is set.
    pub fn with_max_filtered(mut self, max_filtered: usize) -> Self {
        self.max_filtered = max_filtered;
        self
    }

    /// The number of events to store per filtered output file. This is purely here for memory
    /// management. If your filtered output can fit into memory, settng this number high can be
    /// really good for speed. If you can't then you need to lower this number to make more files.
    /// This will come with a performance hit, but still very useable.
    pub fn with_filtered_output_size(mut self, size: usize) -> Self {
        self.filtered_output_size = size;
        self
    }

    /// The number of events between ouputs. The `report` function of analysis modules is called
    /// this often.
    pub fn with_update_interval(mut self, interval: usize) -> Self {
        self.update_interval = interval;
        self
    }

    /// Delete the filtered data from the output directory. Useful if you are trying to keep data
    /// duplication down, annoying if you are repeatedly running analyiss on the same filtered data
    /// and you have to regenerate it everytime. By default, the data is left on the output drive.
    pub fn clean_up_filtered(mut self) -> Self {
        self.delete_filtered_data_dir = true;
        self
    }

    /// If there is already a data directory with filtered data, use that rather than regenerating
    /// it.
    pub fn use_existing_filtered(mut self, use_existing: bool) -> Self {
        self.use_existing = use_existing;
        self
    }

    pub fn input_directory(&self) -> Option<&str> {
        self.input_directory.as_deref()
    }

    pub fn output_directory(&self) -> Option<&str> {
        self.output_directory.as_deref()
    }
}

impl<'a, E: Archive> Anl<'a, E> {
    pub fn with_event_generator(mut self, event_generator: EventGenerator<E>) -> Self {
        self.event_generator = Some(event_generator);
        self
    }
}

impl<'a, E: Archive> Anl<'a, E>
where
    DataSet<E>: Serialize<
        CompositeSerializer<
            AlignedSerializer<AlignedVec>,
            FallbackScratch<HeapScratch<256>, AllocScratch>,
            SharedSerializeMap,
        >,
    >,
    <E as Archive>::Archived: Deserialize<E, rkyv::Infallible>,
    <DataSet<E> as Archive>::Archived: Sync,
    E: Sync + Send,
    dyn EventMixer<E>: Sync + Send,
    dyn EventScrambler<E>: Sync + Send,
{
    fn generate_filtered_indices(&self, in_dir: &Path, out_dir: &Path) {
        println!("{}", out_dir.to_str().unwrap());
        let timer = std::time::Instant::now();
        if self.filter.is_none() {
            return;
        }

        // Create a data collection for the original data.
        let memory_maps = Anl::map_data(in_dir);
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
        Anl::generate_filtered_idx_file(out_dir, &mut data_set)
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
        let (in_dir, real_out_dir, mixed_out_dir, filtered_out_dir) = self.manage_output_paths();

        // Generate data set
        let mem_mapped_files = Anl::map_data(in_dir.as_path());

        let idx_data = if self.filter_manually_set {
            if !filtered_out_dir.as_ref().unwrap().join("idx.rkyv").exists() || !self.use_existing {
                if !filtered_out_dir.as_ref().unwrap().exists() {
                    std::fs::create_dir(filtered_out_dir.as_ref().unwrap())
                        .expect("Filtered data direcory could not be created");
                }

                self.generate_filtered_indices(
                    in_dir.as_path(),
                    filtered_out_dir.as_ref().unwrap().as_path(),
                );
            }

            let idx_path = filtered_out_dir.as_ref().unwrap().join("idx.rkyv");
            let idx_path = idx_path.as_path();

            Some(indices(idx_path))
        } else {
            None
        };

        //let idx_map =
        //unsafe { Mmap::map(&).expect("Input file could not be memory mapped") };

        let data_set: ArchivedData<E> = DataCollection::new(&mem_mapped_files);
        //let idx_set: DataCollection<<Idx as Archive>::Archived, Idx> =
        //DataCollection::new(&mem_mapped_idx_files);

        if self.should_run_real_analysis() {
            self.run_real_analysis(&data_set, &idx_data, real_out_dir.unwrap());
            //self.run_real_analysis(&data_set, real_out_dir.unwrap());
        }

        if self.should_run_mixed_analysis() {
            //self.run_mixed_analysis(&data_set, mixed_out_dir.unwrap());
            self.run_mixed_analysis(&data_set, &idx_data, mixed_out_dir.unwrap());
        }

        if self.filter_manually_set && self.delete_filtered_data_dir && !self.use_existing {
            drop(mem_mapped_files);
            println!("[[ REMOVING FILTERED DATA ]]");
            if filtered_out_dir.as_ref().unwrap().is_dir() {
                std::fs::remove_dir_all(filtered_out_dir.as_ref().unwrap()).unwrap();
            }
        }
        Anl::make_announcment("DONE");
        println!("Output Directory : {}", out_dir_parent.unwrap());
    }
    fn should_run_real_analysis(&self) -> bool {
        !self.real_modules.is_empty()
    }
    fn should_run_mixed_analysis(&self) -> bool {
        self.event_generator.is_some() && !self.mixed_modules.is_empty()
    }

    //fn run_real_analysis(&mut self, dataset: &ArchivedData<E>, out_dir: PathBuf) {
    fn run_real_analysis(
        &mut self,
        dataset: &ArchivedData<E>,
        filtered_indices: &Option<Vec<usize>>,
        out_dir: PathBuf,
    ) {
        Anl::make_announcment("INITIALIZE");
        self.real_modules
            .iter_mut()
            .for_each(|module| module.initialize(&out_dir));

        let time_in_event_s: f64 = 0.;
        let start_outer = std::time::Instant::now();

        Anl::make_announcment("EVENT LOOP");

        let max_events = match filtered_indices {
            Some(indices) => indices.len(),
            None => dataset.len(),
        };
        let max_events = match self.max_real_events {
            Some(value) => std::cmp::min(value, max_events),
            None => max_events,
        };

        let indices: Vec<usize> = match filtered_indices {
            Some(indices) => indices[..max_events].into(),
            None => (0..max_events).collect(),
        };

        for (event_num, idx) in indices.iter().enumerate().progress() {
            let event = dataset.event_by_idx(*idx).unwrap();
            self.real_modules.iter_mut().for_each(|module| {
                if module.filter_event(&event, event_num) {
                    module.analyze_event(&event, event_num);
                }
            })
        }

        /*
        let indices = Arc::new(Mutex::new(indices));

        let mut write_batches = Vec::new();
        let mut read_batches = Vec::new();
        //let num_threads = 30;
        let num_threads = 3;
        for _thread in 0..num_threads {
            write_batches.push(Arc::new(Mutex::new(Vec::<E>::new())));
            read_batches.push(Arc::new(Mutex::new(Vec::<E>::new())));
        }
        //let analyzed = Arc::new(Mutex::new(0_usize));
        let mut analyzed = 0;
        let bar = ProgressBar::new(max_events as u64);
        loop {
            std::mem::swap(&mut read_batches, &mut write_batches);
            std::thread::scope(|s| {
                //dbg!();
                for write_batch in write_batches.iter() {
                    let write_batch = write_batch.clone();
                    let indices = indices.clone();
                    let _handle = s.spawn(move || {
                        let mut indices = indices.lock().unwrap();
                        let len = indices.len();
                        let num = len - std::cmp::min(len, 10_000);
                        let idxs = indices.split_off(num);
                        drop(indices);
                        let mut write_batch = write_batch.lock().unwrap();
                        *write_batch = dataset.events_by_idxs(*idxs.first().unwrap(), *idxs.last().unwrap())
                        //write_batch.clear();
                        //for idx in idxs.iter() {
                            ////println!("{idx}");
                            //write_batch.push(dataset.event_by_idx(*idx).unwrap());
                        //}
                    });
                }

                let anl_event_time = std::time::Instant::now();
                for read_batch in read_batches.iter() {
                    for event in read_batch.lock().unwrap().iter() {
                        self.real_modules.iter_mut().for_each(|module| {
                            if module.filter_event(event, 0) {
                                module.analyze_event(event, 0);
                            }
                        })
                    }
                    let num_analyzed = read_batch.lock().unwrap().len();
                    analyzed += num_analyzed;
                    bar.inc(num_analyzed as u64);
                }
                time_in_event_s += anl_event_time.elapsed().as_secs_f64();

                //dbg!();
            });

            if write_batches.iter().all(|wb| wb.lock().unwrap().is_empty()) {
                break;
            }
        }
        */
        //bar.finish();

        println!("Time in analyze_event functions: {} s", time_in_event_s);
        println!(
            "Time in event loop: {} s",
            start_outer.elapsed().as_secs_f64()
        );

        Anl::make_announcment("FINALIZE");
        self.real_modules
            .iter_mut()
            .for_each(|module| module.finalize());

        Anl::make_announcment("GENERATE OUTPUT");
        self.real_modules
            .iter_mut()
            .for_each(|module| module.generate_output(&out_dir));
    }

    fn run_mixed_analysis(
        &mut self,
        dataset: &ArchivedData<E>,
        filtered_indices: &Option<Vec<usize>>,
        out_dir: PathBuf,
    ) {
        Anl::make_announcment("INITIALIZE");
        self.mixed_modules
            .iter_mut()
            .for_each(|module| module.initialize(&out_dir));

        Anl::make_announcment("EVENT LOOP");
        let max_events = match self.max_mixed_events {
            MixedEventMaximum::Factor(factor) => (factor * dataset.len() as f64) as usize,
            MixedEventMaximum::Absolute(value) => value,
        };

        let batch_size = Arc::new(Mutex::new(1_000_usize));

        //let mut write_batch = Arc::new(Mutex::new(Vec::<E>::new()));
        let generator = Arc::new(self.event_generator.as_ref().unwrap());

        let attempt = Arc::new(Mutex::new(0_usize));
        let analyzed = Arc::new(Mutex::new(0_usize));
        let prev_batch_attempts = Arc::new(Mutex::new(0_usize));
        let overall_timer = std::time::Instant::now();
        let mut timer = std::time::Instant::now();

        let mut write_batches = Vec::new();
        let mut read_batches = Vec::new();
        for _thread in 0..10 {
            write_batches.push(Arc::new(Mutex::new(Vec::<E>::new())));
            read_batches.push(Arc::new(Mutex::new(Vec::<E>::new())));
        }

        let filtered_indices = match filtered_indices {
            None => (0..dataset.len()).collect(),
            Some(idxs) => idxs.clone(),
        };

        let dataset = Arc::new(FilteredArchivedData::new(dataset, &filtered_indices));

        let bar = ProgressBar::new(max_events as u64);
        loop {
            std::mem::swap(&mut read_batches, &mut write_batches);
            *prev_batch_attempts.lock().unwrap() = 0;

            std::thread::scope(|s| {
                for write_batch in write_batches.iter() {
                    let dataset = dataset.clone();
                    let generator = generator.clone();
                    let batch_size = batch_size.clone();
                    let write_batch = write_batch.clone();
                    let prev_batch_attempts = prev_batch_attempts.clone();
                    let attempt = attempt.clone();
                    let _handle = s.spawn(move || {
                        let bs = *batch_size.lock().unwrap();
                        let mut batch = write_batch.lock().unwrap();
                        batch.clear();

                        for idx in 0..bs {
                            let event = generator.generate_event(&dataset, idx);
                            if let Some(e) = event {
                                batch.push(e);
                            }
                        }
                        *prev_batch_attempts.lock().unwrap() += bs;
                        *attempt.lock().unwrap() += bs;
                    });
                }

                for batch in read_batches.iter() {
                    let mut analyzed = analyzed.lock().unwrap();
                    let mut local_events = 0;
                    for (idx, event) in batch.lock().unwrap().iter().enumerate() {
                        self.mixed_modules.iter_mut().for_each(|module| {
                            if module.filter_event(event, *analyzed + idx) {
                                module.analyze_event(event, *analyzed + idx);
                            }
                            bar.inc(1);
                        });
                        local_events += 1;
                    }
                    *analyzed += local_events;
                }
            }); //scope

            let time_since_last_s = timer.elapsed().as_secs_f64();
            if time_since_last_s > 2. {
                let events_attempts =
                    *attempt.lock().unwrap() - *prev_batch_attempts.lock().unwrap();
                let events_analyzed = *analyzed.lock().unwrap();
                let time_since_last_s = timer.elapsed().as_secs_f64();
                let overall_time_s = overall_timer.elapsed().as_secs_f64();
                //Anl::update_mixed_events(
                //events_attempts,
                //events_analyzed,
                //time_since_last_s,
                //overall_time_s,
                //);
                timer = std::time::Instant::now();
            }

            if *analyzed.lock().unwrap() >= max_events {
                break;
            }
        }
        bar.finish();

        Anl::make_announcment("FINALIZE");
        self.mixed_modules
            .iter_mut()
            .for_each(|module| module.finalize());

        Anl::make_announcment("GENERATE OUTPUT");
        self.mixed_modules
            .iter_mut()
            .for_each(|module| module.generate_output(&out_dir));
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
    fn manage_output_paths(&self) -> (PathBuf, Option<PathBuf>, Option<PathBuf>, Option<PathBuf>) {
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

        let filtered_out_dir = if self.filter_manually_set {
            Some(out_dir.clone())
        } else {
            None
        };

        if !out_dir.is_dir() {
            create_dir(&out_dir).expect("Output directory could not be created");
        }

        let mixed_out_dir = if !self.mixed_modules.is_empty() && self.event_generator.is_some() {
            let result = Some(out_dir.join(format!(
                "mixed_{}",
                self.event_generator.as_ref().unwrap().name()
            )));
            if !result.as_ref().unwrap().is_dir() {
                create_dir(result.as_ref().unwrap())
                    .expect("Mixed analysis output directory could not be created");
            }
            result
        } else {
            None
        };

        let real_out_dir;
        if !self.real_modules.is_empty() {
            real_out_dir = Some(out_dir.join("real"));
            if !real_out_dir.as_ref().unwrap().is_dir() {
                create_dir(real_out_dir.as_ref().unwrap())
                    .expect("Real analysis output directory could not be created");
            }
        } else {
            real_out_dir = None;
        }

        (in_dir.into(), real_out_dir, mixed_out_dir, filtered_out_dir)
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
