use crate::data_set::{DataFileCollection, DataSet};
use colored::Colorize;
use indicatif::{MultiProgress, ProgressBar};
use memmap2::Mmap;
use rkyv::{Archive, Deserialize};
use std::cell::RefCell;
use std::fs::create_dir_all;
use std::io::Read;
use std::sync::{Arc, Mutex, RwLock};
use std::{
    fs::File,
    path::{Path, PathBuf},
};
use thousands::Separable;

type A<E> = <E as Archive>::Archived;

#[derive(clap::ValueEnum, Clone, Debug, Default)]
pub enum FileReadMethod {
    #[default]
    Mmap,
    AllocBytes,
}

/// The generic form of an event based analysis module. An `Analysis` or `MixedAnalysis` can take in one or more of
/// these modules and manage the calling of all of these functions for you in a systematic way, or
/// one could use `AnalysisModule`s on their own right for organizational purposes.
pub trait AnlModuleMT<E: Archive, R>: Send + Sync {
    /// Required name of the module, can be used for naming outputs or keeping track of outputs
    fn name(&self) -> String {
        String::from("anl")
    }

    /// Runs before the event loop. The output directory is passed in case the module generates
    /// output that should be buffered and written during analysis rather than holding on to all of
    /// the data in memory.
    fn initialize(&mut self, _output_directory: Option<&Path>) {}

    /// Pre filter events before event is called. This work could be done in the begining of
    /// `analyze_event`, but this is sometimes cleaner, generally it is better to use an
    /// `EventFilter` though for broad analysis.
    fn filter_event(&self, _event: &<E as Archive>::Archived, _idx: usize) -> bool {
        true
    }

    /// Runs once per event
    fn analyze_event(&self, _event: &A<E>, _idx: usize) -> Option<R>;

    fn handle_result_chunk(&mut self, results: &mut Vec<R>);

    /// Place to put periodic print statements every once in a while (interval determined by the
    /// user)
    fn report(&mut self) {}

    /// Runs after the event loop
    fn finalize(&mut self, _output_directory: Option<&Path>) {}
}

type ImplAnlMod<E, R> = Option<Arc<RwLock<Box<dyn AnlModuleMT<E, R>>>>>;
pub struct AnlMT<E: Archive, R> {
    /// The analysis scripts used to analyze the generated events
    anl_module: ImplAnlMod<E, R>,
    /// Where to find the actual input data. This value needs to get set manually, otherwise
    /// `run_analysis` will panic.
    input_directory: Option<PathBuf>,
    /// Where to ouput data to. The data will actually be written to a subdirectory of this
    /// location, using the `self.mixer.name()` as the subdirectory name. This value needs to get
    /// set manually, otherwise `run_analysis` will panic.
    output_directory: Option<PathBuf>,
    //
    update_interval: usize,
    //
    threads: usize,
    //
    max_events: Option<usize>,
    //
    file_read_method: Option<FileReadMethod>,
}

impl<E: Archive, R> Default for AnlMT<E, R> {
    fn default() -> Self {
        Self::new()
    }
}

impl<E: Archive, R> AnlMT<E, R> {
    /// create a new `MixedAnalysis` from a boxed `EventMixer`
    pub fn new() -> Self {
        AnlMT::<E, R> {
            anl_module: None,
            max_events: None,
            input_directory: None,
            output_directory: None,
            update_interval: 10_000,
            threads: std::thread::available_parallelism().unwrap().get(),
            file_read_method: None,
        }
    }
    /// Add an anlsysis module to use for the unmixed data. These analysis modules are not
    /// automatically applied to the mixed events
    pub fn with_anl_module(mut self, module: impl AnlModuleMT<E, R> + 'static) -> Self {
        //pub fn with_anl_module<M:  AnlModule<E, R> + 'static>(mut self, module: M) -> Self {
        let module: Arc<RwLock<Box<dyn AnlModuleMT<E, R>>>> =
            Arc::new(RwLock::new(Box::new(module)));
        self.anl_module = Some(module);
        self
    }

    /// A directory containing the raw `rkyv::Archived` data.
    pub fn with_input_directory(mut self, input: &str) -> Self {
        self.input_directory = Some(Path::new(input).to_path_buf());
        self
    }

    pub fn with_num_threads(mut self, threads: usize) -> Self {
        let threads = if threads == 0 {
            std::thread::available_parallelism().unwrap().get()
        } else {
            let max_threads = std::thread::available_parallelism().unwrap().get();
            std::cmp::min(max_threads, threads)
        };
        self.threads = threads;
        self
    }

    /// Directory where the output data should be written.
    /// Subdirectories will be generated within this directory.
    pub fn with_output_directory(mut self, input: &str) -> Self {
        self.output_directory = Some(Path::new(input).to_path_buf());
        self
    }

    /// The number of events between ouputs. The `report` function of analysis modules is called
    /// this often.
    pub fn with_update_interval(mut self, interval: usize) -> Self {
        self.update_interval = interval;
        self
    }

    pub fn with_file_read_method(mut self, method: FileReadMethod) -> Self {
        self.file_read_method = Some(method);
        self
    }

    pub fn with_max_events(mut self, max_events: usize) -> Self {
        self.max_events = Some(max_events);
        self
    }

    pub fn input_directory(&self) -> Option<&Path> {
        self.input_directory.as_deref()
    }

    pub fn output_directory(&self) -> Option<&Path> {
        self.output_directory.as_deref()
    }

    /// Look through the input files to determine which file read method to use.
    /// This is only called if an explicit method is not provided.
    pub fn file_read_method_heuristic(&self, _files: &[PathBuf]) -> FileReadMethod {
        // TODO: come up with a heuristic
        FileReadMethod::Mmap
    }
}

//DataSet<E>: Serialize<
//CompositeSerializer<
//AlignedSerializer<AlignedVec>,
//FallbackScratch<HeapScratch<256>, AllocScratch>,
//SharedSerializeMap,
//>,
//>,
impl<E, R> AnlMT<E, R>
where
    E: Archive,
    <E as Archive>::Archived: Sync,
    R: Send,
    R: Clone,
    //R: Sync,
{
    pub fn run(&mut self) {
        let global_timer = std::time::Instant::now();
        let anl_module = self.anl_module.as_ref().expect("No module attatched");
        let in_dir = if let Some(path) = self.input_directory() {
            path
        } else {
            panic!("No input directory")
        };

        AnlMT::<E, R>::make_announcment("INITIALIZE");
        let now = std::time::Instant::now();
        anl_module
            .write()
            .expect("Failed to get write lock")
            .initialize(self.output_directory());
        println!("Initialize took {}s", now.elapsed().as_secs_f64());

        AnlMT::<E, R>::make_announcment("EVENT LOOP");
        let now = std::time::Instant::now();
        let dir_entries = std::fs::read_dir(in_dir).expect("problem opening input directory");
        let paths: Vec<PathBuf> = dir_entries
            .map(|el| el.expect("").path())
            .collect::<Vec<PathBuf>>();
        let final_file_method = match self.file_read_method {
            None => &self.file_read_method_heuristic(&paths),
            Some(ref s) => s,
        };

        let mut final_index = 10_000_000;

        let file_set = DataFileCollection::new_from_path(in_dir);
        let data_set = file_set.datasets::<E>();
        let total_events = if let Some(size) = self.max_events {
            data_set.len().min(size)
        } else {
            data_set.len()
        };
        while final_index < total_events - 1 {
            final_index += 10_000_000;
            let result_chunk = match final_file_method {
                FileReadMethod::Mmap => {
                    let anl = anl_module.read().expect("Failed to get read lock");

                    let num_threads = self.threads;
                    let results = Arc::new(Mutex::new(Vec::<Vec<R>>::with_capacity(num_threads)));

                    println!("Threads:  {}", num_threads);
                    println!("Analyzing: {}", data_set.len());
                    let pbar = MultiProgress::new();
                    //pbar.set_draw_target(indicatif::ProgressDrawTarget::stderr());
                    pbar.set_move_cursor(true);

                    let threaded_result_chunk = |start: usize, stop: usize| {
                        let mut num_since_update = 0;
                        let pb = pbar.add(ProgressBar::new((stop - start) as u64));
                        let update_inteveral = total_events / 1000;
                        let result = data_set
                            .limited_archived_iter(start, stop)
                            .enumerate()
                            .filter(|(idx, event)| anl.filter_event(event, *idx))
                            .flat_map(|(idx, event)| {
                                num_since_update += 1;
                                if num_since_update % update_inteveral == 0 {
                                    pb.inc(num_since_update as u64);
                                    num_since_update = 0;
                                }
                                anl.analyze_event(event, idx)
                            })
                            .collect::<Vec<R>>();

                        results.lock().unwrap().push(result);
                    };

                    let chunk_size = 10_000_000 / num_threads;
                    let remainder = 10_000_000 % num_threads;

                    std::thread::scope(|s| {
                        let mut start = final_index;
                        (0..num_threads).for_each(|thread_idx| {
                            let mut stop = start + chunk_size + 1;
                            if thread_idx < remainder {
                                stop += 1
                            }
                            s.spawn(move || threaded_result_chunk(start, stop));

                            start = stop
                        });
                    });

                    results
                }
                FileReadMethod::AllocBytes => {
                    let num_threads = self.threads;
                    let mut file_data: Vec<Vec<u8>> = vec![vec![]; num_threads];
                    let results: Arc<Mutex<Vec<Vec<R>>>> =
                        Arc::new(Mutex::new(vec![vec![]; num_threads]));
                    let anl = anl_module.read().expect("Failed to get read lock");
                    println!("Threads:  {}", num_threads);
                    println!("Analyzing: {} files", paths.len());
                    struct ShootMyselfInTheFoot(*mut Vec<u8>);
                    unsafe impl Send for ShootMyselfInTheFoot {}
                    unsafe impl Sync for ShootMyselfInTheFoot {}
                    let ptr = ShootMyselfInTheFoot(file_data.as_mut_ptr());
                    let pbar = MultiProgress::new();

                    // Parallelize over paths for now
                    let threaded_result_chunk = |start: usize, stop: usize, thread_idx: usize| {
                        let pb = pbar.add(ProgressBar::new((stop - start) as u64));
                        let result = paths[start..stop]
                            .iter()
                            // .progress_count((stop - start) as u64)
                            .filter(|path| path.to_str().unwrap().ends_with(".rkyv"))
                            .flat_map(|path| {
                                let _ = &ptr;
                                let fdata = unsafe { &mut (*ptr.0.wrapping_add(thread_idx)) };
                                let mut f = File::open(path).unwrap();
                                fdata.clear();
                                f.read_to_end(fdata).unwrap();
                                let data_set = DataSet::<E>::read_from_rkyv(fdata);
                                let res = data_set
                                    .archived_events()
                                    .iter()
                                    .enumerate()
                                    .filter(|(e_ind, e)| anl.filter_event(e, *e_ind))
                                    .filter_map(|(e_ind, e)| anl.analyze_event(e, e_ind))
                                    .collect::<Vec<R>>();
                                pb.inc(1);
                                res
                            })
                            .collect::<Vec<R>>();

                        results.lock().unwrap().push(result);
                    };

                    // TODO: make this divide more evenly among threads
                    let chunk_size = paths.len() / num_threads;
                    let remainder = paths.len() % num_threads;

                    std::thread::scope(|s| {
                        let mut start = 0;
                        (0..num_threads).for_each(|thread_idx| {
                            let mut stop = (start + chunk_size + 1).min(paths.len());
                            if thread_idx < remainder {
                                stop += 1
                            }
                            s.spawn(move || threaded_result_chunk(start, stop, thread_idx));
                            start = stop
                        });
                    });

                    results
                }
            };
            println!("Event loop took {}s", now.elapsed().as_secs_f64());

            AnlMT::<E, R>::make_announcment("HANDLE RESULTS");
            let now = std::time::Instant::now();
            {
                let mut lock = result_chunk.lock().unwrap();
                let mut anl = anl_module.write().expect("Failed to get write lock");
                lock.iter_mut().for_each(|chunk| {
                    anl.handle_result_chunk(chunk);
                });
            }
            println!("Result handling took {}s", now.elapsed().as_secs_f64());
        }

        AnlMT::<E, R>::make_announcment("FINALIZE");
        let now = std::time::Instant::now();
        anl_module
            .write()
            .expect("Failed to get write lock")
            .finalize(self.output_directory());

        println!("Finalizing took {}s", now.elapsed().as_secs_f64());

        AnlMT::<E, R>::make_announcment("DONE");
        println!("Anl took {}s", global_timer.elapsed().as_secs_f64());
    }

    #[allow(dead_code)]
    fn update_real_events(analyzed: usize) {
        println!("Analyzed : {}", analyzed.separate_with_underscores());
    }

    fn make_announcment(text: &str) {
        let s = format!("[[ {} ]]", text).blue().bold();
        println!("{}", s);
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

/// The generic form of an event based analysis module. An `Analysis` or `MixedAnalysis` can take in one or more of
/// these modules and manage the calling of all of these functions for you in a systematic way, or
/// one could use `AnalysisModule`s on their own right for organizational purposes.
pub trait AnlModule<E: Archive>: Send + Sync {
    /// Required name of the module, can be used for naming outputs or keeping track of outputs
    fn name(&self) -> String {
        String::from("anl")
    }

    /// Runs before the event loop. The output directory is passed in case the module generates
    /// output that should be buffered and written during analysis rather than holding on to all of
    /// the data in memory.
    fn initialize(&mut self, _output_directory: Option<&Path>) {}

    /// Pre filter events before event is called. This work could be done in the begining of
    /// `analyze_event`, but this is sometimes cleaner, generally it is better to use an
    /// `EventFilter` though for broad analysis.
    fn filter_event(&self, _event: &<E as Archive>::Archived, _idx: usize) -> bool {
        true
    }

    /// Runs once per event
    fn analyze_event(&mut self, _event: E, _idx: usize) {}

    /// Place to put periodic print statements every once in a while (interval determined by the
    /// user)
    fn post_chunk(&mut self, _output_directory: Option<&Path>) {}

    /// Runs after the event loop
    fn finalize(&mut self, _output_directory: Option<&Path>) {}

    fn output_file_name(
        &self,
        _output_directory: Option<&Path>,
        prefix: &str,
        file_idx: Option<usize>,
    ) -> PathBuf {
        // file management - this sections can be abstracted quite a bit... it's largely boilerplate
        let output_dir = match _output_directory {
            Some(dir) => dir,
            None => &std::env::current_dir().unwrap(),
        };
        let output_dir = output_dir.join(Path::new(&self.name()));
        if !output_dir.is_dir() {
            create_dir_all(&output_dir).unwrap();
        }
        let prefix = if prefix == "" { "file" } else { prefix };

        let file_name = match file_idx {
            Some(file_idx) => format!("{}_{}.parquet", prefix, file_idx),
            None => prefix.into(),
        };

        let file_path = output_dir.join(file_name);

        file_path
    }
}

pub struct Anl<E: Archive> {
    /// The analysis scripts used to analyze the generated events
    anl_module: Option<RefCell<Box<dyn AnlModule<E>>>>,
    /// Where to find the actual input data. This value needs to get set manually, otherwise
    /// `run_analysis` will panic.
    input_directory: Option<PathBuf>,
    /// Where to ouput data to. The data will actually be written to a subdirectory of this
    /// location, using the `self.mixer.name()` as the subdirectory name. This value needs to get
    /// set manually, otherwise `run_analysis` will panic.
    output_directory: Option<PathBuf>,
    //
    update_interval: usize,
    //
    threads: usize,
    //
    max_events: Option<usize>,
    //
    file_read_method: Option<FileReadMethod>,
}

impl<E: Archive> Default for Anl<E> {
    fn default() -> Self {
        Self::new()
    }
}

impl<E: Archive> Anl<E> {
    /// create a new `MixedAnalysis` from a boxed `EventMixer`
    pub fn new() -> Self {
        Anl::<E> {
            anl_module: None,
            max_events: None,
            input_directory: None,
            output_directory: None,
            update_interval: 10_000,
            threads: std::thread::available_parallelism().unwrap().get(),
            file_read_method: None,
        }
    }
    /// Add an anlsysis module to use for the unmixed data. These analysis modules are not
    /// automatically applied to the mixed events
    pub fn with_anl_module(mut self, module: impl AnlModule<E> + 'static) -> Self {
        //pub fn with_anl_module<M:  AnlModule<E, R> + 'static>(mut self, module: M) -> Self {
        let module: RefCell<Box<dyn AnlModule<E>>> = RefCell::new(Box::new(module));
        self.anl_module = Some(module);
        self
    }

    /// A directory containing the raw `rkyv::Archived` data.
    pub fn with_input_directory(mut self, input: &str) -> Self {
        self.input_directory = Some(Path::new(input).to_path_buf());
        self
    }

    pub fn with_num_threads(mut self, threads: usize) -> Self {
        let threads = if threads == 0 {
            std::thread::available_parallelism().unwrap().get()
        } else {
            let max_threads = std::thread::available_parallelism().unwrap().get();
            std::cmp::min(max_threads, threads)
        };
        self.threads = threads;
        self
    }

    /// Directory where the output data should be written.
    /// Subdirectories will be generated within this directory.
    pub fn with_output_directory(mut self, input: &str) -> Self {
        self.output_directory = Some(Path::new(input).to_path_buf());
        self
    }

    /// The number of events between ouputs. The `report` function of analysis modules is called
    /// this often.
    pub fn with_update_interval(mut self, interval: usize) -> Self {
        self.update_interval = interval;
        self
    }

    pub fn with_file_read_method(mut self, method: FileReadMethod) -> Self {
        self.file_read_method = Some(method);
        self
    }

    pub fn with_max_events(mut self, max_events: usize) -> Self {
        self.max_events = Some(max_events);
        self
    }

    pub fn input_directory(&self) -> Option<&Path> {
        self.input_directory.as_deref()
    }

    pub fn output_directory(&self) -> Option<&Path> {
        self.output_directory.as_deref()
    }

    /// Look through the input files to determine which file read method to use.
    /// This is only called if an explicit method is not provided.
    pub fn file_read_method_heuristic(&self, _files: &[PathBuf]) -> FileReadMethod {
        // TODO: come up with a heuristic
        FileReadMethod::Mmap
    }
}

//DataSet<E>: Serialize<
//CompositeSerializer<
//AlignedSerializer<AlignedVec>,
//FallbackScratch<HeapScratch<256>, AllocScratch>,
//SharedSerializeMap,
//>,
//>,
impl<E> Anl<E>
where
    E: Archive,
    <E as Archive>::Archived: Deserialize<E, rkyv::Infallible>,
{
    pub fn run(&mut self) {
        let global_timer = std::time::Instant::now();
        let in_dir = if let Some(path) = self.input_directory() {
            path
        } else {
            panic!("No input directory")
        };

        Anl::<E>::make_announcment("INITIALIZE");
        let now = std::time::Instant::now();

        if self.anl_module.is_none() {
            Anl::<E>::make_announcment("NO MODULE ATTACHED");
            return;
        }

        self.anl_module
            .as_ref()
            .unwrap()
            .borrow_mut()
            .initialize(self.output_directory());

        println!("Initialize took {}s", now.elapsed().as_secs_f64());

        Anl::<E>::make_announcment("EVENT LOOP");

        let now = std::time::Instant::now();

        let mut start_index = 0;
        let mut final_index = 10_000_000;

        let file_set = DataFileCollection::new_from_path(in_dir);
        let data_set = file_set.datasets::<E>();

        let total_events = if let Some(size) = self.max_events {
            data_set.len().min(size)
        } else {
            data_set.len()
        };

        let pb = ProgressBar::new(total_events as u64);

        while start_index < total_events - 1 {
            final_index = final_index.min(total_events - 1);
            let mut anl_module = self.anl_module.as_ref().unwrap().borrow_mut();
            for (idx, event) in data_set
                .limited_archived_iter(start_index, final_index)
                .enumerate()
            {
                let event = event.deserialize(&mut rkyv::Infallible).unwrap();
                anl_module.analyze_event(event, idx + start_index);
                pb.inc(1);
            }
            start_index = final_index;
            final_index += 10_000_000;
            anl_module.post_chunk(self.output_directory());
        }
        pb.finish();

        println!("Event loop took {}s", now.elapsed().as_secs_f64());

        Anl::<E>::make_announcment("HANDLE RESULTS");
        println!("Result handling took {}s", now.elapsed().as_secs_f64());

        Anl::<E>::make_announcment("FINALIZE");

        let now = std::time::Instant::now();

        self.anl_module
            .as_ref()
            .unwrap()
            .borrow_mut()
            .finalize(self.output_directory());

        println!("Finalizing took {}s", now.elapsed().as_secs_f64());

        Anl::<E>::make_announcment("DONE");
        println!("Anl took {}s", global_timer.elapsed().as_secs_f64());
    }

    #[allow(dead_code)]
    fn update_real_events(analyzed: usize) {
        println!("Analyzed : {}", analyzed.separate_with_underscores());
    }

    fn make_announcment(text: &str) {
        let s = format!("[[ {} ]]", text).blue().bold();
        println!("{}", s);
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
