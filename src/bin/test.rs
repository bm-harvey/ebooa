use clap::Parser;
use ebooa::anl_module::AnlMT;
use ebooa::anl_module::AnlModuleMT;
use ebooa::anl_module::FileReadMethod;
use ebooa::example_event::MyEvent;
use indicatif::ParallelProgressIterator;
use rayon::prelude::*;
use std::fs::File;
use std::io::Write;

#[derive(Default)]
struct TestAnlMod {
    results: Vec<Res>,
}

#[derive(Clone)]
pub struct Res {
    pub trigger: u16,
    pub momentum: f64,
}

impl AnlModuleMT<MyEvent, Res> for TestAnlMod {
    fn name(&self) -> String {
        String::from("test")
    }

    fn filter_event(&self, _event: &<MyEvent as rkyv::Archive>::Archived, _idx: usize) -> bool {
        //_event.particles().len() == 2
        true
    }

    fn analyze_event(
        &self,
        event: &<MyEvent as rkyv::Archive>::Archived,
        _idx: usize,
    ) -> Option<Res> {
        let event = event.event();

        let trigger = event.trigger();
        let momentum = event
            .particles()
            .iter()
            .map(|measured_particle| measured_particle.physical_particle())
            .map(|physical_particle| physical_particle.momentum())
            .map(|momentum| momentum.mag_3())
            .map(|momentum_mag| momentum_mag.powi(2))
            .sum::<f64>();

        Some(Res { trigger, momentum })
    }

    fn handle_result_chunk(&mut self, results: &mut Vec<Res>) {
        self.results.append(results);
    }

    fn finalize(&mut self, _output_directory: Option<&std::path::Path>) {
        println!("there were {}, triggers.", self.results.len());

        let mean_trigger =
            self.results.iter().map(|r| r.trigger as f64).sum::<f64>() / self.results.len() as f64;

        let mean_momentum =
            self.results.iter().map(|r| r.momentum).sum::<f64>() / self.results.len() as f64;

        println!("the average trigger was: {}.", mean_trigger);
        println!("the average momentum was: {}.", mean_momentum);
    }
}

#[derive(Parser)]
#[command(version)]
struct Args {
    #[arg(short, long)]
    data_dir: String,

    #[arg(short, long, default_value_t = false)]
    gen_data: bool,

    #[arg(short, long, default_value_t = 1_000)]
    files: usize,

    #[arg(short, long)]
    file_method: Option<FileReadMethod>,

    #[arg(short, long, default_value_t = 100_000)]
    events: usize,

    #[arg(short = 'j', default_value_t = 0)]
    threads: usize,
}

fn main() {
    let args = Args::parse();

    // generate files
    let now = std::time::Instant::now();
    if args.gen_data {
        let num_files = args.files;
        let num_events = args.events;

        (0..num_files)
            .into_par_iter()
            .progress()
            .for_each(|file_idx| {
                let mut events = Vec::with_capacity(num_events);
                for _event_idx in 0..num_events {
                    events.push(MyEvent::random());
                }

                let file_name = format!("{}/file_{}.rkyv", args.data_dir, file_idx);
                let mut file = File::create(file_name).unwrap();
                file.write_all(&rkyv::to_bytes::<_, 100_000_000>(&events).unwrap())
                    .unwrap();
            });
    }
    println!("Time to write files: {}s", now.elapsed().as_secs_f64());

    // read back files ... this is the stuff we are trying to make very very fast
    let module = TestAnlMod::default();
    let mut anl = AnlMT::<MyEvent, Res>::new()
        .with_input_directory(&args.data_dir)
        .with_anl_module(module)
        .with_num_threads(args.threads);
    if let Some(method) = args.file_method {
        anl = anl.with_file_read_method(method);
    }
    anl.run();
}
