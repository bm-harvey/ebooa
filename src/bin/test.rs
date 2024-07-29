use clap::Parser;
use ebooa::anl_module::Anl;
use ebooa::anl_module::AnlModule;
use ebooa::example_event::MyEvent;
//use rayon::iter::IntoParallelRefIterator;
//use std::sync::Arc;
//use std::sync::RwLock;

#[derive(Default)]
struct TestAnlMod {
    results: Vec<Res>,
}

pub struct Res {
    pub trigger: u16,
    pub momentum: f64,
}

impl AnlModule<MyEvent, Res> for TestAnlMod {
    fn name(&self) -> String {
        String::from("test")
    }

    fn filter_event(&self, event: &<MyEvent as rkyv::Archive>::Archived, _idx: usize) -> bool {
        event.particles().len() == 2
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
            .map(|mp| mp.physical_particle())
            .map(|pp| pp.momentum())
            .map(|mom| mom.mag_3())
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
            self.results.iter().map(|r| r.momentum as f64).sum::<f64>() / self.results.len() as f64;

        println!("the average trigger was: {}.", mean_trigger);
        println!("the average momentum was: {}.", mean_momentum);
    }
}

#[derive(Parser)]
#[command(version)]
struct Args {
    #[arg(short, long)]
    data_dir: String,
}

fn main() {
    let args = Args::parse();

    //let num_files = 1000;
    //let num_events = 100_000;

    //for file_idx in (0..num_files).progress() {
    //let mut events = Vec::with_capacity(num_events);
    //for _event_idx in 0..num_events {
    //events.push(MyEvent::random());
    //}

    //let file_name = format!("{}/file_{}.rkyv", data_dir, file_idx);
    //let mut file = File::create(&file_name).unwrap();
    //file.write(&rkyv::to_bytes::<_, 256>(&events).unwrap())
    //.unwrap();
    //}

    let module = TestAnlMod::default();
    Anl::<MyEvent, Res>::new()
        .with_input_directory(&args.data_dir)
        .with_anl_module(module)
        .run();
}
