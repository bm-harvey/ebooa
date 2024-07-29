use ebooa::anl_module::Anl;
use ebooa::anl_module::AnlModule;
use ebooa::example_event::{MyEvent, Res};
//use std::sync::Arc;
//use std::sync::RwLock;

#[derive(Default)]
struct TestAnlMod;
impl AnlModule<MyEvent, Res> for TestAnlMod {
    fn name(&self) -> String {
        String::from("test")
    }

    fn analyze_event(
        &self,
        _event: &<MyEvent as rkyv::Archive>::Archived,
        _idx: usize,
    ) -> Option<Res> {
        let trigger = _event.trigger;

        Some(Res { trigger })
    }

    fn handle_result_chunk(&mut self, _results: &mut [Res]) {
        // do nothing ... but probably write these out to file
    }
}

fn main() {
    let  module = TestAnlMod::default();
    //let mut module = Arc::new(RwLock::new(TestAnlMod::default()));

    let mut anl = Anl::<MyEvent, Res>::new()
        .with_input_directory("/path/to/input")
        .with_output_directory("/path/to/output")
        .with_anl_module(module);

    anl.run();
}
