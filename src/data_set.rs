#![allow(unused_imports)]
use memmap2::Mmap;
use rand::Rng;
use rkyv::validation::validators::DefaultValidator;
use rkyv::{archived_root, check_archived_root};
use rkyv::{Archive, CheckBytes, Deserialize, Serialize};
use std::fs::File;
use std::marker::PhantomData;
use std::path::Path;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::RwLock;
//
// KYLE

pub type ArchivedData<'a, E> = DataSetCollection<'a, <DataSet<E> as Archive>::Archived, E>;

/// A DataSet is a collection of events of type E. Requires that E is `rkyv::{Archive, Serialize,
/// Deserialize}` to work properly. Generally, one `DataSet<E>` is written per file, and there can be
/// many files written for an actual data set, but that can be managed later within a
/// `DataCollection<E>`
#[derive(Archive, Deserialize, Serialize)]
#[archive(check_bytes)]
pub struct DataSet<E> {
    events: Vec<E>,
}

impl<E> DataSet<E> {
    pub fn new() -> Self {
        Self { events: Vec::new() }
    }

    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }

    pub fn len(&self) -> usize {
        self.events.len()
    }

    pub fn clear(&mut self) {
        self.events.clear()
    }

    pub fn add_event(&mut self, event: E) {
        self.events.push(event);
    }
}

impl<E> Default for DataSet<E> {
    fn default() -> Self {
        Self::new()
    }
}

impl<E> DataSet<E>
where
    E: Archive,
{
    pub fn read_from_rkyv(mmap: &Mmap) -> &rkyv::Archived<DataSet<E>> {
        unsafe { archived_root::<DataSet<E>>(&mmap[..]) }
    }
}

impl<'a, E> DataSet<E>
where
    E: Archive,
    <E as Archive>::Archived: CheckBytes<DefaultValidator<'a>>,
{
    pub fn validated_read_from_rkyv(mmap: &'a Mmap) -> &rkyv::Archived<Self> {
        //let mmap = Rc::clone(&mmap);
        //let mmap = mmap.as_ref();

        check_archived_root::<DataSet<E>>(&mmap[..])
            .expect("There was a problem validating the data")
    }
}

impl<E: Archive> ArchivedDataSet<E> {
    pub fn len(&self) -> usize {
        self.events.len()
    }

    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }

    pub fn archived_events(&self) -> &rkyv::vec::ArchivedVec<<E as Archive>::Archived> {
        &self.events
    }
    pub fn from_map(map: &Mmap) -> &ArchivedDataSet<E> {
        unsafe { rkyv::archived_root::<DataSet<E>>(&map[..]) }
    }

    pub fn from_bytes(bytes: &[u8]) -> &ArchivedDataSet<E> {
        unsafe { rkyv::archived_root::<DataSet<E>>(bytes) }
    }
}

impl<E: Archive> ArchivedDataSet<E> {
    pub fn archived_event_by_idx(&self, idx: usize) -> Option<&<E as Archive>::Archived> {
        if idx >= self.len() {
            None
        } else {
            Some(&self.events[idx])
        }
    }

    pub fn archived_event_by_idx_unchecked(&self, idx: usize) -> &<E as Archive>::Archived {
        &self.events[idx]
    }
}

impl<E: Archive> ArchivedDataSet<E>
where
    <E as Archive>::Archived: Deserialize<E, rkyv::Infallible>,
{
    pub fn event_by_idx(&self, idx: usize) -> Option<E> {
        if idx >= self.len() {
            None
        } else {
            Some(self.events[idx].deserialize(&mut rkyv::Infallible).unwrap())
        }
    }

    pub fn random_event(&self) -> Option<E> {
        self.event_by_idx(rand::thread_rng().gen())
    }
}

pub struct DataSetCollection<'a, D, E> {
    data_sets: Vec<&'a D>,
    mem_maps: Vec<Mmap>,
    num_accumulated_events: Vec<usize>,
    phantom: PhantomData<E>,
}

impl<'a, E: Archive> DataSetCollection<'a, <DataSet<E> as Archive>::Archived, E> {
    pub fn new(mem_maps: Vec<Mmap>) -> DataSetCollection<'a, <DataSet<E> as Archive>::Archived, E> {
        let data_sets = Vec::new();
        let num_accumulated_events = Vec::new();

        //let mut result = Self {
        //data_sets: Vec::new(),
        //num_accumulated_events: Vec::new(),
        //mem_maps: mem_maps,
        //phantom: PhantomData,
        //};

        //let mut num_events = 0;
        //for m in mem_maps.iter() {
        ////for m in result.mem_maps.iter() {
        //let ds: &ArchivedDataSet<E> = unsafe { rkyv::archived_root::<DataSet<E>>(m) };
        //num_events += ds.len();
        //data_sets.push(ds);
        //num_accumulated_events.push(num_events);
        //}

        Self {
            data_sets,
            mem_maps,
            num_accumulated_events,
            phantom: PhantomData,
        }
    }

    pub fn init(&'a mut self) {
        let mut num_events = 0;
        for m in self.mem_maps.iter() {
            let ds: &ArchivedDataSet<E> = unsafe { rkyv::archived_root::<DataSet<E>>(m) };
            num_events += ds.len();
            self.data_sets.push(ds);
            self.num_accumulated_events.push(num_events);
        }
    }

    pub fn new_from_path(directory: &Path) -> Self {
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

            let memory_map = unsafe {
                Mmap::map_raw(&input_file).expect("Input file could not be memory mapped")
            };
            mmaps.push(memory_map);
        }

        Self::new(mmaps)
    }

    pub fn len(&self) -> usize {
        self.data_sets.iter().map(|ds| ds.len()).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.data_sets.is_empty()
    }

    pub fn num_sets(&self) -> usize {
        self.data_sets.len()
    }

    pub fn data_set_by_idx(&self, idx: usize) -> Option<&'a ArchivedDataSet<E>> {
        if idx >= self.num_sets() {
            None
        } else {
            Some(self.data_sets[idx])
        }
    }
}

impl<'a, E: Archive> DataSetCollection<'a, <DataSet<E> as Archive>::Archived, E>
where
    <E as Archive>::Archived: Deserialize<E, rkyv::Infallible>,
{
    fn unchecked_data_set_idx(&self, idx: usize, low: usize, high: usize) -> usize {
        let mid = (high + low) / 2;

        if low == high {
            return low;
        }

        if idx >= self.num_accumulated_events[mid] {
            self.unchecked_data_set_idx(idx, mid + 1, high)
        } else {
            self.unchecked_data_set_idx(idx, low, mid)
        }
    }
    fn binary_unchecked_data_set_idx(&self, idx: usize) -> usize {
        self.unchecked_data_set_idx(idx, 0, self.num_accumulated_events.len() - 1)
    }

    pub fn events_by_idxs(&self, idx_1: usize, idx_2: usize) -> Vec<E> {
        let ds_idx_1 = self.binary_unchecked_data_set_idx(idx_1);
        let ds_idx_2 = self.binary_unchecked_data_set_idx(idx_2 - 1);

        if ds_idx_1 == ds_idx_2 {
            let mut event_idx_1 = idx_1;
            let mut event_idx_2 = idx_2;
            if ds_idx_1 > 0 {
                event_idx_1 -= self.num_accumulated_events[ds_idx_1 - 1];
                event_idx_2 -= self.num_accumulated_events[ds_idx_1 - 1];
            }
            let idx_range = event_idx_1..event_idx_2;
            let ds_idx = ds_idx_1;

            let ds = self.data_sets[ds_idx];
            idx_range
                .map(|event_idx| {
                    let archived = &ds.archived_events()[event_idx];
                    let event: E = archived
                        .deserialize(&mut rkyv::Infallible)
                        .expect("Issue deserializing the event from its archived form");
                    event
                })
                .collect()
        } else {
            (idx_1..idx_2)
                .map(|idx| self.event_by_idx(idx).unwrap())
                .collect()
        }
    }
    pub fn event_by_idx(&self, idx: usize) -> Option<E> {
        let ds_idx = self.binary_unchecked_data_set_idx(idx);
        let mut event_idx = idx;
        if ds_idx > 0 {
            event_idx -= self.num_accumulated_events[ds_idx - 1];
        }
        let ds = self.data_sets[ds_idx];
        let archived = &ds.archived_events()[event_idx];

        let event: E = archived
            .deserialize(&mut rkyv::Infallible)
            .expect("Issue deserializing the event from its archived form");

        Some(event)
    }
    pub fn random_event(&self) -> E {
        self.event_by_idx(rand::thread_rng().gen_range(0..self.len()))
            .unwrap()
    }
    pub fn archived_event_by_idx(&self, idx: usize) -> Option<&<E as Archive>::Archived> {
        let mut starting_idx = 0;
        for ds_idx in 0..self.num_sets() {
            let ds = &self.data_sets[ds_idx];
            let events_in_ds = ds.len();
            if events_in_ds + starting_idx > idx {
                let archived = &ds.archived_events()[idx - starting_idx];

                return Some(archived);
            } else {
                starting_idx += events_in_ds;
            }
        }
        None
    }

    pub fn iter(&'a self) -> DataCollectionIter<'a, E> {
        DataCollectionIter::<E>::new(self)
    }

    pub fn archived_iter<'b>(&'a self) -> ArchivedDataCollectionIter<'b, E>
    where
        'a: 'b,
    {
        ArchivedDataCollectionIter::<E>::new(self)
    }

    pub fn limited_archived_iter(
        &'a self,
        start: usize,
        stop: usize,
    ) -> LimitedArchivedDataCollectionIter<'a, E> {
        LimitedArchivedDataCollectionIter::<E>::new(self, start, stop)
    }
}

pub struct DataCollectionIter<'a, E: Archive>
where
    <E as Archive>::Archived: Deserialize<E, rkyv::Infallible>,
{
    data: &'a DataSetCollection<'a, <DataSet<E> as Archive>::Archived, E>,
    data_set: Option<&'a ArchivedDataSet<E>>,
    data_set_idx: usize,
    idx: usize,
}

impl<'a, E: Archive> DataCollectionIter<'a, E>
where
    <E as Archive>::Archived: Deserialize<E, rkyv::Infallible>,
{
    pub fn new(data: &'a ArchivedDataSet<E>) -> Self {
        //pub fn new(data: &'a DataSetCollection<'a, <DataSet<E> as Archive>::Archived, E>) -> Self {
        Self {
            data,
            data_set: data.data_set_by_idx(0),
            data_set_idx: 0,
            idx: 0,
        }
    }
}

impl<'a, E> Iterator for DataCollectionIter<'a, E>
where
    E: Archive,
    <E as Archive>::Archived: Deserialize<E, rkyv::Infallible>,
{
    type Item = E;

    fn next(&mut self) -> Option<Self::Item> {
        if self.data_set_idx >= self.data.num_sets() {
            return None;
        }

        self.data_set?;

        if self.idx >= self.data_set.unwrap().len() {
            self.idx = 0;
            self.data_set_idx += 1;
            self.data_set = self.data.data_set_by_idx(self.data_set_idx);
            match self.data_set {
                None => None,
                Some(_) => self.next(),
            }
        } else {
            let idx = self.idx;
            self.idx += 1;
            let archived = self.data_set.unwrap().archived_events().get(idx).unwrap();
            Some(
                archived
                    .deserialize(&mut rkyv::Infallible)
                    .expect("Issue deserializing from archived form"),
            )
        }
    }
}

pub struct ArchivedDataCollectionIter<'a, E: Archive>
where
    <E as Archive>::Archived: Deserialize<E, rkyv::Infallible>,
{
    data: &'a DataSetCollection<'a, <DataSet<E> as Archive>::Archived, E>,
    data_set: Option<&'a ArchivedDataSet<E>>,
    data_set_idx: usize,
    idx: usize,
}

impl<'a, E: Archive> ArchivedDataCollectionIter<'a, E>
where
    <E as Archive>::Archived: Deserialize<E, rkyv::Infallible>,
{
    pub fn new(data: &'a DataSetCollection<'a, <DataSet<E> as Archive>::Archived, E>) -> Self {
        Self {
            data,
            data_set: data.data_set_by_idx(0),
            data_set_idx: 0,
            idx: 0,
        }
    }
}

impl<'a, E> Iterator for ArchivedDataCollectionIter<'a, E>
where
    E: Archive,
    <E as Archive>::Archived: Deserialize<E, rkyv::Infallible>,
{
    type Item = &'a <E as Archive>::Archived;

    fn next(&mut self) -> Option<Self::Item> {
        if self.data_set_idx >= self.data.num_sets() {
            return None;
        }

        self.data_set?;

        if self.idx >= self.data_set.unwrap().len() {
            self.idx = 0;
            self.data_set_idx += 1;
            self.data_set = self.data.data_set_by_idx(self.data_set_idx);
            match self.data_set {
                None => None,
                Some(_) => self.next(),
            }
        } else {
            let idx = self.idx;
            self.idx += 1;
            Some(self.data_set.unwrap().archived_events().get(idx).unwrap())
        }
    }
}

pub struct LimitedArchivedDataCollectionIter<'a, E: Archive>
where
    <E as Archive>::Archived: Deserialize<E, rkyv::Infallible>,
{
    data: &'a DataSetCollection<'a, <DataSet<E> as Archive>::Archived, E>,
    data_set: Option<&'a ArchivedDataSet<E>>,
    data_set_idx: usize,
    idx: usize,
    stop_idx: usize,
}

impl<'a, E: Archive> LimitedArchivedDataCollectionIter<'a, E>
where
    <E as Archive>::Archived: Deserialize<E, rkyv::Infallible>,
{
    pub fn new(
        data: &'a DataSetCollection<'a, <DataSet<E> as Archive>::Archived, E>,
        start_idx: usize,
        stop_idx: usize,
    ) -> Self {
        Self {
            data,
            data_set: data.data_set_by_idx(0),
            data_set_idx: 0,
            idx: start_idx,
            stop_idx: std::cmp::min(stop_idx, data.len()),
        }
    }
}

impl<'a, E> Iterator for LimitedArchivedDataCollectionIter<'a, E>
where
    E: Archive,
    <E as Archive>::Archived: Deserialize<E, rkyv::Infallible>,
{
    type Item = (&'a <E as Archive>::Archived, usize);

    fn next(&mut self) -> Option<Self::Item> {
        if self.data_set_idx >= self.data.num_sets() {
            return None;
        }

        self.data_set?;

        if self.idx == self.stop_idx {
            return None;
        }

        if self.idx >= self.data_set.unwrap().len() {
            self.idx = 0;
            self.data_set_idx += 1;
            self.data_set = self.data.data_set_by_idx(self.data_set_idx);
            match self.data_set {
                None => None,
                Some(_) => self.next(),
            }
        } else {
            let idx = self.idx;
            self.idx += 1;
            Some((
                self.data_set.unwrap().archived_events().get(idx).unwrap(),
                idx,
            ))
        }
    }
}
