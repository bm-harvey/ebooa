use memmap2::Mmap;
use rand::Rng;
use rkyv::validation::validators::DefaultValidator;
use rkyv::{archived_root, check_archived_root, Archived};
use rkyv::{Archive, CheckBytes, Deserialize, Serialize};
use std::fs::File;
use std::path::Path;
//use rkyv::{Archive, Deserialize, Serialize};

#[derive(Archive, Deserialize, Serialize)]
#[archive(check_bytes)]
pub struct Event;

#[derive(Archive, Deserialize, Serialize)]
#[archive(check_bytes)]
#[derive(Default)]
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

impl<E> DataSet<E>
where
    E: Archive,
{
    //pub fn read_from_rkyv(mmap: &Mmap) -> &rkyv::Archived<DataSet<E>> {
    //unsafe { archived_root::<DataSet<E>>(&mmap[..]) }
    //}
    pub fn read_from_rkyv(bytes: &[u8]) -> &rkyv::Archived<DataSet<E>> {
        unsafe { archived_root::<DataSet<E>>(bytes) }
    }
}

impl<'a, E> DataSet<E> {
    pub fn validated_read_from_rkyv(bytes: &'a [u8]) -> &'a rkyv::Archived<Self>
    where
        E: Archive,
        Archived<E>: CheckBytes<DefaultValidator<'a>>,
    {
        check_archived_root::<DataSet<E>>(bytes).expect("There was a problem validating the data")
    }
}

impl<E> ArchivedDataSet<E>
where
    E: Archive,
{
    pub fn len(&self) -> usize {
        self.events.len()
    }

    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }

    pub fn archived_events(&self) -> &rkyv::vec::ArchivedVec<Archived<E>> {
        &self.events
    }

    pub fn from_map(map: &Mmap) -> &ArchivedDataSet<E> {
        unsafe { rkyv::archived_root::<DataSet<E>>(&map[..]) }
    }

    pub fn from_bytes(bytes: &[u8]) -> &ArchivedDataSet<E> {
        unsafe { rkyv::archived_root::<DataSet<E>>(bytes) }
    }
}

impl<E> ArchivedDataSet<E>
where
    E: Archive,
{
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

impl<E> ArchivedDataSet<E>
where
    E: Archive,
    Archived<E>: Deserialize<E, rkyv::Infallible>,
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

pub struct DataFileCollection {
    mem_maps: Vec<Mmap>,
}

impl DataFileCollection {
    pub fn new(mem_maps: Vec<Mmap>) -> DataFileCollection {
        Self { mem_maps }
    }

    pub fn new_from_path(directory: &Path) -> Self {
        let mut mmaps = Vec::new();

        let paths = std::fs::read_dir(directory).expect("problem opening input directory");
        for path in paths {
            let path = path.expect("Ivalid path").path();

            let path_name = path
                .to_str()
                .expect("Path could not be interpretted as str");

            // if the file is not marked as an rkyv file, don't try to read it as one
            let length = path_name.len();
            if !path_name[length - 5..].contains(".rkyv") {
                continue;
            }

            let input_file = File::open(path_name).expect("File could not be found");

            let memory_map =
                unsafe { Mmap::map(&input_file).expect("Input file could not be memory mapped") };
            mmaps.push(memory_map);
        }

        Self::new(mmaps)
    }

    pub fn datasets<E>(&self) -> DataSetCollection<E>
    where
        E: Archive,
    {
        let mut num_events = 0;
        let mut data_sets = Vec::new();
        let mut num_accumulated_events = Vec::new();
        for m in self.mem_maps.iter() {
            let ds: &ArchivedDataSet<E> = DataSet::<E>::read_from_rkyv(m);
            num_events += ds.len();
            data_sets.push(ds);
            num_accumulated_events.push(num_events);
        }

        DataSetCollection {
            data_sets,
            //num_accumulated_events,
        }
    }
}

pub struct DataSetCollection<'a, E>
where
    E: Archive,
{
    data_sets: Vec<&'a ArchivedDataSet<E>>,
    //num_accumulated_events: Vec<usize>,
}

impl<'a, E> DataSetCollection<'a, E>
where
    E: Archive,
{
    pub fn num_sets(&self) -> usize {
        self.data_sets.len()
    }

    pub fn len(&self) -> usize {
        self.data_sets.iter().map(|ds| ds.len()).sum::<usize>()
    }

    pub fn is_empty(&self) -> bool {
        self.data_sets.is_empty() || self.data_sets.iter().all(|ds| ds.is_empty())
    }

    pub fn data_set_by_idx(&self, idx: usize) -> Option<&'a ArchivedDataSet<E>> {
        if idx >= self.num_sets() {
            None
        } else {
            Some(self.data_sets[idx])
        }
    }
}

impl<'a, E> DataSetCollection<'a, E>
where
    E: Archive,
{
    pub fn archived_iter(&'a self) -> ArchivedDataCollectionIter<'a, E> {
        ArchivedDataCollectionIter::new(self)
    }

    pub fn limited_archived_iter(
        &'a self,
        start: usize,
        stop: usize,
    ) -> LimitedArchivedDataCollectionIter<'a, E> {
        LimitedArchivedDataCollectionIter::new(self, start, stop)
    }
}

pub struct ArchivedDataCollectionIter<'a, E: Archive> {
    data: &'a DataSetCollection<'a, E>,
    current_data_set: Option<&'a ArchivedDataSet<E>>,
    data_set_idx: usize,
    idx: usize,
}
impl<'a, E: Archive> ArchivedDataCollectionIter<'a, E> {
    pub fn new(data: &'a DataSetCollection<E>) -> Self {
        Self {
            data,
            current_data_set: data.data_set_by_idx(0),
            data_set_idx: 0,
            idx: 0,
        }
    }
}

impl<'a, E> Iterator for ArchivedDataCollectionIter<'a, E>
where
    E: Archive,
{
    type Item = &'a <E as Archive>::Archived;

    fn next(&mut self) -> Option<Self::Item> {
        if self.data_set_idx >= self.data.num_sets() {
            return None;
        }

        self.current_data_set?;

        if self.idx >= self.current_data_set.unwrap().len() {
            self.idx = 0;
            self.data_set_idx += 1;
            self.current_data_set = self.data.data_set_by_idx(self.data_set_idx);
            match self.current_data_set {
                None => None,
                Some(_) => self.next(),
            }
        } else {
            let idx = self.idx;
            self.idx += 1;
            Some(
                self.current_data_set
                    .unwrap()
                    .archived_events()
                    .get(idx)
                    .unwrap(),
            )
        }
    }
}

pub struct LimitedArchivedDataCollectionIter<'a, E: Archive> {
    data: &'a DataSetCollection<'a, E>,
    data_set: Option<&'a ArchivedDataSet<E>>,
    data_set_idx: usize,
    idx: usize,
    reverse_counter: usize,
}

impl<'a, E: Archive> LimitedArchivedDataCollectionIter<'a, E> {
    pub fn new(data: &'a DataSetCollection<'a, E>, start_idx: usize, stop_idx: usize) -> Self {
        // idx needs to be the idx of the event for that file
        // data_set_idx needs to be calculated

        let (data_set_idx, num_events_in_prev) = {
            let mut result = (0, 0);
            let mut num_events_in_prev = 0;
            for data_set_idx in 0..data.num_sets() {
                let ds = data
                    .data_set_by_idx(data_set_idx)
                    .expect("Tried to reach a dataset that doesn't exist");
                let num_events_in_this = ds.len();
                if num_events_in_prev + num_events_in_this > start_idx {
                    result = (data_set_idx, num_events_in_prev);
                    break;
                }
                num_events_in_prev += num_events_in_this;
            }
            result
        };

        let count = stop_idx - start_idx;
        let idx = start_idx - num_events_in_prev;
        Self {
            data,
            data_set: data.data_set_by_idx(data_set_idx),
            data_set_idx,
            idx,
            reverse_counter: count,
        }
    }
}

impl<'a, E> Iterator for LimitedArchivedDataCollectionIter<'a, E>
where
    E: Archive,
{
    type Item = &'a <E as Archive>::Archived;

    fn next(&mut self) -> Option<Self::Item> {
        if self.reverse_counter == 0 {
            return None;
        }

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
            self.reverse_counter -= 1;
            Some(self.data_set.unwrap().archived_events().get(idx).unwrap())
        }
    }
}
