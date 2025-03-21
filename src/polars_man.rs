//#![allow(dead_code)]
//#![allow(unused_imports)]

use polars::prelude::*;
//use rand::Rng;

use std::{cell::RefCell, rc::Rc};

pub type Column<T> = Rc<RefCell<TypedColumnData<T>>>;

#[derive(Default, Debug)]
pub struct TypedColumnData<T> {
    name: String,
    column: Vec<Option<T>>,
    handle: Option<T>,
}

/// T should be convertible to a PolarsDataType
impl<T> TypedColumnData<T> {
    fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            column: Vec::<Option<T>>::new(),
            handle: None,
        }
    }

    //fn fill(&mut self) {
        //self.column.push(self.handle.take());
    //}

    pub fn set_val(&mut self, val: T) {
        self.handle = Some(val);
    }
    pub fn set_null(&mut self) {
        self.handle = None;
    }

    pub fn handle_mut(&mut self) -> &mut Option<T> {
        &mut self.handle
    }

    //fn iter(&self) -> std::slice::Iter<Option<T>> {
        //self.column.iter()
    //}

    //fn iter_mut(&mut self) -> std::slice::IterMut<Option<T>> {
        //self.column.iter_mut()
    //}

    //fn into_iter(self) -> std::vec::IntoIter<Option<T>> {
        //self.column.into_iter()
    //}

    fn len(&self) -> usize {
        self.column.len()
    }
}

pub fn set_col<T>(col: &Rc<RefCell<TypedColumnData<T>>>, val: T) {
    col.borrow_mut().set_val(val);
}

pub trait ManagedColumn {
    fn as_series(&self) -> Series;
    fn name(&self) -> &str;
    fn fill(&mut self);
    fn clear(&mut self);
    fn set_null(&mut self);
    fn len(&self) -> usize;
    fn mem_usage(&self) -> usize;
    //fn set<T>(&mut self, val: T);
}

impl<T> ManagedColumn for TypedColumnData<T>
where
    Series: NamedFrom<Vec<Option<T>>, [Option<T>]>,
    T: Clone,
{
    fn as_series(&self) -> Series {
        let name = self.name.clone();
        let data = self.column.clone();
        Series::new(name.into(), data)
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn fill(&mut self) {
        self.column.push(self.handle.take());
    }

    fn set_null(&mut self) {
        self.set_null();
    }

    fn clear(&mut self) {
        self.column.clear();
    }

    fn len(&self) -> usize {
        self.len()
    }

    fn mem_usage(&self) -> usize {
        std::mem::size_of::<T>() * self.column.len()
    }
}

pub struct ManagedDataFrame {
    columns: Vec<Rc<RefCell<dyn ManagedColumn>>>,
}

impl ManagedDataFrame {
    pub fn new() -> Self {
        Self {
            columns: Vec::new(),
        }
    }

    pub fn attach_column<T>(&mut self, name: &str) -> Rc<RefCell<TypedColumnData<T>>>
    where
        T: Clone + 'static,
        Series: NamedFrom<Vec<Option<T>>, [Option<T>]>,
    {
        let col = Rc::new(RefCell::new(TypedColumnData::<T>::new(name)));
        let dyn_col = col.clone();

        self.columns.push(dyn_col);
        col
    }

    pub fn fill(&mut self) {
        //dbg!();
        for col in self.columns.iter_mut() {
            let mut col = col.borrow_mut();
            col.fill();
            col.set_null();
        }
    }

    pub fn estimated_memory_usage(&self) -> usize {
        self.columns
            .iter()
            .map(|col| col.borrow().mem_usage())
            .sum()
    }

    fn df(&self) -> DataFrame {
        let mut df = DataFrame::default();
        for col in self.columns.iter() {
            let col = col.borrow().as_series();
            df.with_column(col).unwrap();
        }
        df
    }

    pub fn len(&self) -> usize {
        if let Some(col) = self.columns.last() {
            col.borrow().len()
        } else {
            0
        }
    }

    pub fn write_to_parquet(&self, path: &str) {
        let mut df = self.df();
        let write_location = path;
        let wtr = std::fs::File::create(write_location).unwrap();

        //let level = ZstdLevel::try_new(1).unwrap();
        //let compression = ParquetCompression::Zstd(Some(level));

        ParquetWriter::new(wtr)
            //.with_compression(compression)
            .finish(&mut df)
            .unwrap();
    }
}
