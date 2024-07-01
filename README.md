# EBOOA - Event-Based Object Oriented Analysis 

Designed around the idea that one might have a database of lots of very complex data individual events that need to be analyzed. 

Multithreading and native support for parquet output are definite goals. 

Leveraging rkyv for Zero Copy Deserialization to allow for very fast analysis, especially when only small amounts of the data need to be deserialzed in order to filter the data. 

