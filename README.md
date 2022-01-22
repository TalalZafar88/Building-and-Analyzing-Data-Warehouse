# Building-and-Analysing-Data-Warehouse
The aim of the project was to design, implement, and analyze a Data Warehouse (DW) prototype so that we make analysis of shopping behavior, optimize selling techniques etc. We had to build the warehouse with the data source which we were given. For this we implemented a real time ETL (Extraction, Transformation, and Loading) because the warehouse schema is different from database. We implemented the #Mesh Join# algorithm for integrating the transactional data with master data before loading into warehouse. After building the warehouse I analyzed the DW by applying OLAP queries.

# About MeshJoin Algorithm
The MESHJOIN (Mesh Join) algorithm has been introduced by Polyzotis in 2008 with objective of implementing the join operation in the transformation phase of ETL.

The main components of MESHJOIN are: The disk-buffer which will be an array and used to load the disk partitions in memory. Typically, MD is large, it has to be loaded in memory in partitions. Normally, the size of each partition in MD is equal to the size of the disk-buffer. Also MD is traversed cyclically in an endless loop. The hash table which stores the customers’ transactions (tuples). The queue is used to keep the record of all the customers’ transactions in memory with respect to their arrival times. The queue has same number of partitions as MD to make sure that each tuple has joined with the whole MD before leaving the join operator. The stream-buffer will be an array and is used to hold the customer transaction meanwhile the algorithm completes one iteration.

