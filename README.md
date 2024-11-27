DBx1000-IndexOrganized
=======

DBx1000-IndexOrganized is developed based on DBx1000: https://github.com/yxymit/DBx1000.

DBx1000 is an single node OLTP database management system (DBMS). 
The goal of DBx1000 is to make DBMS scalable on future 1000-core processors. 
We implemented all the seven classic concurrency control schemes in DBx1000. They exhibit different scalability properties under different workloads. 

The concurrency control scalability study is described in the following paper. 

    Staring into the Abyss: An Evaluation of Concurrency Control with One Thousand Cores
    Xiangyao Yu, George Bezerra, Andrew Pavlo, Srinivas Devadas, Michael Stonebraker
    http://www.vldb.org/pvldb/vol8/p209-yu.pdf
    
The major changes made in this repository:

  - added support for index organization and optimized its performance.
  
  - developed a B-tree index to improve support for scan queries.
  
  - implemented an MDP-based approach to generate optimized configurations in an atomic manner.
  
  - enhanced the TPCC benchmark implementation to enable comprehensive evaluation and assessment.

Build & Test
------------

To build the database.

    make -j

To test the database

    ./rundb
    
Configuration
-------------

DBMS configurations can be changed in the config.h file. Please refer to README for the meaning of each configuration. Here we only list several most important ones. 

    THREAD_CNT        : Number of worker threads running in the database.
    WORKLOAD          : Supported workloads include YCSB and TPCC
    CC_ALG            : Concurrency control algorithm. Seven algorithms are supported 
                        (DL_DETECT, NO_WAIT, HEKATON, SILO, TICTOC) 
    MAX_TXN_PER_PART  : Number of transactions to run per thread per partition.
                        
Configurations can also be specified as command argument at runtime. Run the following command for a full list of program argument. 
    
    ./rundb -h

