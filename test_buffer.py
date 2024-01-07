from string import Template
from dataclasses import dataclass
import os
import subprocess

# import matplotlib.pyplot as plt
import csv


def read_file_into_string(file_path):
    with open(file_path, 'r') as file:
        file_contents = file.read()
    return file_contents


@dataclass
class Config:
    thread_cnt: int
    workload: str
    cc_alg: str
    index_struct: str
    max_txn_per_part: int
    max_tuple_size: int
    synth_table_size: int
    zipf_theta: float
    read_perc: float
    write_perc: float
    insert_perc: float
    aggressive_inling: int
    buffering: int
    message_count: int
    dram_block_size: int
    split_threshold: int


def initiation(conf):
    template_path = 'config.h.template'
    template = Template(read_file_into_string(template_path))
    return template.substitute(
        thread_cnt=conf.thread_cnt,
        workload=conf.workload,
        cc_alg=conf.cc_alg,
        index_struct=conf.index_struct,
        max_txn_per_part=conf.max_txn_per_part,
        max_tuple_size=conf.max_tuple_size,
        synth_table_size=conf.synth_table_size,
        zipf_theta=conf.zipf_theta,
        read_perc=conf.read_perc,
        write_perc=conf.write_perc,
        insert_perc=conf.insert_perc,
        aggressive_inling=conf.aggressive_inling,
        buffering=conf.buffering,
        message_count=conf.message_count,
        dram_block_size=conf.dram_block_size,
        split_threshold=conf.split_threshold)


def result_filename(tuple_sz, leaf_sz):
    return f'test_result/result_10Mdata_1Mops_{tuple_sz}_{leaf_sz}.csv'


def run_test(test_config, write_type, tuple_sz, leaf_sz):
    with open('config.h', 'w') as file:
        config_h = initiation(test_config)
        file.write(config_h)
    os.system('rm -rf build && mkdir build && cd build && cmake -DCMAKE_BUILD_TYPE=Release .. && make -j`nproc`')

    # 本次实验结果写到哪个文件里
    filename = result_filename(tuple_sz, leaf_sz)
    # 运行的结果(有返回值等信息)
    run_result = subprocess.run(['./build/rundb'], stdout=subprocess.PIPE)
    # 本次实验的输出
    output = run_result.stdout.decode('utf-8')
    # 第一行summary和第二行summary
    summary1, summary2 = [line for line in output.split('\n') if line.startswith('[summary]')]
    # 完整的行
    summary1 = summary1[10:]
    summary2 = summary2[10:]

    summary1_items = [kvpair.split('=')[1] for kvpair in summary1.split(', ')]
    all_summary = summary1_items[:3]
    all_summary.append(summary2.split('=')[1].strip())
    # all_summary = summary1_needed+'\n'+summary2+'\n'
    header = 'txn_cnt,abort_cnt,run_time,throughput(txn/s)'

    if write_type == 'wr':
        with open(filename, 'w') as file:
            file.write(header)
            file.write('\n')
            file.write(', '.join(all_summary))
            file.write('\n')
    else:
        with open(filename, 'a') as file:
            file.write('\n')
            file.write(', '.join(all_summary))
            file.write('\n')

    print(header)
    print(','.join(all_summary))


# zif, read,write,scan,insert, scan_length, operation_per_query
# node size = 15 entries
# record size

# config1  =  Config( 1, 'YCSB', 'HEKATON', 'IndexBtree', 1024*1024*4, 1000,   1024*1024, 0.0, 1.0,0.0,0.0, 0, 0, 0, 440, 456) #select
# config2  =  Config( 1, 'YCSB', 'HEKATON', 'IndexBtree', 1024*1024*4, 1000,   1024*1024, 0.0, 0.5,0.0,0.5, 0, 0, 0, 440, 456) #select/insert
# config3  =  Config( 1, 'YCSB', 'HEKATON', 'IndexBtree', 1024*1024*4, 1000,   1024*1024, 0.0, 0.5,0.5,0.0, 0, 0, 0, 440, 456) #select/update
# config4  =  Config( 1, 'YCSB', 'HEKATON', 'IndexBtree', 1024*1024*4, 1000,   1024*1024, 0.0, 0.0,0.5,0.5, 0, 0, 0, 440, 456) #update/insert

config1 = Config(1, 'YCSB', 'HEKATON', 'IndexBtree', 1024 * 1024, 10, 1024 * 1024 * 10, 0.0, 1.0, 0.0, 0.0, 0, 0, 0, 440, 456)  # select
config2 = Config(1, 'YCSB', 'HEKATON', 'IndexBtree', 1024 * 1024, 10, 1024 * 1024 * 10, 0.0, 0.5, 0.0, 0.5, 0, 0, 0, 440, 456)  # select/insert
config3 = Config(1, 'YCSB', 'HEKATON', 'IndexBtree', 1024 * 1024, 10, 1024 * 1024 * 10, 0.0, 0.5, 0.5, 0.0, 0, 0, 0, 440, 456)  # select/update
config4 = Config(1, 'YCSB', 'HEKATON', 'IndexBtree', 1024 * 1024, 10, 1024 * 1024 * 10, 0.0, 0.0, 0.5, 0.5, 0, 0, 0, 440, 456)  # update/insert

configs = [config1, config2, config3, config4]
# configs = [config11, config12, config13, config14, config15, config16, config17, config18, config19]


if __name__ == '__main__':
    # os.system('rm -rf test_result && mkdir test_result')
    test_suites = configs
    i = 0

    #tuple_sz = 100
    #leaf_sz = 8
    #h_tree_dram_block_size = 256
    #h_tree_split_threshold = 256
    #b_tree_message_count = 0
    #b_tree_dram_block_size = 1728
    #b_tree_split_threshold = 256
    #be_tree_message_count = 5
    #be_tree_dram_block_size = 1728
    #be_tree_split_threshold = 140
    #bf_tree_message_count = 4
    #bf_tree_dram_block_size = 1728
    #bf_tree_split_threshold = 158
    #bf1_tree_message_count = 8
    #bf1_tree_dram_block_size = 1728
    #bf1_tree_split_threshold = 256

    #tuple_sz = 100
    #leaf_sz = 16
    #h_tree_dram_block_size = 440
    #h_tree_split_threshold = 456
    #b_tree_message_count = 0
    #b_tree_dram_block_size = 3456
    #b_tree_split_threshold = 456
    #be_tree_message_count = 12
    #be_tree_dram_block_size = 3456
    #be_tree_split_threshold = 158
    #bf_tree_message_count = 8
    #bf_tree_dram_block_size = 3456
    #bf_tree_split_threshold = 256
    #bf1_tree_message_count = 16
    #bf1_tree_dram_block_size = 3456
    #bf1_tree_split_threshold = 456

    #tuple_sz = 100
    #leaf_sz = 64
    #h_tree_dram_block_size = 1584
    #h_tree_split_threshold = 1600
    #b_tree_message_count = 0
    #b_tree_dram_block_size = 13264
    #b_tree_split_threshold = 1600
    #be_tree_message_count = 60
    #be_tree_dram_block_size = 13264
    #be_tree_split_threshold = 158
    #bf_tree_message_count = 42
    #bf_tree_dram_block_size = 13264
    #bf_tree_split_threshold = 608
    #bf1_tree_message_count = 64
    #bf1_tree_dram_block_size = 13264
    #bf1_tree_split_threshold = 1600

    #tuple_sz = 100
    #leaf_sz = 256
    #h_tree_dram_block_size = 6192
    #h_tree_split_threshold = 6208
    #b_tree_message_count = 0
    #b_tree_dram_block_size = 52448
    #b_tree_split_threshold = 6208
    #be_tree_message_count = 240  # e=1/2
    #be_tree_dram_block_size = 52448
    #be_tree_split_threshold = 456
    #bf_tree_message_count = 128  # f=1/2
    #bf_tree_dram_block_size = 52448
    #bf_tree_split_threshold = 3136
    #bf1_tree_message_count = 256  # f=1
    #bf1_tree_dram_block_size = 52448
    #bf1_tree_split_threshold = 6208



    tuple_sz = 10
    leaf_sz = 8
   # h_tree_dram_block_size = 256
   # h_tree_split_threshold = 256
   # b_tree_message_count = 0
   # b_tree_dram_block_size = 1024
   # b_tree_split_threshold = 256
    be_tree_message_count = 5
    be_tree_dram_block_size = 1024
    be_tree_split_threshold = 158
    bf_tree_message_count = 4
    bf_tree_dram_block_size = 1024
    bf_tree_split_threshold = 158
    bf1_tree_message_count = 8
    bf1_tree_dram_block_size = 1024
    bf1_tree_split_threshold = 256


    #tuple_sz = 10
    #leaf_sz = 16
    #h_tree_dram_block_size = 440
    #h_tree_split_threshold = 456
    #b_tree_message_count = 0
    #b_tree_dram_block_size = 1864
    #b_tree_split_threshold = 456
    #be_tree_message_count = 12        #e=1/2
    #be_tree_dram_block_size = 1864
    #be_tree_split_threshold = 158
    #bf_tree_message_count = 8         #f=1/2
    #bf_tree_dram_block_size = 1864
    #bf_tree_split_threshold = 256
    #bf1_tree_message_count = 16       #f=1
    #bf1_tree_dram_block_size = 1864
    #bf1_tree_split_threshold = 456

    #tuple_sz = 10
    #leaf_sz = 64
    #h_tree_dram_block_size = 1584
    #h_tree_split_threshold = 1600
    #b_tree_message_count = 0
    #b_tree_dram_block_size = 7424
    #b_tree_split_threshold = 1600
    #be_tree_message_count = 60
    #be_tree_dram_block_size = 7424
    #be_tree_split_threshold = 158
    #bf_tree_message_count = 42
    #bf_tree_dram_block_size = 7424
    #bf_tree_split_threshold = 608
    #bf1_tree_message_count = 64
    #bf1_tree_dram_block_size = 7424
    #bf1_tree_split_threshold = 1600

    #tuple_sz = 10
    #leaf_sz = 256
    #h_tree_dram_block_size = 6192
    #h_tree_split_threshold = 6208
    #b_tree_message_count = 0
    #b_tree_dram_block_size = 29312
    #b_tree_split_threshold = 6208
    #be_tree_message_count = 240  # e=1/2
    #be_tree_dram_block_size = 29312
    #be_tree_split_threshold = 456
    #bf_tree_message_count = 128  # f=1/2
    #bf_tree_dram_block_size = 29312
    #bf_tree_split_threshold = 3136
    #bf1_tree_message_count = 256  # f=1
    #bf1_tree_dram_block_size = 29312
    #bf1_tree_split_threshold = 6208



    # tuple_sz = 1000
    # leaf_sz = 16
    # h_tree_dram_block_size = 440
    # h_tree_split_threshold = 456
    # b_tree_message_count = 0
    # b_tree_dram_block_size = 18364
    # b_tree_split_threshold = 456
    # be_tree_message_count = 12
    # be_tree_dram_block_size = 18364
    # be_tree_split_threshold = 158
    # bf_tree_message_count = 8
    # bf_tree_dram_block_size = 18364
    # bf_tree_split_threshold = 256
    # bf1_tree_message_count = 16
    # bf1_tree_dram_block_size = 18364
    # bf1_tree_split_threshold = 456

    # tuple_sz = 1000
    # leaf_sz = 64
    # h_tree_dram_block_size = 1584
    # h_tree_split_threshold = 1600
    # b_tree_message_count = 0
    # b_tree_dram_block_size = 71536
    # b_tree_split_threshold = 1600
    # be_tree_message_count = 60
    # be_tree_dram_block_size = 71536
    # be_tree_split_threshold = 158
    # bf_tree_message_count = 42
    # bf_tree_dram_block_size = 71536
    # bf_tree_split_threshold = 608
    # bf1_tree_message_count = 64
    # bf1_tree_dram_block_size = 71536
    # bf1_tree_split_threshold = 1600

    # tuple_sz = 10
    # leaf_sz = 81
    # h_tree_dram_block_size = 2000
    # h_tree_split_threshold = 2016
    # b_tree_message_count = 0
    # b_tree_dram_block_size = 9280
    # b_tree_split_threshold = 2016
    # be_tree_message_count = 78
    # be_tree_dram_block_size = 9280
    # be_tree_split_threshold = 158
    # bf_tree_message_count = 60
    # bf_tree_dram_block_size = 9280
    # bf_tree_split_threshold = 584
    # bf1_tree_message_count = 81
    # bf1_tree_dram_block_size = 9280
    # bf1_tree_split_threshold = 2016

    # tuple_sz = 100
    # leaf_sz = 81
    # h_tree_dram_block_size = 2000
    # h_tree_split_threshold = 2016
    # b_tree_message_count = 0
    # b_tree_dram_block_size = 16696
    # b_tree_split_threshold = 2016
    # be_tree_message_count = 78
    # be_tree_dram_block_size = 16696
    # be_tree_split_threshold = 158
    # bf_tree_message_count = 60
    # bf_tree_dram_block_size = 16696
    # bf_tree_split_threshold = 584
    # bf1_tree_message_count = 81
    # bf1_tree_dram_block_size = 16696
    # bf1_tree_split_threshold = 2016

    # tuple_sz = 1000
    # leaf_sz = 81
    # h_tree_dram_block_size = 2000
    # h_tree_split_threshold = 2016
    # b_tree_message_count = 0
    # b_tree_dram_block_size = 89928
    # b_tree_split_threshold = 2016
    # be_tree_message_count = 78
    # be_tree_dram_block_size = 89928
    # be_tree_split_threshold = 158
    # bf_tree_message_count = 60
    # bf_tree_dram_block_size = 89928
    # bf_tree_split_threshold = 584
    # bf1_tree_message_count = 81
    # bf1_tree_dram_block_size = 89928
    # bf1_tree_split_threshold = 2016

    # tuple_sz = 100
    # leaf_sz = 8
    # h_tree_dram_block_size = 256
    # h_tree_split_threshold = 256
    # b_tree_message_count = 0
    # b_tree_dram_block_size = 1728
    # b_tree_split_threshold = 256
    # be_tree_message_count = 5
    # be_tree_dram_block_size = 1728
    # be_tree_split_threshold = 140
    # bf_tree_message_count = 4
    # bf_tree_dram_block_size = 1728
    # bf_tree_split_threshold = 158
    # bf1_tree_message_count = 8
    # bf1_tree_dram_block_size = 1728
    # bf1_tree_split_threshold = 256

    # tuple_sz = 1000000
    # leaf_sz = 16
    # h_tree_dram_block_size = 440
    # h_tree_split_threshold = 456
    # b_tree_message_count = 0
    # b_tree_dram_block_size = 18364
    # b_tree_split_threshold = 456
    # be_tree_message_count = 12
    # be_tree_dram_block_size = 18364
    # be_tree_split_threshold = 158
    # bf_tree_message_count = 8
    # bf_tree_dram_block_size = 18364
    # bf_tree_split_threshold = 256
    # bf1_tree_message_count = 16
    # bf1_tree_dram_block_size = 18364
    # bf1_tree_split_threshold = 456

    # run heap
    #for test in test_suites:
    #    test.max_tuple_size = tuple_sz
    #    test.aggressive_inling = 0
    #    test.buffering = 0
    #    test.message_count = 0
    #    test.dram_block_size = h_tree_dram_block_size
    #    test.split_threshold = h_tree_split_threshold
    #    if i == 0:
    #        run_test(test, 'wr', tuple_sz, leaf_sz)
    #    else:
    #        run_test(test, 'a', tuple_sz, leaf_sz)
    #    i = i + 1

    # run b-tree
    #for test in test_suites:
    #    test.max_tuple_size = tuple_sz
    #    test.aggressive_inling = 1
    #    test.buffering = 0
    #    test.message_count = b_tree_message_count
    #    test.dram_block_size = b_tree_dram_block_size
    #    test.split_threshold = b_tree_split_threshold
    #    run_test(test, 'a', tuple_sz, leaf_sz)

    ## run be-tree
    for test in test_suites:
        test.max_tuple_size = tuple_sz
        test.aggressive_inling = 1
        test.buffering = 1
        test.message_count = be_tree_message_count
        test.dram_block_size = be_tree_dram_block_size
        test.split_threshold = be_tree_split_threshold
        run_test(test, 'a', tuple_sz, leaf_sz)

    # run bf-tree
    for test in test_suites:
        test.max_tuple_size = tuple_sz
        test.aggressive_inling = 1
        test.buffering = 1
        test.message_count = bf_tree_message_count
        test.dram_block_size = bf_tree_dram_block_size
        test.split_threshold = bf_tree_split_threshold
        run_test(test, 'a', tuple_sz, leaf_sz)

    # run bf-tree
    for test in test_suites:
        test.max_tuple_size = tuple_sz
        test.aggressive_inling = 1
        test.buffering = 1
        test.message_count = bf1_tree_message_count
        test.dram_block_size = bf1_tree_dram_block_size
        test.split_threshold = bf1_tree_split_threshold
        run_test(test, 'a', tuple_sz, leaf_sz)
