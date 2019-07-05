'''
Cluster and Cloud Computing
COMP90024
Assignment 1
'''

import json
import time
from collections import Counter
from mpi4py import MPI
import pprint

start = time.time()
comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

with open("melbGrid.json","r") as f:
    grid = json.load(f)

# initialize data structure
block_bounds=[]
block_counts= Counter()
result = {'block_count':block_counts, 'hashtag_count':{}}

# fill up the block information
for feature in grid["features"]:
    block_bounds.append(feature['properties'])
    # initialize the counters of blocks
    result['block_count'][feature['properties']['id']] = 0
    result['hashtag_count'][feature['properties']['id']] = Counter()


'''
returns which block this coordinate sits in
Input: coordinate --> [x,y]
Ourput: block --> str
'''
def get_block(row):
    pos = row['doc']['coordinates']['coordinates']
    for bound in block_bounds:
        if bound['xmin'] < pos[0] <= bound['xmax'] and bound['ymin'] < pos[1] <= bound['ymax']:
            return bound['id']
        else:
            # if not sit within the first block, check next
            continue

'''
count the hashtags, and increment the corespondent block count
Input: row --> json object (python)
    result --> global counter
No output
'''
def record_data(row,result):
    hashtags = row['doc']['entities']['hashtags']
    if hashtags != []:
        for hashtag in hashtags:
            result['hashtag_count'][block][hashtag['text'].lower()] += 1

    result['block_count'][block] += 1

# main process
with open("bigTwitter.json","r",encoding='UTF-8') as f:

    count = 0
    for line in f:
        # allocate lines to different cores respectively
        if rank == count%size:
            try:
                # get away the comma and \n at end on each line
                row = json.loads(line[:-2])
                block = get_block(row)
                # skip the twitter post outside the area we're interested
                if block == None:
                    continue
                record_data(row,result)
                # to deal with the second last line (no comma)
            except Exception as e1:
                try:
                    row = json.loads(line[:-1])
                    block = get_block(row)
                    if block == None:
                        continue
                    record_data(row,result)
                except Exception as e2:
                    # to deal with the final line: ]}
                    continue
        count += 1

    # gathering the data from different cores
    final_result = comm.gather(result, root=0)

    # back to the root node
    if rank == 0:

        # initialize the final output data frame
        output = {'block_count':block_counts, 'hashtag_count':{}}
        for feature in grid["features"]:
            output['block_count'][feature['properties']['id']] = 0
            output['hashtag_count'][feature['properties']['id']] = Counter()

        # merge data(dict) from all ranks
        for data in final_result:

            # Counter can make use of addition
            output['block_count'] += data['block_count']
            for i in output['hashtag_count']:
                output['hashtag_count'][i] += data['hashtag_count'][i]

        print("THE RANK OF BLOCKS:")
        block_rank = output['block_count'].most_common()
        for i in block_rank:
            print("{}: {}".format(i[0],i[1]))
        print()

        print("THE TOP 5 POPULAR HASHTAGES IN EVERY RANKED BLOCK:")
        for c in block_rank:
            print("{}: {}".format(c[0],output['hashtag_count'].get(c[0]).most_common(5)))

        end = time.time()
        print()
        print('TIME CONSUMPTION: {} in seconds'.format(end-start))
