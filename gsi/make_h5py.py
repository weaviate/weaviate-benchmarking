import os
import argparse

import numpy
import h5py

#
# Configuration
#

#
# Globals
#

#
# parse arguments
#
parser = argparse.ArgumentParser()
parser.add_argument("--dataset", required=True)
parser.add_argument("--queries", required=True)
parser.add_argument("--groundtruth")
parser.add_argument("--output", required=True)
args = parser.parse_args()

#
# validate arguments
#

# check 'output' arg
if os.path.exists( args.output ):
    raise Exception("The output file already exists - " + args.output)
elif not os.path.exists( os.path.dirname( args.output )):
    raise Exception("Expected this directory to exist - " + os.path.dirname(args.output) )

# check 'dataset' arg
if not os.path.exists( args.dataset ):
    raise Exception("Dataset does not exist", args.dataset)

# check 'queries' arg
if not os.path.exists( args.queries ):
    raise Exception("Queries does not exist", args.queries)

# check 'groundtruth' arg
if args.groundtruth and not os.path.exists( args.groundtruth ):
    raise Exception("Groundtruth does not exist", args.groundtruth )

# create the h5py file
print("Creating new h5py file at", args.output,"...")
with h5py.File( args.output, 'w') as hf:

    data = numpy.load( args.dataset, mmap_mode='r')
    dset = hf.create_dataset('train', data=data, shape=data.shape, chunks=True)
    
    data = numpy.load( args.queries, mmap_mode='r')
    dset = hf.create_dataset('test', data=data, shape=data.shape, chunks=True)
   
    if args.groundtruth: 
        data = numpy.load( args.groundtruth, mmap_mode='r')
        dset = hf.create_dataset('neighbors', data=data, shape=data.shape, chunks=True)

print("Done.")



