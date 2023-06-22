#!/bin/bash

OUT="/mnt/nas1/fvs_benchmark_datasets/h5py/deep-1M.hdf5"
if [ -f "$OUT" ];then
    echo "The file $OUT already exists."
else
    echo "Creating $OUT"
    python make_h5py.py --dataset /mnt/nas1/fvs_benchmark_datasets/deep-1M.npy --queries /mnt/nas1/fvs_benchmark_datasets/deep-queries-1000.npy --groundtruth /mnt/nas1/fvs_benchmark_datasets/deep-1M-gt-1000.npy --output $OUT
fi

OUT="/mnt/nas1/fvs_benchmark_datasets/h5py/deep-10K.hdf5"
if [ -f "$OUT" ];then
    echo "The file $OUT already exists."
else
    echo "Creating $OUT"
    python make_h5py.py --dataset /mnt/nas1/fvs_benchmark_datasets/deep-10K.npy --queries /mnt/nas1/fvs_benchmark_datasets/deep-queries-1000.npy --groundtruth /mnt/nas1/fvs_benchmark_datasets/deep-10K-gt-1000.npy --output $OUT
fi

OUT="/mnt/nas1/fvs_benchmark_datasets/h5py/deep-2M.hdf5"
if [ -f "$OUT" ];then
    echo "The file $OUT already exists."
else
    echo "Creating $OUT"
    python make_h5py.py --dataset /mnt/nas1/fvs_benchmark_datasets/deep-2M.npy --queries /mnt/nas1/fvs_benchmark_datasets/deep-queries-1000.npy --groundtruth /mnt/nas1/fvs_benchmark_datasets/deep-2M-gt-1000.npy --output $OUT
fi

OUT="/mnt/nas1/fvs_benchmark_datasets/h5py/deep-5M.hdf5"
if [ -f "$OUT" ];then
    echo "The file $OUT already exists."
else
    echo "Creating $OUT"
    python make_h5py.py --dataset /mnt/nas1/fvs_benchmark_datasets/deep-5M.npy --queries /mnt/nas1/fvs_benchmark_datasets/deep-queries-1000.npy --groundtruth /mnt/nas1/fvs_benchmark_datasets/deep-5M-gt-1000.npy --output $OUT
fi

OUT="/mnt/nas1/fvs_benchmark_datasets/h5py/deep-10M.hdf5"
if [ -f "$OUT" ];then
    echo "The file $OUT already exists."
else
    echo "Creating $OUT"
    python make_h5py.py --dataset /mnt/nas1/fvs_benchmark_datasets/deep-10M.npy --queries /mnt/nas1/fvs_benchmark_datasets/deep-queries-1000.npy --groundtruth /mnt/nas1/fvs_benchmark_datasets/deep-10M-gt-1000.npy --output $OUT
fi

OUT="/mnt/nas1/fvs_benchmark_datasets/h5py/deep-20M.hdf5"
if [ -f "$OUT" ];then
    echo "The file $OUT already exists."
else
    echo "Creating $OUT"
    python make_h5py.py --dataset /mnt/nas1/fvs_benchmark_datasets/deep-20M.npy --queries /mnt/nas1/fvs_benchmark_datasets/deep-queries-1000.npy --groundtruth /mnt/nas1/fvs_benchmark_datasets/deep-20M-gt-1000.npy --output $OUT
fi

OUT="/mnt/nas1/fvs_benchmark_datasets/h5py/deep-50M.hdf5"
if [ -f "$OUT" ];then
    echo "The file $OUT already exists."
else
    echo "Creating $OUT"
    python make_h5py.py --dataset /mnt/nas1/fvs_benchmark_datasets/deep-50M.npy --queries /mnt/nas1/fvs_benchmark_datasets/deep-queries-1000.npy --groundtruth /mnt/nas1/fvs_benchmark_datasets/deep-50M-gt-1000.npy --output $OUT
fi

OUT="/mnt/nas1/fvs_benchmark_datasets/h5py/deep-100M.hdf5"
if [ -f "$OUT" ];then
    echo "The file $OUT already exists."
else
    echo "Creating $OUT"
    python make_h5py.py --dataset /mnt/nas1/fvs_benchmark_datasets/deep-100M.npy --queries /mnt/nas1/fvs_benchmark_datasets/deep-queries-1000.npy --groundtruth /mnt/nas1/fvs_benchmark_datasets/deep-100M-gt-1000.npy --output $OUT
fi

OUT="/mnt/nas1/fvs_benchmark_datasets/h5py/deep-150M.hdf5"
if [ -f "$OUT" ];then
    echo "The file $OUT already exists."
else
    echo "Creating $OUT"
    python make_h5py.py --dataset /mnt/nas1/fvs_benchmark_datasets/deep-150M.npy --queries /mnt/nas1/fvs_benchmark_datasets/deep-queries-1000.npy --groundtruth /mnt/nas1/fvs_benchmark_datasets/deep-150M-gt-1000.npy --output $OUT
fi

OUT="/mnt/nas1/fvs_benchmark_datasets/h5py/deep-250M.hdf5"
if [ -f "$OUT" ];then
    echo "The file $OUT already exists."
else
    echo "Creating $OUT"
    python make_h5py.py --dataset /mnt/nas1/fvs_benchmark_datasets/deep-250M.npy --queries /mnt/nas1/fvs_benchmark_datasets/deep-queries-1000.npy --groundtruth /mnt/nas1/fvs_benchmark_datasets/deep-250M-gt-1000.npy --output $OUT
fi


