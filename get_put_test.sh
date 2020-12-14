#!/bin/bash

RUN_DIR=$(pwd)
echo $RUN_DIR
BUILDDIR=/home/qyzhang/ComboTree-SortBuffer/build/

source scripts/set_vmmalloc.sh

for type in art hot masstree clht fastfair levelhash
do
for thread in 1 2 4 8 16 24 32 40 48
do
  LD_PRELOAD="/lib64/libvmmalloc.so" numactl --cpunodebind=0 ./get_put_bench $type a $thread | tee "$type-$thread-get-put.txt"
done
done

cd $BUILDDIR
make clean
make CXX_DEFINES="-DEXPAND_THREADS=1" -j
cd $RUN_DIR/build
make -j
cd $RUN_DIR
numactl --cpunodebind=0 ./get_put_bench combotree a 1 | tee "combotree-1-get-put.txt"

for thread in 1 2 4 8 12 16 20 24
do
  cd $BUILDDIR
  make clean
  make CXX_DEFINES="-DEXPAND_THREADS=$thread" -j
  cd $RUN_DIR/build
  make -j
  cd $RUN_DIR
  numactl --cpunodebind=0 ./get_put_bench combotree a $((thread*2)) | tee "combotree-$((thread*2))-get-put.txt"
done

for type in art hot masstree clht fastfair levelhash
do
for thread in 1 2 4 8 16 24 32 40 48
do
  LD_PRELOAD="/lib64/libvmmalloc.so" numactl --cpunodebind=0 ./delete_bench $type a $thread | tee "$type-$thread-delete.txt"
done
done

cd $BUILDDIR
make clean
make CXX_DEFINES="-DEXPAND_THREADS=1" -j
cd $RUN_DIR/build
make -j
cd $RUN_DIR
numactl --cpunodebind=0 ./delete_bench combotree a 1 | tee "combotree-1-delete.txt"

for thread in 1 2 4 8 12 16 20 24
do
  cd $BUILDDIR
  make clean
  make CXX_DEFINES="-DEXPAND_THREADS=$thread" -j
  cd $RUN_DIR/build
  make -j
  cd $RUN_DIR
  numactl --cpunodebind=0 ./delete_bench combotree a $((thread*2)) | tee "combotree-$((thread*2))-delete.txt"
done