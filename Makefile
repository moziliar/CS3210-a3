compile:
	mpicc tasks.h tasks.c main.c -lm -Wpedantic -Wall -o distr-sched

config-chains:
	mpirun -np 4 ./distr-sched 8 1 1 0.00 < chains.in > chains.out

config-heaps:
	mpirun -np 4 ./distr-sched 5 2 2 0.00 < heaps.in > heaps.out

config-sparse:
	mpirun -np 4 ./distr-sched 12 0 10 0.16 < sparse.in > sparse.out

config-dense:
	mpirun -np 4 ./distr-sched 5 3 5 0.50 < dense.in > dense.out

