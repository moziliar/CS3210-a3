compile:
	mpicc tasks.h tasks.c main.c -lm -Wpedantic -Wall -o distr-sched

compile-v1:
	mpicc tasks.h tasks.c v1-pre-state-machine.c -lm -Wpedantic -Wall -o distr-sched-v1

compile-v2:
	mpicc tasks.h tasks.c v2-state-machine-single-task.c -lm -Wpedantic -Wall -o distr-sched-v2

compile-v3:
	mpicc tasks.h tasks.c v3-multiple-tasks-with-waits.c -lm -Wpedantic -Wall -o distr-sched-v3

compile-v4:
	mpicc tasks.h tasks.c v4-broadcast.c -lm -Wpedantic -Wall -o distr-sched-v4

compile-seq:
	mpicc tasks.h tasks.c seq.c -lm -Wpedantic -Wall -o distr-sched-seq

config-chains:
	mpirun -np 10 ./distr-sched 8 1 1 0.00 < chains.in > chains.out

config-heaps:
	mpirun -np 6 ./distr-sched 2 2 2 0.00 < heaps.in

config-heaps-v1:
	mpirun -np 6 ./distr-sched-v1 2 2 2 0.00 < heaps.in

config-heaps-v2:
	mpirun -np 6 ./distr-sched-v2 2 2 2 0.00 < heaps.in

config-heaps-v3:
	mpirun -np 6 ./distr-sched-v3 2 2 2 0.00 < heaps.in

config-heaps-v4:
	mpirun -np 6 ./distr-sched-v4 2 2 2 0.00 < heaps.in

config-heaps-seq:
	mpirun -np 6 ./distr-sched-seq 2 2 2 0.00 < heaps.in

config-sparse:
	mpirun -np 4 ./distr-sched 12 0 10 0.16 < sparse.in > sparse.out

config-dense:
	mpirun -np 4 ./distr-sched 5 3 5 0.50 < dense.in > dense.out

