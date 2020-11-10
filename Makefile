compile:
	mpicc tasks.h tasks.c main.c -lm -Wpedantic -Wall -o distr-sched

compile-b:
	mpicc tasks.h tasks.c extension/bonus.c -lm -Wpedantic -Wall -o distr-sched-bonus
	
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
	mpirun -np 4 ./distr-sched 8 1 1 0.00 < cases/chains.in

config-heaps:
	mpirun -np 4 ./distr-sched 5 2 2 0.00 < cases/heaps.in

config-sparse:
	mpirun -np 4 ./distr-sched 12 0 10 0.16 < cases/sparse.in 

config-dense:
	mpirun -np 4 ./distr-sched 5 3 5 0.50 < cases/dense.in 

config-fan:
	mpirun -np 4 ./distr-sched 3 7 7 0.00 < cases/fan.in 
	
config-almost-chain:
	mpirun -np 4 ./distr-sched 10 0 1 0.20 < cases/almost-chain.in 

config-extra-chain:
	mpirun -np 4 ./distr-sched 8 1 3 0.50 < cases/extra-chain.in 

