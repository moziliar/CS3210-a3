#include "tasks.h"

// Struct used to implement a queue of task nodes
typedef struct task_node {
    task_t task;
    struct task_node *next;
} task_node_t;

int main(int argc, char *argv[]) {
    /*
     * =======================================================================
     * | [START]             DO NOT MODFIY THIS PORTION              [START] |
     * =======================================================================
     */
    int rank, num_procs;
    params_t params;

    // Initialise MPI processes and their ranks
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &num_procs);

    /*
     * Create MPI datatypes from general datatypes, displacement and block sizes
     * Constant arrays initialised in tasks.h - beware of C structure packing!
     */
    MPI_Datatype MPI_PARAMS_T, MPI_METRIC_T;

    MPI_Type_create_struct(2, params_t_lengths, params_t_displs,
            params_t_types, &MPI_PARAMS_T);
    MPI_Type_create_struct(2, metric_t_lengths, metric_t_displs,
            metric_t_types, &MPI_METRIC_T);

    MPI_Type_commit(&MPI_PARAMS_T);
    MPI_Type_commit(&MPI_METRIC_T);

    /*
     * By default, mpirun directs stdin to the rank 0 process and directs
     * standard input to /dev/null on all other processes
     */
    if (rank == 0) {
        if (argc != 5) {
            printf("Syntax: ./distr-sched <H> <Nmin> <Nmax> <P>\n");
            printf("ERROR: incorrect number of command line arguments\n");
            MPI_Abort(MPI_COMM_WORLD, EXIT_FAILURE);
        }

        // Parse task generation parameters from command-line arguments
        params = parse_params(argv);
    }

    /*
     * Broadcast task generation parameters then set them as globals locally
     * All other processes block here until rank 0 process executes broadcast
     */
    MPI_Bcast(&params, 1, MPI_PARAMS_T, 0, MPI_COMM_WORLD);
    set_generation_params(&params);

    // Start tracking execution metrics for each MPI process
    timespec_t start;
    clock_gettime(CLOCK_MONOTONIC, &start);
    metric_t stats = { rank, { 0, 0, 0, 0, 0 }, 1, 0 };

    /*
     * =======================================================================
     * | [END]              OK TO MODIFY CODE AFTER HERE               [END] |
     * =======================================================================
     */

    // MPI Task Struct
    size_t uint_s = sizeof(uint32_t);
    size_t int_s = sizeof(int);

    MPI_Datatype MPI_TASK_T;
    int          blocklens[]     = {1, 1, 1, 1, 1, 1, 4, 4};
    MPI_Aint     displacements[] = {0, uint_s, int_s, int_s, uint_s, uint_s, int_s, uint_s};
    MPI_Datatype old_types[]     = {MPI_UNSIGNED, MPI_INT, MPI_INT, MPI_UNSIGNED, MPI_UNSIGNED, MPI_INT, MPI_UNSIGNED, MPI_UNSIGNED};

    MPI_Type_create_struct(8, blocklens, displacements, old_types, &MPI_TASK_T);
    MPI_Type_commit(&MPI_TASK_T);

    // task queue
    task_node_t *head, *tail;
    int task_queue_len;

    head = tail = NULL;
    task_queue_len = 0;

    // flags
    int TASK_TAG = 0;
    int BUSY_TAG = 1;

    int FREE = 0;
    int BUSY = 1;

    // TODO we can consider optimizing on this afterwards, maybe just trying to send
    // the task out to processes immediately will help
    if (rank == 0) {
        // Head and tail pointers of the task queue

        // Read initial tasks
        int count;
        scanf("%d", &count);

        for (int i = 0; i < count; i++) {
            task_node_t *node = (task_node_t*) calloc(1, sizeof(task_node_t));
            if (node == NULL) {
                fprintf(stderr, "Failed to calloc task queue node\n");
                MPI_Abort(MPI_COMM_WORLD, EXIT_FAILURE);
            }

            scanf("%d %u", &node->task.type, &node->task.arg_seed);
            node->task.id = node->task.arg_seed;
            node->task.gen = 0;
            node->next = NULL;

            if (i == 0) {
                // First take is both head and tail of queue
                head = node;
                tail = node;
            } else {
                // Second task onwards added at tail
                tail->next = node;
                tail = node;
            }

	    task_queue_len++;
        }
    }
    /*
     * After the rank 0 node reads in the tasks, we can already treat it as one of the worker node.
     * The task distribution will be the same from this point onwards.
     */

    // Declare array to store generated descendant tasks
    int num_new_tasks = 0;
    task_t *task_buffer = (task_t*) malloc(Nmax * sizeof(task_t));

    task_t *task_msg_buffer = (task_t*) malloc(num_procs * sizeof(task_t));

    MPI_Request *task_reqs = malloc(num_procs * sizeof(MPI_Request));
    MPI_Request *busy_reqs = malloc(num_procs * sizeof(MPI_Request));

    // status buffer
    int *curr_status = calloc(num_procs * sizeof(int), 0);

    int running = 1;
    while (running) {
        switch (task_queue_len) {
            case 1: ;
                // just execute the task
                task_node_t *curr = head;
                head = tail = NULL; // no more tasks
                task_queue_len = 0;
                execute_task(&stats, &curr->task, &num_new_tasks, task_buffer);

                // enqueue all the child tasks
                for (int i = 0; i < num_new_tasks; i++) {
                    task_node_t *node = calloc(1, sizeof(task_node_t));
                    node->task = task_buffer[i];
                    node->next = NULL;

                    if (task_queue_len == 0) {
                        head = tail = node;
                    } else {
                        tail->next = node;
                        tail = node;
                    }
                    task_queue_len++;
                }

                if (rank == 0) {
                    printf("finished with %d tasks\n", task_queue_len);
                }
                free(curr);
                break;
            case 0: ;
                /*
                 * 1. probe to see if there are tasks from previous round of broadcast and add to queue if any
                 * 1.0.5 test any to see if there are tasks from previous round of task distribution that failed to send and add to queue if any
                 * 1.1. break if queue not empty
                 * 1.2. else Ibcst FREE
                 * 2. IRecv tasks from all
                 * 3. Waitany
                 * 4. Ibcst BUSY
                 * 5. loop to receive all incoming tasks and add to queue
                 * 6. execute task
                 */

                // TODO: step 1.0.5
                // MPI_Testany(num_procs-1, task_reqs, ...)

                // step 1
                
                int has_message;
                MPI_Status status;
                int receiving = 1;
                int retry = 3;
                while (receiving) {
                    MPI_Iprobe(MPI_ANY_SOURCE, TASK_TAG, MPI_COMM_WORLD, &has_message, &status);
                    if (!has_message) {
                        if (rank == 0) {
                            // retry thrice
                            if (retry == 0) break;
                            retry--;
                            sleep(1);
                        }
                        continue;
                    }
                    
                    int count;
                    MPI_Get_count(&status, MPI_TASK_T, &count); // status.count is maximum count
                    for (int j = 0; j < count; j++) {
                        task_t new_task;
                        
                        MPI_Recv(&new_task, 1, MPI_TASK_T, status.MPI_SOURCE, TASK_TAG, MPI_COMM_WORLD, NULL);

                        task_node_t *node = (task_node_t*) calloc(1, sizeof(task_node_t));
                        node->task = new_task;
                        node->next = NULL;

                        if (head == NULL) {
                            head = tail = node;
                        } else {
                            tail->next = node;
                            tail = node;
                        }

                        task_queue_len++;
                    }

                    if (task_queue_len > 0) {
                        receiving = 0;
                    }
                }
                if (rank == 0) {
                    printf("checking 1.2\n");
                }

                // step 1.2
                int any_busy = FREE;
                curr_status[rank] = FREE;
                for (int i = 0; i < num_procs; i++) {
                    if (i == rank) {
                        continue;
                    } 

                    MPI_Isend(&curr_status[rank], 1, MPI_INT, i, BUSY_TAG, MPI_COMM_WORLD, &busy_reqs[i]); // don't care if sent successfully
                    any_busy |= curr_status[i]; // check if all are free, used by rank 0
                }

                // handle exit here
                if (rank == 0) {
                    while (1) {
                        printf("checking leftover\n");
                        if (!any_busy) {
                            running = 0;

                            printf("terminate\n");
                            // send termination signal to all
                            for (int i = 0; i < num_procs; i++) {
                                if (i == rank) {
                                    continue;
                                } 

                                curr_status[i] = -1;
                                MPI_Send(&curr_status[i], 1, MPI_INT, i, BUSY_TAG, MPI_COMM_WORLD);
                            }

                            break;
                        }

                        int any_message;
                        MPI_Iprobe(MPI_ANY_SOURCE, TASK_TAG, MPI_COMM_WORLD, &any_message, &status);
                        if (any_message) {
                            break;
                        }

                        any_busy = FREE;
                        for (int i = 0; i < num_procs; i++) {
                            if (i == rank) {
                                continue;
                            } 

                            MPI_Irecv(&curr_status[i], 1, MPI_INT, i, BUSY_TAG, MPI_COMM_WORLD, &busy_reqs[i]);
                            any_busy |= curr_status[i]; // check if all are free
                        }
                        sleep(1);
                    }
                } else {
                    MPI_Irecv(&curr_status[rank], 1, MPI_INT, 0, BUSY_TAG, MPI_COMM_WORLD, &busy_reqs[rank]);
                    if (curr_status[rank] == -1) {
                        running = 0;
                        break;
                    }
                    MPI_Probe(MPI_ANY_SOURCE, TASK_TAG, MPI_COMM_WORLD, &status);
                }
                // status now contains info from other nodes
                // declare busy, grab a task and start
                curr_status[rank] = BUSY;
                for (int i = 0; i < num_procs; i++) {
                    if (i == rank) {
                        continue;
                    } 

                    MPI_Isend(&curr_status[rank], 1, MPI_INT, i, BUSY_TAG, MPI_COMM_WORLD, &busy_reqs[i]); // don't care if sent successfully
                }

                // at this point, the task queue length is guaranteed to be > 0
                task_t new_task;
                MPI_Recv(&new_task, 1, MPI_TASK_T, status.MPI_SOURCE, TASK_TAG, MPI_COMM_WORLD, NULL);

                task_node_t *node = calloc(1, sizeof(task_node_t));
                node->task = new_task;
                node->next = NULL;

                tail->next = node;
                tail = node;
                task_queue_len++;

                break;
            default: ;
                /*
                 * 1.1. (task_queue_len > 1) loop: try to Ibcst tasks
                 * 1.2. (task_queue_len = 1) break and Ibcst BUSY
                 * 2.1. Upon receiving a status broadcast from node 2, test until there's no more backlog from 2
                 * 2.2. If node 2 is free, ISend task and remove from task_queue
                 * 3. execute task
                 */
                
                // try to receive the latest status update first
                int i;
                for (i = 0; i < num_procs; i++) {
                    if (i == rank) {
                        continue;
                    } 

                    MPI_Irecv(&curr_status[i], 1, MPI_INT, i, BUSY_TAG, MPI_COMM_WORLD, &busy_reqs[i]); // don't care if received successfully
                }

                // step 1.1
                for (i = 0; i < num_procs; i++) {
                    if (task_queue_len <= 1) break;
                    if (curr_status[i] == FREE && i != rank) {
                        task_t to_send = head->task;
                        head = head->next;
                        MPI_Isend(&to_send, 1, MPI_TASK_T, i, TASK_TAG, MPI_COMM_WORLD, &task_reqs[i]); // TODO: should we store the handle for 1.0.5?
                        task_queue_len--;
                    }
                }

                if (task_queue_len > 1) {
                    // just execute the task
                    task_node_t *curr = head;
                    head = head->next;
                    task_queue_len--;
                    execute_task(&stats, &curr->task, &num_new_tasks, task_buffer);

                    // enqueue all the child tasks
                    for (int i = 0; i < num_new_tasks; i++) {
                        task_node_t *node = calloc(1, sizeof(task_node_t));
                        node->task = task_buffer[i];
                        node->next = NULL;

                        tail->next = node;
                        tail = node;
                        task_queue_len++;
                    }

                    free(curr);
                }

                break; // task_queue_len should be 1, fall into case above
        }
    }

    free(task_reqs);
    free(busy_reqs);

    free(task_buffer);
    free(task_msg_buffer);

    /*
     * =======================================================================
     * | [START]             DO NOT MODFIY THIS PORTION              [START] |
     * =======================================================================
     */
    MPI_Barrier(MPI_COMM_WORLD);

    // Complete tracking execution metrics for each process
    timespec_t end;
    clock_gettime(CLOCK_MONOTONIC, &end);
    stats.total_time += compute_interval(&start, &end);

    if (DEBUG == 0) {
        if (rank == 0) {
            metric_t *metrics = (metric_t*)
                malloc(num_procs * sizeof(metric_t));
            if (metrics == NULL) {
                fprintf(stderr,
                        "Failed to malloc buffer for execution metrics\n");
                MPI_Abort(MPI_COMM_WORLD, EXIT_FAILURE);
            }

            MPI_Gather(&stats, 1, MPI_METRIC_T, metrics, 1, MPI_METRIC_T,
                    0, MPI_COMM_WORLD);

            // Print execution metrics by rank order
            for (int i = 0; i < num_procs; i++) {
                print_metrics(&metrics[i]);
            }

            free(metrics);
        } else {
            MPI_Gather(&stats, 1, MPI_METRIC_T, NULL, 0, MPI_DATATYPE_NULL,
                    0, MPI_COMM_WORLD);
        }
    }

    MPI_Barrier(MPI_COMM_WORLD);

    // Terminate MPI execution environment
    MPI_Finalize();

    return 0;
    /*
     * =======================================================================
     * | [END]                  END OF SKELETON CODE                   [END] |
     * =======================================================================
     */
}

