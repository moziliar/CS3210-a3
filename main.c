#include "tasks.h"
#define TASK_TAG 0
#define BUSY_TAG 1

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
    MPI_Datatype task_struct;
    int          blocklens[] = {1, 1};
    MPI_Aint     displacements[] = {0, sizeof(int)};
    MPI_Datatype old_types[] = {MPI_INT, MPI_UNSIGNED};

    MPI_Type_create_struct(2, blocklens, displacements, old_types, &task_struct);
    MPI_Type_commit(&task_struct);

    // task queue
    task_node_t *head, *tail;
    int task_queue_len;

    head = tail = NULL;
    task_queue_len = 0;

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

        // TODO distribute initial tasks
    }
    /*
     * After the rank 0 node reads in the tasks, we can already treat it as one of the worker node.
     * The task distribution will be the same from this point onwards.
     */

    // Declare array to store generated descendant tasks
    int num_new_tasks = 0;
    task_t *task_buffer = (task_t*) malloc(Nmax * sizeof(task_t));

    task_t *task_msg_buffer = (task_t*) malloc(num_procs * sizeof(task_t));

    while (true) {
        switch (task_queue_len) {
            case 1:
                // just execute the task
                task_node_t *curr = head;
                execute_task(&stats, &curr->task, &num_new_tasks, task_buffer);
                task_queue_len--;
                break;
            case 0:
                /*
                 * 1. test any to see if there are tasks from previous round of broadcast and add to queue if any
                 * 1.1. break if queue not empty
                 * 1.2. else Ibcst FREE
                 * 2. IRecv tasks from all
                 * 3. Waitany
                 * 4. Ibcst BUSY
                 * 5. loop to receive all incoming tasks and add to queue
                 * 6. execute task
                 */
                int flag = 0;
                MPI_Testany(num_procs-1, 
                break;
            default:
                /*
                 * 1.1. (task_queue_len > 1) loop: try to Ibcst tasks
                 * 1.2. (task_queue_len = 1) break and Ibcst BUSY
                 * 2.1. Upon receiving a status broadcast from node 2, test until there's no more backlog from 2
                 * 2.2. If node 2 is free, ISend task and remove from task_queue
                 * 3. execute task
                 */
                break;
        }
    }
        




    // TODO wait for a task
    for (int i = 0; i < num_procs; i++) {
      if (i == rank) continue;

      MPI_Irecv(&task_msg_buffer[i], 1, MPI_T)

    }
    

    // Clear tasks in queue one by one
    while (head != NULL) {
        task_node_t *curr = head;
        execute_task(&stats, &curr->task, &num_new_tasks, task_buffer);

        for (int i = 0; i < num_new_tasks; i++) {
            // Construct new node for descendant task
            task_node_t *node = calloc(1, sizeof(task_node_t));
            node->task = task_buffer[i];
            node->next = NULL;

            tail->next = node;
            tail = node;
        }

        // Destroy node for completed task
        head = head->next;
        free(curr);
    }

    free(task_buffer);

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

