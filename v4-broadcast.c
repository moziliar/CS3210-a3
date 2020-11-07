#include "tasks.h"

#define MAX_TASK_IN_MSG 50

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
  MPI_Aint     displacements[] = {0, uint_s, uint_s + int_s, uint_s + 2 * int_s, 
                                  2 * uint_s + 2 * int_s, 
                                  3 * uint_s + 2 * int_s, 
                                  3 * uint_s + 3 * int_s, 
                                  7 * uint_s + 3 * int_s};
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
  int COUNT_TAG = 2;

  int FREE = 0;
  int BUSY = 1;

  int total_active_tasks = 0;

  // TODO we can consider optimizing on this afterwards, maybe just trying to send
  // the task out to processes immediately will help
  if (rank == 0) {
    // Head and tail pointers of the task queue

    // Read initial tasks
    scanf("%d", &total_active_tasks);

    for (int i = 0; i < total_active_tasks; i++) {
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
        // Seond task onwards added at tail
        tail->next = node;
        tail = node;
      }

      task_queue_len++;
    }
  }

  // tell everyone that there are n tasks at the start, then start distributing them.
  MPI_Bcast(&total_active_tasks, 1, MPI_INT, 0, MPI_COMM_WORLD);

  /*
   * After the rank 0 node reads in the tasks, we can already treat it as one of the worker node.
   * The task distribution will be the same from this point onwards.
   */

  // Declare array to store generated descendant tasks
  task_t *task_buffer = (task_t*) malloc(Nmax * sizeof(task_t));

  task_t task_msg_buffer[MAX_TASK_IN_MSG];

  MPI_Comm* count_comms = malloc(num_procs * sizeof(MPI_Comm));
  MPI_Comm* busy_comms = malloc(num_procs * sizeof(MPI_Comm));
  int* incoming_busy = calloc(num_procs, sizeof(int));
  int* incoming_count = calloc(num_procs, sizeof(int));

  // to store the broadcasted info
  // info is simply value
  // for task_type == 1 (BUSY), value = BUSY / FREE
  // for task_type == 2 (COUNT), value = num_new_procs
  MPI_Request *count_reqs = malloc(num_procs * sizeof(MPI_Request));
  MPI_Request *busy_reqs = malloc(num_procs * sizeof(MPI_Request));
  MPI_Request my_count_req = MPI_REQUEST_NULL;
  MPI_Request my_busy_req = MPI_REQUEST_NULL;

  // for outgoing messages
  MPI_Request *task_reqs = malloc(num_procs * sizeof(MPI_Request));
  for (int i = 0; i < num_procs; i++) {
    MPI_Comm_dup(MPI_COMM_WORLD, &count_comms[i]);
    MPI_Comm_dup(MPI_COMM_WORLD, &busy_comms[i]);
    task_reqs[i] = MPI_REQUEST_NULL;
    count_reqs[i] = MPI_REQUEST_NULL;
    busy_reqs[i] = MPI_REQUEST_NULL;

    if (rank == i) continue;

    // set up ibcast
#if DEBUG
    printf("rank %d, prepared to receive broadcast for %d\n", rank, i);
#endif
    MPI_Ibcast(&incoming_count[i], 1, MPI_INT, i, count_comms[i], &count_reqs[i]);
    MPI_Ibcast(&incoming_busy[i], 1, MPI_INT, i, busy_comms[i], &busy_reqs[i]);
  }
  // everyone runs ibcast for nodes except self
  // busy and count are broadcasted
  // can use mpi_test to check.
  // so every iterations, we -use MPI_testany for busy, count
  // then mpi probe for task
  // might be a problem when we extend this to bonus...

  // status buffer
  int *is_busy = calloc(num_procs, sizeof(int));
  is_busy[0] = BUSY;


  int has_message;
  MPI_Status incoming_status;
  int broadcast_index;
  int has_broadcast;
  while (1) {
    if (total_active_tasks == 0) {
#if DEBUG
      printf("rank %d has no more active tasks, quitting\n", rank);
#endif
      break;
    }

    // TODO investigate whether removing the continue for messages will improve runtime
    // current behavior prioritizes receiving messages over processing. we should
    // see whether this is better.
    MPI_Testany(num_procs, count_reqs, &broadcast_index, &has_broadcast, MPI_STATUS_IGNORE);
    if (has_broadcast) {
#if DEBUG
      printf("rank %d received task update from %d\n", rank, broadcast_index);
#endif
      // update global counter
      total_active_tasks = total_active_tasks - 1 + incoming_count[broadcast_index];
      // prepare for the next broadcast
      MPI_Ibcast(&incoming_count[broadcast_index], 1, MPI_INT, broadcast_index, 
                 count_comms[broadcast_index], &count_reqs[broadcast_index]);
      continue;
    }

    MPI_Testany(num_procs, busy_reqs, &broadcast_index, &has_broadcast, MPI_STATUS_IGNORE);
    if (has_broadcast) {
#if DEBUG
      printf("rank %d received busy update from %d\n", rank, broadcast_index);
#endif
      // update busy view, to determine who we can send tasks to
      is_busy[broadcast_index] = incoming_busy[broadcast_index];
      MPI_Ibcast(&incoming_busy[broadcast_index], 1, MPI_INT, broadcast_index, 
                 busy_comms[broadcast_index], &busy_reqs[broadcast_index]);
      continue;
    }

    MPI_Iprobe(MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &has_message, &incoming_status);

    if (has_message && incoming_status.MPI_TAG == TASK_TAG) {
      int sender = incoming_status.MPI_SOURCE;
      // this should never exceed MAX_TASK_IN_MSG
      int num_tasks;
      MPI_Get_count(&incoming_status, MPI_TASK_T, &num_tasks);
#if DEBUG
      printf("rank %d received %d tasks from %d\n", rank, num_tasks, sender);
#endif
      MPI_Recv(&task_msg_buffer, num_tasks, MPI_TASK_T, sender, TASK_TAG, MPI_COMM_WORLD, NULL);

      for (int i = 0; i < num_tasks; i++) {
        task_node_t *node = (task_node_t*) calloc(1, sizeof(task_node_t));
        node->task = task_msg_buffer[i];
#if DEBUG
        printf("rank %d has received task type %d, seed %d\n", rank, task_msg_buffer[i].type, task_msg_buffer[i].arg_seed);
#endif
        node->next = NULL;

        if (task_queue_len == 0) {
          head = tail = node;
        } else {
          tail->next = node;
          tail = node;
        }

        task_queue_len++;
      }

#if DEBUG
      printf("rank %d, queue length: %d\n", rank, task_queue_len);
#endif
      continue;
    }

    if (is_busy[rank]) {
      if (task_queue_len == 0) {
        // set to not busy, inform everyone that i am free
        is_busy[rank] = FREE;

        // wait to ensure the broadcast is complete before modifying buffer
        // MPI_Wait(&my_busy_req, MPI_STATUS_IGNORE);

        #if DEBUG
        printf("rank %d broadcasted to world i am free\n", rank);
        #endif
        MPI_Ibcast(&is_busy[rank], 1, MPI_INT, rank, 
                 busy_comms[rank], &my_busy_req);
        continue;
      } 
      
      if (task_queue_len > 1) {
        // distribute task until left with 1 task minimum
        for (int i = 0; i < num_procs && task_queue_len > 1; i++) {
          if (is_busy[i] == FREE && i != rank) {
            // MPI_Wait(&task_reqs[i], MPI_STATUS_IGNORE);
            int num_tasks_to_send = task_queue_len > MAX_TASK_IN_MSG * 2
                           ? MAX_TASK_IN_MSG
                           : task_queue_len / 2;
#if DEBUG
            printf("Rank %d sent %d tasks to %d\n", rank, num_tasks_to_send, i);
#endif
            for (int j = 0; j < num_tasks_to_send; j++) {
              task_msg_buffer[j] = head->task;
              head = head->next;
              task_queue_len--;
#if DEBUG
              printf("rank %d is sending task type %d to %d\n", rank, task_msg_buffer[j].type, i);
#endif
            }
            MPI_Isend(&task_msg_buffer, num_tasks_to_send, MPI_TASK_T, i, TASK_TAG, MPI_COMM_WORLD, &task_reqs[i]); // TODO: should we store the handle for 1.0.5?
            is_busy[i] = BUSY;
            // TODO do we need to free the heads?
          }
        }
      }

      int num_new_tasks = 0;
      // execute task
      task_node_t *curr = head;
      task_queue_len--;
      if (task_queue_len == 0) {
        head = tail = NULL;
      } else {
        head = head->next;
      }
      execute_task(&stats, &curr->task, &num_new_tasks, task_buffer);

      total_active_tasks = total_active_tasks - 1 + num_new_tasks;

      // wait to ensure the broadcast is complete before modifying buffer
      // MPI_Wait(&my_count_req, MPI_STATUS_IGNORE);

      // incoming_count[rank] = num_new_tasks;
      MPI_Ibcast(&num_new_tasks, 1, MPI_INT, rank, count_comms[rank], &my_count_req);

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

      free(curr);

#if DEBUG
      printf("rank %d completed a task with %d tasks in queue\n", rank, task_queue_len);
#endif
      continue;

    } else if (!is_busy[rank]) {
      if (task_queue_len == 0) {
        // continue waiting for task / updates
#if DEBUG
        sleep(1);
#endif
        continue;
      } else if (task_queue_len != 0) {
        is_busy[rank] = BUSY;

        // wait to ensure the broadcast is complete before modifying buffer
        //MPI_Wait(&my_broadcast_req, MPI_STATUS_IGNORE);

        MPI_Ibcast(&is_busy[rank], 1, MPI_INT, rank, 
                 busy_comms[rank], &my_busy_req);
        continue;
      }
    } 
  }

  free(task_reqs);
  free(task_buffer);
  free(count_reqs);
  free(busy_reqs);
  free(incoming_busy);
  free(incoming_count);
  free(is_busy);
  free(count_comms);
  free(busy_comms);

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
