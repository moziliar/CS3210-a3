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

  // for outgoing messages
  MPI_Request *busy_reqs = malloc(num_procs * sizeof(MPI_Request));
  MPI_Request *count_reqs = malloc(num_procs * sizeof(MPI_Request));
  for (int i = 0; i < num_procs; i++) {
    busy_reqs[i] = MPI_REQUEST_NULL;
    count_reqs[i] = MPI_REQUEST_NULL;
  }

  // status buffer
  int *is_busy = calloc(num_procs, sizeof(int));
  is_busy[0] = BUSY;

  int has_message;
  MPI_Status incoming_status;
  while (1) {
    if (total_active_tasks == 0) {
#ifdef PRINT
      printf("rank %d has no more active tasks, quitting\n", rank);
#endif
      break;
    }

    MPI_Iprobe(MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &has_message, &incoming_status);

    if (has_message && incoming_status.MPI_TAG == COUNT_TAG) {
#ifdef PRINT
      printf("rank %d received task update from %d\n", rank, incoming_status.MPI_SOURCE);
#endif
      // update global counter
      int temp_new_tasks = 0;
      MPI_Recv(&temp_new_tasks, 1, MPI_INT, incoming_status.MPI_SOURCE, COUNT_TAG, MPI_COMM_WORLD, NULL);
      total_active_tasks = total_active_tasks - 1 + temp_new_tasks;
      continue;
    } else if (has_message && incoming_status.MPI_TAG == BUSY_TAG) {
      int sender = incoming_status.MPI_SOURCE;
#ifdef PRINT
      printf("rank %d received busy update from %d\n", rank, sender);
#endif
      // update busy view, to determine who we can send tasks to
      MPI_Recv(&is_busy[sender], 1, MPI_INT, sender, BUSY_TAG, MPI_COMM_WORLD, NULL);
      continue;
    } else if (has_message && incoming_status.MPI_TAG == TASK_TAG) {
      int sender = incoming_status.MPI_SOURCE;
      // this should never exceed MAX_TASK_IN_MSG
      int num_tasks;
      MPI_Get_count(&incoming_status, MPI_TASK_T, &num_tasks);
#ifdef PRINT
      printf("rank %d received %d tasks from %d\n", rank, num_tasks, sender);
#endif
      MPI_Recv(&task_msg_buffer, num_tasks, MPI_TASK_T, sender, TASK_TAG, MPI_COMM_WORLD, NULL);

      for (int i = 0; i < num_tasks; i++) {
        task_node_t *node = (task_node_t*) calloc(1, sizeof(task_node_t));
        node->task = task_msg_buffer[i];
#ifdef PRINT
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

#ifdef PRINT
      printf("rank %d, queue length: %d\n", rank, task_queue_len);
#endif
      continue;
    }

    if (is_busy[rank]) {
      if (task_queue_len == 0) {
        // set to not busy, inform everyone that i am free
        is_busy[rank] = FREE;
        for (int i = 0; i < num_procs; i++) {
          if (i == rank) continue;

          MPI_Isend(&is_busy[rank], 1, MPI_INT, i, BUSY_TAG, MPI_COMM_WORLD, &busy_reqs[i]);
        }
        //MPI_Waitall(num_procs, busy_reqs, MPI_STATUSES_IGNORE);
        continue;
      } 
      
      if (task_queue_len > 1) {
        // distribute task until left with 1 task minimum
        int inc = rank % 2 == 0 ? 1 : -1;
        for (int i = rank % 2 == 0 ? 0 : num_procs - 1; 
        (rank % 2 == 0 && i < num_procs && task_queue_len > 1) 
        || (rank % 2 != 0 && i >= 0 && task_queue_len > 1); i = i + inc) {
          if (is_busy[i] == FREE && i != rank) {
            int num_tasks_to_send = task_queue_len > MAX_TASK_IN_MSG * 2
                           ? MAX_TASK_IN_MSG
                           : task_queue_len / 2;
#ifdef PRINT
              printf("Rank %d sent %d tasks to %d\n", rank, num_tasks_to_send, i);
#endif
            for (int j = 0; j < num_tasks_to_send; j++) {
              task_msg_buffer[j] = head->task;
              head = head->next;
              task_queue_len--;
#ifdef PRINT
              printf("rank %d is sending task type %d to %d\n", rank, task_msg_buffer[j].type, i);
#endif
            }
            MPI_Send(&task_msg_buffer, num_tasks_to_send, MPI_TASK_T, i, TASK_TAG, MPI_COMM_WORLD); 
            is_busy[i] = BUSY;
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

      for (int i = 0; i < num_procs; i++) {
        if (i == rank) continue;

        MPI_Isend(&num_new_tasks, 1, MPI_INT, i, COUNT_TAG, MPI_COMM_WORLD, &count_reqs[i]);
      }

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

      // we want to ensure num_new_tasks is fully recognized by the receivers
#ifdef PRINT
      printf("rank %d waiting for all to receive\n", rank);
#endif
      MPI_Waitall(num_procs, count_reqs, MPI_STATUSES_IGNORE);
#ifdef PRINT
      printf("rank %d finished waiting for all to receive\n", rank);
#endif

      free(curr);

#ifdef PRINT
      printf("rank %d completed a task with %d tasks in queue\n", rank, task_queue_len);
#endif
      continue;

    } else if (!is_busy[rank]) {
      if (task_queue_len == 0) {
        // continue waiting for task / updates
#ifdef PRINT
        sleep(1);
#endif
        continue;
      } else if (task_queue_len != 0) {
        is_busy[rank] = BUSY;
        for (int i = 0; i < num_procs; i++) {
          if (i == rank) continue;

          MPI_Isend(&is_busy[rank], 1, MPI_INT, i, BUSY_TAG, MPI_COMM_WORLD, &busy_reqs[i]);
        }
        //MPI_Waitall(num_procs, busy_reqs, MPI_STATUSES_IGNORE);
        continue;
      }
    } 
  }

  free(busy_reqs);
  free(count_reqs);
  free(is_busy);
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
