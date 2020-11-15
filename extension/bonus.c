#include "tasks.h"
#include <search.h>

#define MAX_TASK_IN_MSG 50
#define PRINT 0

// arbitrarily define a max size for hashtable
#define HTABLE_SIZE 10000

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

  // for storing task ids for dependencies
  (void) hcreate(HTABLE_SIZE);
  ENTRY item;
  ENTRY *found_item;
  char key_to_find[11];
  char string_space[HTABLE_SIZE*11];
  uint32_t data_space[HTABLE_SIZE];
  char *str_ptr = string_space;
  uint32_t *data_ptr = data_space;

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
#if PRINT
      printf("rank %d has no more active tasks, quitting\n", rank);
#endif
      break;
    }

    MPI_Iprobe(MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &has_message, &incoming_status);

    if (has_message && incoming_status.MPI_TAG == COUNT_TAG) {
      // message will contain 3 things, 
      // 0 - number of new tasks, 1 - completed task id, 2 - completed output
      // update global counter
      uint32_t temp[3];
      MPI_Recv(&temp, 3, MPI_UNSIGNED, incoming_status.MPI_SOURCE, COUNT_TAG, MPI_COMM_WORLD, NULL);
#if PRINT
      printf("rank %d received task update %u from %d\n", rank, temp[1], incoming_status.MPI_SOURCE);
#endif
      total_active_tasks = total_active_tasks - 1 + temp[0];
      sprintf(str_ptr, "%u", temp[1]);
      item.key = str_ptr;
      *data_ptr = temp[2];
      item.data = data_ptr;
      //item.data = (void*) temp[2]; // store the data directly, instead of using it as an address
      str_ptr += strlen(str_ptr) + 1;
      data_ptr++;
      // insert into hashtable
      (void) hsearch(item, ENTER);
      continue;

    } else if (has_message && incoming_status.MPI_TAG == BUSY_TAG) {
      int sender = incoming_status.MPI_SOURCE;
#if PRINT
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
#if PRINT
      printf("rank %d received %d tasks from %d\n", rank, num_tasks, sender);
#endif
      MPI_Recv(&task_msg_buffer, num_tasks, MPI_TASK_T, sender, TASK_TAG, MPI_COMM_WORLD, NULL);

      for (int i = 0; i < num_tasks; i++) {
        task_node_t *node = (task_node_t*) calloc(1, sizeof(task_node_t));
        node->task = task_msg_buffer[i];
#if PRINT
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

#if PRINT
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
        for (int i = 0; i < num_procs && task_queue_len > 1; i++) {
          if (is_busy[i] == FREE && i != rank) {
            int num_tasks_to_send = task_queue_len > MAX_TASK_IN_MSG * 2
                           ? MAX_TASK_IN_MSG
                           : task_queue_len / 2;
#if PRINT
              printf("Rank %d sent %d tasks to %d\n", rank, num_tasks_to_send, i);
#endif
            for (int j = 0; j < num_tasks_to_send; j++) {
              task_msg_buffer[j] = head->task;
              head = head->next;
              task_queue_len--;
#if PRINT
              printf("rank %d is sending task type %d to %d\n", rank, task_msg_buffer[j].type, i);
#endif
            }
            MPI_Send(&task_msg_buffer, num_tasks_to_send, MPI_TASK_T, i, TASK_TAG, MPI_COMM_WORLD); 
            is_busy[i] = BUSY;
          }
        }
      }

      // if current node has unfulfilled dependencies, move it to back of list
      // and continue to next iteration
      // otherwise, update the task's arg_seed accordingly
      // this implementation is quite inefficient, as it means the process
      // will busy wait, with itself marked as busy
      // until the other procs have said that some task is complete
      // progress is guaranteed though, as it will keep checking for update messages
      // and cycle through the list.
      // we can likely improve this with some heuristics if this turns out to be really really slow
      // such as by telling others they are free to receive more tasks even though they aren't

      task_node_t *curr = head;
      int unfulfilled = 0;
      uint32_t seed = (&curr->task)->arg_seed;
      for (int i = 0; i < (&curr->task)->num_dependencies; i++) {
        sprintf(key_to_find, "%u", (&curr->task)->dependencies[i]);
        item.key = key_to_find;
        if ((found_item = hsearch(item, FIND)) == NULL) {
          // failed to find item
          unfulfilled = 1;
#if PRINT
          printf("rank %d moving %u to back of queue size %d\n", rank, (&curr->task)->id, task_queue_len);
          sleep(1);
#endif
          // move to back of list
          if (task_queue_len == 1) {
            // don't need to do anything
            break;
          }

          head = head->next;
          curr->next = NULL;
          tail->next = curr;
          tail = curr;
          break;
        } else {
          seed |= (*((uint32_t*)(found_item->data)) & (&curr->task)->masks[i]);
        }
      }

      if (unfulfilled) continue;

      // this works because arg_seed is 0 for tasks with dependencies.
      // we only do this if we have all dependencies.
#if PRINT
      for (int i = 0; i < (&curr->task)->num_dependencies; i++) {
        sprintf(key_to_find, "%u", (&curr->task)->dependencies[i]);
        item.key = key_to_find;
        found_item = hsearch(item, FIND);
        uint32_t data = *((uint32_t*) (found_item->data));
        (&curr->task)->arg_seed |= (data & (&curr->task)->masks[i]);
        printf("    r%d: task %u is dependent on task %s, with output %u, mask %u\n", rank, 
          (&curr->task)->id, found_item->key, data, (&curr->task)->masks[i]);
      }
#endif
      (&curr->task)->arg_seed = seed;
      printf("%u\n", seed);

      uint32_t temp[3];
      // execute task
      task_queue_len--;
      if (task_queue_len == 0) {
        head = tail = NULL;
      } else {
        head = head->next;
      }
      execute_task(&stats, &curr->task, (int*) &temp, task_buffer);

      total_active_tasks = total_active_tasks - 1 + temp[0];
      temp[1] = (&curr->task)->id;
      temp[2] = (&curr->task)->output;

      for (int i = 0; i < num_procs; i++) {
        if (i == rank) continue;

        MPI_Isend(&temp, 3, MPI_UNSIGNED, i, COUNT_TAG, MPI_COMM_WORLD, &count_reqs[i]);
      }
      MPI_Waitall(num_procs, count_reqs, MPI_STATUSES_IGNORE);

      // insert into hashtable
      sprintf(str_ptr, "%u", temp[1]);
      item.key = str_ptr;
      *data_ptr = temp[2];
      item.data = data_ptr;
      //item.data = (void*) temp[2]; // store the data directly, instead of using it as an address
      str_ptr += strlen(str_ptr) + 1;
      data_ptr++;
#if PRINT
      printf("-- storing key %s, data %u\n", item.key, (uint32_t) item.data);
#endif
      (void) hsearch(item, ENTER);

      // enqueue all the child tasks
      for (int i = 0; i < temp[0]; i++) {
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

#if PRINT
      printf("rank %d completed a task with %d tasks in queue\n", rank, task_queue_len);
#endif
      continue;

    } else if (!is_busy[rank]) {
      if (task_queue_len == 0) {
        // continue waiting for task / updates
#if PRINT
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
  hdestroy();

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
