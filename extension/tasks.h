#include <inttypes.h>
#include <math.h>
#include <memory.h>
#include <mpi.h>
#include <stddef.h>
#include <stdlib.h>
#include <stdio.h>
#include <stdint.h>
#include <time.h>

/*
 * ========== MODIFY THESE AS REQUIRED ==========
 */
#define DEBUG 0
#define CHILD_DEPTH 1

/*
 * ================ DO NOT MODIFY ================
 * ====== GLOBAL TASK GENERATION PARAMETERS ======
 */
#define UNSIGNED_INT_MAX 4294967295
#define INT_MAX 2147483647

extern int H, Nmin, Nmax;
extern float P;

/*
 * ================ DO NOT MODIFY ================
 * =============== TASK PARAMETERS ===============
 */
#define TASK_PRIME 1
#define TASK_MATMULT 2
#define TASK_LCS 3
#define TASK_SHA 4
#define TASK_BITONIC 5

#define PRIME_MIN ((1 << 16) - 1)
#define PRIME_MAX ((1 << 24) - 1)
#define MATMULT_MIN 400
#define MATMULT_MAX 800
#define LCS_ALPH_MIN 4
#define LCS_ALPH_MAX 10
#define LCS_MIN 16000
#define LCS_MAX 48000
#define SHA_MIN 65536
#define SHA_MAX 1048576
#define BITONIC_MIN 14
#define BITONIC_MAX 21

/*
 * ================ DO NOT MODIFY ================
 * =========== SHA256 MACROS AND TYPES ===========
 */
#define SHA256_BLOCK_SIZE 32            // SHA256 outputs a 32 byte digest

#define ROTLEFT(a, b) (((a) << (b)) | ((a) >> (32 - (b))))
#define ROTRIGHT(a, b) (((a) >> (b)) | ((a) << (32 - (b))))

#define CH(x, y, z) (((x) & (y)) ^ (~(x) & (z)))
#define MAJ(x, y, z) (((x) & (y)) ^ ((x) & (z)) ^ ((y) & (z)))
#define EP0(x) (ROTRIGHT(x, 2) ^ ROTRIGHT(x, 13) ^ ROTRIGHT(x, 22))
#define EP1(x) (ROTRIGHT(x, 6) ^ ROTRIGHT(x, 11) ^ ROTRIGHT(x, 25))
#define SIG0(x) (ROTRIGHT(x, 7) ^ ROTRIGHT(x, 18) ^ ((x) >> 3))
#define SIG1(x) (ROTRIGHT(x, 17) ^ ROTRIGHT(x, 19) ^ ((x) >> 10))

typedef unsigned char BYTE;
typedef uint32_t      WORD;

static const WORD k[64] = {
    0x428a2f98,0x71374491,0xb5c0fbcf,0xe9b5dba5,0x3956c25b,0x59f111f1,0x923f82a4,0xab1c5ed5,
    0xd807aa98,0x12835b01,0x243185be,0x550c7dc3,0x72be5d74,0x80deb1fe,0x9bdc06a7,0xc19bf174,
    0xe49b69c1,0xefbe4786,0x0fc19dc6,0x240ca1cc,0x2de92c6f,0x4a7484aa,0x5cb0a9dc,0x76f988da,
    0x983e5152,0xa831c66d,0xb00327c8,0xbf597fc7,0xc6e00bf3,0xd5a79147,0x06ca6351,0x14292967,
    0x27b70a85,0x2e1b2138,0x4d2c6dfc,0x53380d13,0x650a7354,0x766a0abb,0x81c2c92e,0x92722c85,
    0xa2bfe8a1,0xa81a664b,0xc24b8b70,0xc76c51a3,0xd192e819,0xd6990624,0xf40e3585,0x106aa070,
    0x19a4c116,0x1e376c08,0x2748774c,0x34b0bcb5,0x391c0cb3,0x4ed8aa4a,0x5b9cca4f,0x682e6ff3,
    0x748f82ee,0x78a5636f,0x84c87814,0x8cc70208,0x90befffa,0xa4506ceb,0xbef9a3f7,0xc67178f2
};

typedef struct {
    BYTE data[64];
    WORD datalen;
    unsigned long long bitlen;
    WORD state[8];
} SHA256_CTX;

// ============= END SHA-256 SECTION =============

static const int params_t_lengths[2] = { 3, 1 };
static const MPI_Aint params_t_displs[2] = { 0, 12 };
static const MPI_Datatype params_t_types[2] = { MPI_INT, MPI_FLOAT };

static const int metric_t_lengths[2] = { 6, 2 };
static const MPI_Aint metric_t_displs[2] = { 0, 24 };
static const MPI_Datatype metric_t_types[3] = { MPI_INT, MPI_UINT64_T };

typedef struct {
    int H;
    int Nmin;
    int Nmax;
    float P;
} params_t;

typedef struct timespec timespec_t;

typedef struct {
    int rank;
    int completed[5];
    uint64_t total_time;
    uint64_t compute_time;
} metric_t;

typedef struct {
    uint32_t id;
    int gen;
    int type;
    uint32_t arg_seed;
    uint32_t output;
    int num_dependencies;
    uint32_t dependencies[4];
    uint32_t masks[4];
} task_t;

/*
 * ================ DO NOT MODIFY ================
 * === TASK GENERATION AND EXECUTION FUNCTIONS ===
 */
// Pseudo-random number generator
uint32_t get_next(uint32_t seed);

params_t parse_params(char *argv[]);
void set_generation_params(params_t* params);
uint64_t compute_interval(timespec_t *start, timespec_t *end);
void execute_task(metric_t *stats, task_t *task, int *num_generated,
        task_t children[]);
void generate_desc_tasks(task_t *root, uint32_t seed, int *num_generated,
        task_t children[]);

/*
 * ================ DO NOT MODIFY ================
 * ========= TASK PROCEDURES AND HELPERS =========
 * ======== SHOULD NOT BE INVOKED DIRECTLY =======
 */
uint32_t PRIME(uint32_t num);
uint32_t MATMULT(int m, int n, int p, uint32_t seed);
uint32_t LCS(int alph, int len1, int len2, uint32_t seed);
uint32_t BITONIC(int n, uint32_t seed);
char* SHA(int len, uint32_t seed);

// Sub-routines for bitonic sort
void bitonic_compare_swap(uint32_t a[], int i, int j, int dir);
void bitonic_merge(uint32_t a[], int low, int cnt, int dir);
void bitonic_sort(uint32_t a[], int low, int cnt, int dir);

// Helper routines for SHA-256
void sha256_init(SHA256_CTX *ctx);
void sha256_update(SHA256_CTX *ctx, const BYTE data[], size_t len);
void sha256_final(SHA256_CTX *ctx, BYTE hash[]);

/*
 * ================ DO NOT MODIFY ================
 * Useful helper routines for I/O
 */
void print_metrics(metric_t *stats);
void print_task(task_t *task, int num_desc);

