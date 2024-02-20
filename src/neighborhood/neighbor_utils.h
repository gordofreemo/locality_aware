#ifndef MPI_ADVANCED_NEIGHBOR_UTILS_H
#define MPI_ADVANCED_NEIGHBOR_UTILS_H

#include "mpi.h"
#include "neighbor.h"
#include <vector>
#include <unistd.h>
#include <stdlib.h>

// Declarations of C++ methods
#ifdef __cplusplus
extern "C"
{
#endif

void topology_discovery_personalized(
        int recv_procs[], 
        int recv_ptr[], 
        int recv_n_msgs, 
        long off_proc_columns[], 
        int recv_counts[], 
        int first_col, 
        int* send_size_msgs, 
        int send_idx[], 
        int send_ptr[], 
        int send_procs[], 
        int send_counts[], 
        MPI_Request send_req[], 
        int* send_n_msgs);

void topology_discovery_nonblocking(
        int procs[], 
        int ptr[], 
        int n_msgs, 
        long off_proc_columns[],
        int counts[], 
        int idx[], 
        int first_col);

void topology_discovery_loc_aware(
        int procs[], 
        int ptr[], 
        int n_msgs, 
        long off_proc_columns[], 
        int counts[], 
        int idx[], 
        int first_col, 
        int size_msgs, 
        MPIX_Comm* comm);


#ifdef __cplusplus
}
#endif

#endif
