#ifndef MPI_ADVANCE_COMM_CREATION_H
#define MPI_ADVANCE_COMM_CREATION_H

#include "mpi.h"
#include <vector>

// Declarations of C++ methods
#ifdef __cplusplus
extern "C"
{
#endif

// Must Form Recv Comm before Send!
template <typename U>
void form_send_comm_standard(ParMat<U>& A);

// Must Form Recv Comm before Send!
template <typename U>
void form_send_comm_torsten(ParMat<U>& A);

// Must Form Recv Comm before Send!
template <typename U>
void form_send_comm_rma(ParMat<U>& A);

// Must Form Recv Comm before Send!
void allocate_rma_dynamic(MPI_Win* win, int** sizes);
void free_rma_dynamic(MPI_Win* win, int* sizes);
template <typename U>
void form_send_comm_rma_dynamic(ParMat<U>& A, MPI_Win win, int* sizes);

#ifdef __cplusplus
}
#endif

#endif







