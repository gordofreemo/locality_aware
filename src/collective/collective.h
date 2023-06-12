#ifndef MPI_ADVANCE_COLLECTIVES_H
#define MPI_ADVANCE_COLLECTIVES_H

#ifndef THREADS_PER_GPU
#define THREADS_PER_GPU 10
#endif 

#include <stdlib.h>
#include <stdio.h>
#include <mpi.h>
#include "omp.h"
#include "utils/utils.h"
#include "allgather.h"
#include "alltoall.h"
#include "alltoallv.h"

#ifdef __cplusplus
extern "C"
{
#endif

int MPIX_Alltoall(const void* sendbuf,
        const int sendcount,
        MPI_Datatype sendtype,
        void* recvbuf,
        const int recvcount,
        MPI_Datatype recvtype,
        MPIX_Comm* comm);

int MPI_Alltoallv(const void* sendbuf,
        const int sendcounts[],
        const int sdispls[],
        MPI_Datatype sendtype,
        void* recvbuf,
        const int recvcounts[],
        const int rdispls[],
        MPI_Datatype recvtype,
        MPI_Comm comm);
int MPIX_Alltoallv(const void* sendbuf,
        const int sendcounts[],
        const int sdispls[],
        MPI_Datatype sendtype,
        void* recvbuf,
        const int recvcounts[],
        const int rdispls[],
        MPI_Datatype recvtype,
        MPIX_Comm* comm);

int MPI_Allgather(const void* sendbuf,
        int sendcount,
        MPI_Datatype sendtype,
        void* recvbuf,
        int recvcount,
        MPI_Datatype recvtype,
        MPI_Comm comm);

#ifdef __cplusplus
}
#endif


#endif
