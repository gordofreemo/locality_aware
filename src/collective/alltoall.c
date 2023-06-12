#include "alltoall.h"
#include <string.h>
#include <math.h>

// TODO : Add Locality-Aware Bruck Alltoall Algorithm!
// TODO : Change to PMPI_Alltoall and test with profiling library!

/**************************************************
 * Locality-Aware Point-to-Point Alltoall
 *  - Aggregates messages locally to reduce 
 *      non-local communciation
 *  - First redistributes on-node so that each
 *      process holds all data for a subset
 *      of other nodes
 *  - Then, performs inter-node communication
 *      during which each process exchanges
 *      data with their assigned subset of nodes
 *  - Finally, redistribute received data
 *      on-node so that each process holds
 *      the correct final data
 *************************************************/
int MPI_Alltoall(const void* sendbuf,
        const int sendcount,
        MPI_Datatype sendtype,
        void* recvbuf,
        const int recvcount,
        MPI_Datatype recvtype,
        MPI_Comm comm)
{
    return alltoall_pairwise(sendbuf,
        sendcount,
        sendtype,
        recvbuf,
        recvcount,
        recvtype,
        comm);
}


int MPIX_Alltoall(const void* sendbuf,
        const int sendcount,
        MPI_Datatype sendtype,
        void* recvbuf,
        const int recvcount,
        MPI_Datatype recvtype,
        MPIX_Comm* mpi_comm)
{    
    return alltoall_pairwise(sendbuf,
        sendcount,
        sendtype,
        recvbuf,
        recvcount,
        recvtype,
        mpi_comm->global_comm);
}

int alltoall_pairwise(const void* sendbuf,
        const int sendcount,
        MPI_Datatype sendtype,
        void* recvbuf,
        const int recvcount,
        MPI_Datatype recvtype,
        MPI_Comm comm)
{
    int rank, num_procs;
    MPI_Comm_rank(comm, &rank);
    MPI_Comm_size(comm, &num_procs);

    int tag = 102944;
    int send_proc, recv_proc;
    int send_pos, recv_pos;
    MPI_Status status;

    char* recv_buffer = (char*)recvbuf;
    char* send_buffer = (char*)sendbuf;

    int send_size, recv_size;
    MPI_Type_size(sendtype, &send_size);
    MPI_Type_size(recvtype, &recv_size);

#ifdef GPU
    cudaMemoryType send_type, recv_type;
    cudaPointerAttributes mem;
    cudaPointerGetAttributes(&mem, sendbuf);
    int ierr = cudaGetLastError();
    if (ierr == cudaErrorInvalidValue)
        send_type = cudaMemoryTypeHost;
    else
        send_type = mem.type;
    cudaPointerGetAttributes(&mem, recvbuf);
    ierr = cudaGetLastError();
    if (ierr == cudaErrorInvalidValue)
        recv_type = cudaMemoryTypeHost;
    else
        recv_type = mem.type;

    if (send_type == cudaMemoryTypeDevice &&
            recv_type == cudaMemoryTypeDevice)
        cudaMemcpy(recv_buffer + (rank * recvcount * recv_size),
                send_buffer + (rank * sendcount * send_size),
                sendcount * send_size,
                cudaMemcpyDeviceToDevice);
    else if (send_type == cudaMemoryTypeDevice)
        cudaMemcpy(recv_buffer + (rank * recvcount * recv_size),
                send_buffer + (rank * sendcount * send_size),
                sendcount * send_size,
                cudaMemcpyDeviceToHost);
    else if (recv_type == cudaMemoryTypeDevice)
        cudaMemcpy(recv_buffer + (rank * recvcount * recv_size),
                send_buffer + (rank * sendcount * send_size), 
                sendcount * send_size,
                cudaMemcpyHostToDevice);
    else
#endif
    memcpy(recv_buffer + (rank * recvcount * recv_size),
        send_buffer + (rank * sendcount * send_size),
        sendcount * send_size);


    // Send to rank + i
    // Recv from rank - i
    for (int i = 1; i < num_procs; i++)
    {
        send_proc = rank + i;
        if (send_proc >= num_procs)
            send_proc -= num_procs;
        recv_proc = rank - i;
        if (recv_proc < 0)
            recv_proc += num_procs;
        send_pos = send_proc * sendcount * send_size;
        recv_pos = recv_proc * recvcount * recv_size;

        MPI_Sendrecv(send_buffer + send_pos, 
                sendcount, 
                sendtype, 
                send_proc, 
                tag,
                recv_buffer + recv_pos, 
                recvcount, 
                recvtype, 
                recv_proc, 
                tag,
                comm, 
                &status);
    }
    return MPI_SUCCESS;
}

int alltoall_nonblocking(const void* sendbuf,
        const int sendcount,
        MPI_Datatype sendtype,
        void* recvbuf,
        const int recvcount,
        MPI_Datatype recvtype,
        MPI_Comm comm)
{
    int rank, num_procs;
    MPI_Comm_rank(comm, &rank);
    MPI_Comm_size(comm, &num_procs);

    int tag = 102944;
    int send_proc, recv_proc;
    int send_pos, recv_pos;
    MPI_Status status;

    char* recv_buffer = (char*)recvbuf;
    char* send_buffer = (char*)sendbuf;

    int send_size, recv_size;
    MPI_Type_size(sendtype, &send_size);
    MPI_Type_size(recvtype, &recv_size);

    MPI_Request* requests = (MPI_Request*)malloc(2*(num_procs-1)*sizeof(MPI_Request));

#ifdef GPU
    cudaMemoryType send_type, recv_type;
    cudaPointerAttributes mem;
    cudaPointerGetAttributes(&mem, sendbuf);
    int ierr = cudaGetLastError();
    if (ierr == cudaErrorInvalidValue)
        send_type = cudaMemoryTypeHost;
    else
        send_type = mem.type;
    cudaPointerGetAttributes(&mem, recvbuf);
    ierr = cudaGetLastError();
    if (ierr == cudaErrorInvalidValue)
        recv_type = cudaMemoryTypeHost;
    else
        recv_type = mem.type;

    if (send_type == cudaMemoryTypeDevice &&
            recv_type == cudaMemoryTypeDevice)
        cudaMemcpy(recv_buffer + (rank * recvcount * recv_size),
                send_buffer + (rank * sendcount * send_size),
                sendcount * send_size,
                cudaMemcpyDeviceToDevice);
    else if (send_type == cudaMemoryTypeDevice)
        cudaMemcpy(recv_buffer + (rank * recvcount * recv_size),
                send_buffer + (rank * sendcount * send_size),
                sendcount * send_size,
                cudaMemcpyDeviceToHost);
    else if (recv_type == cudaMemoryTypeDevice)
        cudaMemcpy(recv_buffer + (rank * recvcount * recv_size),
                send_buffer + (rank * sendcount * send_size),
                sendcount * send_size,
                cudaMemcpyHostToDevice);
    else
#endif
    memcpy(recv_buffer + (rank * recvcount * recv_size),
        send_buffer + (rank * sendcount * send_size),
        sendcount * send_size);

    // Send to rank + i
    // Recv from rank - i
    for (int i = 1; i < num_procs; i++)
    {
        send_proc = rank + i;
        if (send_proc >= num_procs)
            send_proc -= num_procs;
        recv_proc = rank - i;
        if (recv_proc < 0)
            recv_proc += num_procs;
        send_pos = send_proc * sendcount * send_size;
        recv_pos = recv_proc * recvcount * recv_size;

        MPI_Isend(send_buffer + send_pos,
                sendcount, 
                sendtype, 
                send_proc,
                tag, 
                comm,
                &(requests[i-1]));
        MPI_Irecv(recv_buffer + recv_pos,
                recvcount,
                recvtype,
                recv_proc,
                tag,
                comm,
                &(requests[num_procs + i - 2]));
    }

    MPI_Waitall(2*(num_procs-1), requests, MPI_STATUSES_IGNORE);

    free(requests);
    return 0;
}

int alltoall_bruck(const void* sendbuf,
        const int sendcount,
        MPI_Datatype sendtype,
        void* recvbuf,
        const int recvcount,
        MPI_Datatype recvtype,
        MPI_Comm comm)
{
    int rank, num_procs;
    MPI_Comm_rank(comm, &rank);
    MPI_Comm_size(comm, &num_procs);

    int tag = 102944;
    MPI_Request requests[2];

    char* recv_buffer = (char*)recvbuf;

    int recv_size;
    MPI_Type_size(recvtype, &recv_size);

    if (sendbuf != recvbuf)
        memcpy(recvbuf, sendbuf, recvcount*recv_size*num_procs);

    // Perform all-to-all
    int stride, ctr, group_size;
    int send_proc, recv_proc, size;
    int num_steps = log2((float)(num_procs));
    int msg_size = recvcount*recv_size;
    int total_count = recvcount*num_procs;

    // TODO : could have only half this size
    char* contig_buf = (char*)malloc(total_count*recv_size);
    char* tmpbuf = (char*)malloc(total_count*recv_size);

    // 1. rotate local data
    if (rank)
        rotate(recv_buffer, rank*msg_size, num_procs*msg_size);

    // 2. send to left, recv from right
    stride = 1;
    for (int i = 0; i < num_steps; i++)
    {
        recv_proc = rank - stride;
        if (recv_proc < 0) recv_proc += num_procs;
        send_proc = rank + stride;
        if (send_proc >= num_procs) send_proc -= num_procs;

        group_size = stride * recvcount;
        
        ctr = 0;
        for (int i = group_size; i < total_count; i += (group_size*2))
        {
            for (int j = 0; j < group_size; j++)
            {
                for (int k = 0; k < recv_size; k++)
                {
                    contig_buf[ctr*recv_size+k] = recv_buffer[(i+j)*recv_size+k];
                }
                ctr++;
            }
        }

        size = ((int)(total_count / group_size) * group_size) / 2;

        MPI_Isend(contig_buf, size, recvtype, send_proc, tag, comm, &(requests[0]));
        MPI_Irecv(tmpbuf, size, recvtype, recv_proc, tag, comm, &(requests[1]));
        MPI_Waitall(2, requests, MPI_STATUSES_IGNORE);

        ctr = 0;
        for (int i = group_size; i < total_count; i += (group_size*2))
        {
            for (int j = 0; j < group_size; j++)
            {
                for (int k = 0; k < recv_size; k++)
                {
                    recv_buffer[(i+j)*recv_size+k] = tmpbuf[ctr*recv_size+k];
                }
                ctr++;
            }
        }

        stride *= 2;

    } 

    // 3. rotate local data
    if (rank < num_procs)
        rotate(recv_buffer, (rank+1)*msg_size, num_procs*msg_size);


    // TODO :: REVERSE!

    return 0;
}


// 2-Step Aggregation (large messages)
// Gather all data to be communicated between nodes
// Send to node+i, recv from node-i
int alltoall_pairwise_loc(const void* sendbuf,
        const int sendcount,
        MPI_Datatype sendtype,
        void* recvbuf,
        const int recvcount,
        MPI_Datatype recvtype,
        MPIX_Comm* mpi_comm)
{
    int rank, num_procs;
    int local_rank, PPN; 
    int num_nodes, rank_node;
    MPI_Comm_rank(mpi_comm->global_comm, &rank);
    MPI_Comm_size(mpi_comm->global_comm, &num_procs);
    MPI_Comm_rank(mpi_comm->local_comm, &local_rank);
    MPI_Comm_size(mpi_comm->local_comm, &PPN);
    num_nodes = mpi_comm->num_nodes;
    rank_node = mpi_comm->rank_node;

    const char* send_buffer = (char*) sendbuf;
    char* recv_buffer = (char*) recvbuf;

    int sbytes, rbytes;
    MPI_Type_size(sendtype, &sbytes);
    MPI_Type_size(recvtype, &rbytes);
    int sendcount_node = sendcount * PPN;
    int recvcount_node = recvcount * PPN;
    int recv_bytes = recvcount*rbytes;
    int send_bytes_node = sendcount_node * sbytes;
    int recv_bytes_node = recvcount_node * rbytes;

    int tag = 102913;
    int send_proc, recv_proc;
    int send_pos, recv_pos;
    int send_node, recv_node;
    MPI_Status status;
    char* tmpbuf = (char*)malloc(num_procs*recv_bytes);

    /************************************************
     * Step 1 : Send aggregated data to node
     ***********************************************/
    for (int i = 0; i < num_nodes; i++)
    {
        send_node = rank_node + i;
        if (send_node >= num_nodes)
            send_node -= num_nodes;
        recv_node = rank_node - i;
        if (recv_node < 0)
            recv_node += num_nodes;

        send_pos = send_node * send_bytes_node;
        recv_pos = recv_node * recv_bytes_node;

        MPI_Sendrecv(send_buffer + send_pos, sendcount_node, sendtype, 
                send_node*PPN + local_rank, tag,
                tmpbuf + recv_pos, recvcount_node, recvtype,
                recv_node*PPN + local_rank, tag, 
                mpi_comm->global_comm, &status);
    }

    /************************************************
     * Step 2 : Redistribute received data within node
     ************************************************/
    for (int i = 0; i < num_nodes; i++)
        for (int j = 0; j < PPN; j++)
            memcpy(recv_buffer + ((j*num_nodes+i)*recv_bytes),
                    tmpbuf + ((i*PPN+j)*recv_bytes),
                    recv_bytes);

    for (int i = 0; i < PPN; i++)
    {
        send_proc = local_rank + i;
        if (send_proc >= PPN)
            send_proc -= PPN;
        recv_proc = local_rank - i;
        if (recv_proc < 0)
            recv_proc += PPN;

        send_pos = send_proc * recv_bytes * num_nodes;
        recv_pos = recv_proc * recv_bytes * num_nodes;

        MPI_Sendrecv(recv_buffer + send_pos, recvcount * num_nodes, recvtype,
                send_proc, tag,
                tmpbuf + recv_pos, recvcount * num_nodes, recvtype,
                recv_proc, tag,
                mpi_comm->local_comm, &status);

    }

    for (int i = 0; i < num_nodes; i++)
        for (int j = 0; j < PPN; j++)
            memcpy(recv_buffer + ((i*PPN+j)*recv_bytes),
                    tmpbuf + ((j*num_nodes+i)*recv_bytes),
                    recv_bytes);

    free(tmpbuf);

    return 0;
}

