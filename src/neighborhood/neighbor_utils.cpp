#include "neighbor.h"
#include <vector>
#include <unistd.h>
#include <stdlib.h>


/*
  Modifications to this function compared to old method:
    - Removed struct members and replaced them with arguments to function (assume template U was int)
    - Comment out the lines that pushed back to the send_comm
    - Made idx vector a local vector
    - Made req vector a local vector 
    - Made malloc to idx vector 
    - Commented out allocation to n_msgs
*/
void form_send_comm_standard(int procs[], int ptr[], int n_msgs, long off_proc_columns[], int counts[], int idx[], int first_col)
{
    int rank, num_procs;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &num_procs);

    std::vector<long> recv_buf;
    std::vector<int> sizes(num_procs, 0);
    std::vector<MPI_Request> req;
    std::vector<int> idx_local; 
    int start, end, proc, count, ctr;
    int size_msgs;
    MPI_Status recv_status;

    // Allreduce to find size of data I will receive
    for (int i = 0; i < n_msgs; i++)
        sizes[procs[i]] = ptr[i+1] - ptr[i];
    MPI_Allreduce(MPI_IN_PLACE, sizes.data(), num_procs, MPI_INT, MPI_SUM, MPI_COMM_WORLD);
    size_msgs = sizes[rank];

    int msg_tag = 1234;
    for (int i = 0; i < n_msgs; i++)
    {
        proc = procs[i];
        MPI_Isend(&(off_proc_columns[ptr[i]]), counts[i], MPI_LONG, proc, msg_tag, 
                MPI_COMM_WORLD, &(req[i]));
    }

    // Wait to receive values
    // until I have received fewer than the number of global indices I am waiting on
    if (size_msgs)
    {
        idx_local.resize(size_msgs);
        recv_buf.resize(size_msgs);
    }
    ctr = 0;
    // A.send_comm->ptr.push_back(0);
    while (ctr < size_msgs)
    {
        // Wait for a message
        MPI_Probe(MPI_ANY_SOURCE, msg_tag, MPI_COMM_WORLD, &recv_status);

        // Get the source process and message size
        proc = recv_status.MPI_SOURCE;
        // A.send_comm->procs.push_back(proc);
        MPI_Get_count(&recv_status, MPI_LONG, &count);
        // A.send_comm->counts.push_back(count);

        // Receive the message, and add local indices to send_comm
        MPI_Recv(&(recv_buf[ctr]), count, MPI_LONG, proc, msg_tag, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        for (int i = 0; i < count; i++)
        {
            idx_local[ctr+i] = (recv_buf[ctr+i] - first_col);
        }
        ctr += count;
        // A.send_comm->ptr.push_back((U)(ctr));
    }
    
    // Set send sizes
    // n_msgs = A.send_comm->procs.size();

    if (n_msgs)
        req.resize(n_msgs);

    if (n_msgs)
        MPI_Waitall(n_msgs, req.data(), MPI_STATUSES_IGNORE);

    idx = (int *) malloc(sizeof(int) * idx_local.size());
    for (int i = 0; i < idx_local.size(); i++)
      idx[i] = idx_local[i];
}
