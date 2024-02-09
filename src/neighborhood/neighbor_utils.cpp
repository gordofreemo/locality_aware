#include "mpi.h"
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
void topology_discovery_personalized(int procs[], int ptr[], int n_msgs, long off_proc_columns[], int counts[], int idx[], int first_col)
{
    int rank, num_procs;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &num_procs);

    std::vector<long> recv_buf;
    std::vector<int> sizes(num_procs, 0);
    std::vector<MPI_Request> req(n_msgs, 0);
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
        MPI_Waitall(n_msgs, req.data(), MPI_STATUSES_IGNORE);

    idx = (int *) malloc(sizeof(int) * idx_local.size());
    for (int i = 0; i < idx_local.size(); i++)
      idx[i] = idx_local[i];
}

void topology_discovery_nonblocking(int procs[], int ptr[], int n_msgs, long off_proc_columns[], int counts[], int idx[], int first_col)
{
    int rank, num_procs;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &num_procs);

    std::vector<long> recv_buf;
    std::vector<MPI_Request> req(n_msgs, 0);
    std::vector<int> idx_local;
    int start, end, proc, count, ctr, flag;
    int ibar = 0;
    MPI_Status recv_status;
    MPI_Request bar_req;
    

    // Allreduce to find size of data I will receive
    
    // Send a message to every process that I will need data from
    // Tell them which global indices I need from them
    int msg_tag = 1234;
    for (int i = 0; i < n_msgs; i++)
    {
        proc = procs[i];
        MPI_Issend(&(off_proc_columns[ptr[i]]), counts[i], MPI_LONG, proc, msg_tag,
                MPI_COMM_WORLD, &(req[i]));
    }

    // Wait to receive values
    // until I have received fewer than the number of global indices I am waiting on
    ctr = 0;
    while (1)
    {
        // Wait for a message
        MPI_Iprobe(MPI_ANY_SOURCE, msg_tag, MPI_COMM_WORLD, &flag, &recv_status);
        if (flag)
        {
            // Get the source process and message size
            proc = recv_status.MPI_SOURCE;
            // A.send_comm.procs.push_back(proc);
            MPI_Get_count(&recv_status, MPI_LONG, &count);
            // A.send_comm.counts.push_back(count);
            if (count > recv_buf.size()) recv_buf.resize(count);

            // Receive the message, and add local indices to send_comm
            MPI_Recv(recv_buf.data(), count, MPI_LONG, proc, msg_tag, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            for (int i = 0; i < count; i++)
            {
                idx_local.push_back(recv_buf[i] - first_col);
            }
            ctr += count;
            // A.send_comm.ptr.push_back((U)(ctr));
        }
        
        // If I have already called my Ibarrier, check if all processes have reached
        // If all processes have reached the Ibarrier, all messages have been sent
        if (ibar)
        {
            MPI_Test(&bar_req, &flag, MPI_STATUS_IGNORE);
            if (flag) break;
        }
        else
        {
            // Test if all of my synchronous sends have completed.
            // They only complete once actually received.
            MPI_Testall(n_msgs, req.data(), &flag, MPI_STATUSES_IGNORE);
            if (flag)
            {
                ibar = 1;
                MPI_Ibarrier(MPI_COMM_WORLD, &bar_req);
            }    
        }
    }
    
    // Set send sizes
    // A.send_comm.n_msgs = A.send_comm.procs.size();
    idx = (int *) malloc(sizeof(int) * idx_local.size());
    for (int i = 0; i < idx_local.size(); i++)
        idx[i] = idx_local[i];
}

void topology_discovery_loc_aware(int procs[], int ptr[], int n_msgs, long off_proc_columns[], int counts[], int idx[], int first_col, int size_msgs, MPIX_Comm* comm)
{
    int rank, num_procs, local_rank, PPN;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &num_procs);
    MPI_Comm_rank(comm->local_comm, &local_rank);
    MPI_Comm_size(comm->local_comm, &PPN);

    std::vector<long> recv_buf;
    std::vector<long> send_buf(size_msgs + 2*n_msgs);
    std::vector<MPI_Request> req(n_msgs, 0);
    std::vector<int> sizes(PPN, 0);
    int start, end, proc, count, ctr, flag;
    int ibar = 0;
    MPI_Status recv_status;
    MPI_Request bar_req;

    // Send a message to every process that I will need data from
    // Tell them which global indices I need from them
    int msg_tag = 1234;
    int node = -1;
    if (n_msgs > 0)
    {
        node = procs[0] / PPN;
    }
    int n_sends = 0;
    int first = 0;
    int last = 0;
    for (int i = 0; i < n_msgs; i++)
    {
        proc = procs[i];
	if (proc/PPN != node)
	{
            MPI_Issend(&(send_buf[first]), last - first, MPI_LONG, node*PPN + local_rank,
			    msg_tag, MPI_COMM_WORLD, &(req[n_sends++]));
	    first = last;
	    node = proc/PPN;
	}
	send_buf[last++] = proc;
	send_buf[last++] = counts[i];
	for (int j = 0; j < counts[i]; j++)
	{
           send_buf[last++] = off_proc_columns[ptr[i]+j];
	}
    }

    if (node >= 0)
    {
        MPI_Issend(&(send_buf[first]), last - first, MPI_LONG, node*PPN + local_rank,
                msg_tag, MPI_COMM_WORLD, &(req[n_sends++]));
    }

//    std::vector<long>* local_buf = new std::vector<long>[PPN];
    std::vector<std::vector<long>> local_buf(PPN);
    // Wait to receive values
    // until I have received fewer than the number of global indices I am waiting on

    while (1)
    {
        // Wait for a message
        MPI_Iprobe(MPI_ANY_SOURCE, msg_tag, MPI_COMM_WORLD, &flag, &recv_status);
        if (flag)
        {
            // Get the source process and message size
            proc = recv_status.MPI_SOURCE;
            MPI_Get_count(&recv_status, MPI_LONG, &count);
            if (count > recv_buf.size()) recv_buf.resize(count);

            // Receive the message, and add local indices to send_comm
            MPI_Recv(recv_buf.data(), count, MPI_LONG, proc, msg_tag, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
	    
            int idx = 0;
	    while (idx < count)
	    {
                long dest_proc = recv_buf[idx++] - (comm->rank_node*PPN);
		long dest_size = recv_buf[idx++];
		local_buf[dest_proc].push_back((long)proc);
		local_buf[dest_proc].push_back(dest_size);
		for (int i = 0; i < dest_size; i++)
                {
                    local_buf[dest_proc].push_back(recv_buf[idx++]);
                }
	    }
        }
        
	
        // If I have already called my Ibarrier, check if all processes have reached
        // If all processes have reached the Ibarrier, all messages have been sent
        if (ibar)
        {
            MPI_Test(&bar_req, &flag, MPI_STATUS_IGNORE);
            if (flag) break;
        }
        else
        {
            // Test if all of my synchronous sends have completed.
            // They only complete once actually received.
            MPI_Testall(n_sends, A.recv_comm->req.data(), &flag, MPI_STATUSES_IGNORE);
            if (flag)
            {
                ibar = 1;
                MPI_Ibarrier(MPI_COMM_WORLD, &bar_req);
            }    
        }
    }

    // STEP 2 : Local Communication
    for (int i = 0; i < PPN; i++)
    {
        sizes[i] = local_buf[i].size();
    }
    MPI_Allreduce(MPI_IN_PLACE, sizes.data(), PPN, MPI_INT, MPI_SUM, comm->local_comm);
    int local_size_msgs = sizes[local_rank];


    std::vector<MPI_Request> local_req(PPN);

    // Send a message to every process that I will need data from
    // Tell them which global indices I need from them
    int local_tag = 2345;
    n_sends = 0;
    for (int i = 0; i < PPN; i++)
    {
        if (local_buf[i].size())
        {
            MPI_Isend(local_buf[i].data(), local_buf[i].size(), MPI_LONG, i, local_tag,
                    comm->local_comm, &(local_req[n_sends++]));
        }
    }

    // Wait to receive values
    // until I have received fewer than the number of global indices I am waiting on
    if (local_size_msgs)
    {
        A.send_comm->idx.resize(local_size_msgs);
        recv_buf.resize(local_size_msgs);
    }

    ctr = 0;
    while (ctr < local_size_msgs)
    {
        // Wait for a message
        MPI_Probe(MPI_ANY_SOURCE, local_tag, comm->local_comm, &recv_status);

        // Get the source process and message size
        proc = recv_status.MPI_SOURCE;
        MPI_Get_count(&recv_status, MPI_LONG, &count);

        // Receive the message, and add local indices to send_comm
        MPI_Recv(&(recv_buf[ctr]), count, MPI_LONG, proc, local_tag, comm->local_comm, MPI_STATUS_IGNORE);
        ctr += count;
    }
    if (n_sends) MPI_Waitall(n_sends, local_req.data(), MPI_STATUSES_IGNORE);

    // Last Step : Step through recvbuf to find proc of origin, size, and indices
    ctr = 0;
    int idx_prime = 0;
    //A.send_comm->ptr.push_back(0);
    while (idx_prime < local_size_msgs)
    {
	//A.send_comm->procs.push_back(recv_buf[idx++]);
	count = recv_buf[idx_prime++];
	//A.send_comm->counts.push_back(count);
	for (int i = 0; i < count; i++)
	{
        idx_local.push_back(recv_buf[idx_prime++] - first_col);
	}
	//A.send_comm->ptr.push_back((U)(ctr));
    }

    idx = (int *) malloc(sizeof(int) * idx_local.size());
    for (int i = 0; i < idx_local.size(); i++) 
        idx[i] = idx_local[i];
    // Set send sizes
    //A.send_comm->n_msgs = A.send_comm->procs.size();
    //A.send_comm->size_msgs = A.send_comm->ptr[A.send_comm->n_msgs];

    //if (A.send_comm->n_msgs)
    //    A.send_comm->req.resize(A.send_comm->n_msgs);
    // SendComm: n_msgs, size_msgs, ptr, counts, procs, idx, req
}