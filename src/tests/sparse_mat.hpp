#ifndef MPI_SPARSE_MAT_HPP
#define MPI_SPARSE_MAT_HPP

#include "mpi.h"
#include <vector>

#include <unistd.h>

class Mat
{
  public:
    std::vector<int> rowptr;
    std::vector<int> col_idx;
    std::vector<double> data;
    int n_rows;
    int n_cols;
    int nnz;
};

template <typename U>
class Comm 
{
  public:
    int n_msgs;
    int size_msgs;
    std::vector<int> procs;
    std::vector<U> ptr;
    std::vector<int> counts;
    std::vector<int> idx;
    std::vector<MPI_Request> req;

    Comm()
    {
        n_msgs = 0;
        size_msgs = 0;
    }

    ~Comm()
    {
    }
};

template <typename U>
class ParMat 
{
  public:
    Mat on_proc;
    Mat off_proc;
    int global_rows;
    int global_cols;
    int local_rows;
    int local_cols;
    int first_row;
    int first_col;
    int off_proc_num_cols;
    std::vector<long> off_proc_columns;
    Comm<U>* send_comm;
    Comm<U>* recv_comm;
    MPI_Comm dist_graph_comm;

    ParMat()
    {
        send_comm = new Comm<U>();
	recv_comm = new Comm<U>();
    }

    ~ParMat()
    {
        delete send_comm;
	delete recv_comm;
    }

    void reset_comm()
    {
	delete send_comm;
	send_comm = new Comm<U>();
    }
};

template <typename U>
void form_recv_comm(ParMat<U>& A)
{
    int rank, num_procs;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &num_procs);

    // Gather first col for all processes into list
    std::vector<int> first_cols(num_procs+1);
    MPI_Allgather(&A.first_col, 1, MPI_INT, first_cols.data(), 1, MPI_INT, MPI_COMM_WORLD);
    first_cols[num_procs] = A.global_cols;

    // Map Columns to Processes
    int proc = 0;
    int prev_proc = -1;
    for (int i = 0; i < A.off_proc_num_cols; i++)
    {
        int global_col = A.off_proc_columns[i];
        while (first_cols[proc+1] <= global_col)
            proc++;
        if (proc != prev_proc)
        {
            A.recv_comm->procs.push_back(proc);
            A.recv_comm->ptr.push_back((U)(i));
            prev_proc = proc;
        }
    }

    // Set Recv Sizes
    A.recv_comm->ptr.push_back((U)(A.off_proc_num_cols));
    A.recv_comm->n_msgs = A.recv_comm->procs.size();
    A.recv_comm->size_msgs = A.off_proc_num_cols;
    if (A.recv_comm->n_msgs == 0)
        return;

    A.recv_comm->req.resize(A.recv_comm->n_msgs);
    A.recv_comm->counts.resize(A.recv_comm->n_msgs);
    for (int i = 0; i < A.recv_comm->n_msgs; i++)
        A.recv_comm->counts[i] = A.recv_comm->ptr[i+1] - A.recv_comm->ptr[i];
}

// Must Form Recv Comm before Send!
template <typename U>
void form_send_comm_standard(ParMat<U>& A)
{
    int rank, num_procs;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &num_procs);

    double t0, tfinal;

    std::vector<long> recv_buf;
    std::vector<int> sizes(num_procs, 0);
    int start, end, proc, count, ctr;
    MPI_Status recv_status;

    // Allreduce to find size of data I will receive
#ifdef PROFILE
MPI_Barrier(MPI_COMM_WORLD);
t0 = MPI_Wtime();
#endif

    for (int i = 0; i < A.recv_comm->n_msgs; i++)
        sizes[A.recv_comm->procs[i]] = A.recv_comm->ptr[i+1] - A.recv_comm->ptr[i];
    MPI_Allreduce(MPI_IN_PLACE, sizes.data(), num_procs, MPI_INT, MPI_SUM, MPI_COMM_WORLD);
    A.send_comm->size_msgs = sizes[rank];

#ifdef PROFILE
tfinal = (MPI_Wtime() - t0);
MPI_Reduce(&tfinal, &t0, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
if (rank == 0) printf("Allreduce time %e\n", t0);

    // Send a message to every process that I will need data from
    // Tell them which global indices I need from them
MPI_Barrier(MPI_COMM_WORLD);
t0 = MPI_Wtime();
#endif
    int msg_tag = 1234;
    for (int i = 0; i < A.recv_comm->n_msgs; i++)
    {
        proc = A.recv_comm->procs[i];
        MPI_Isend(&(A.off_proc_columns[A.recv_comm->ptr[i]]), A.recv_comm->counts[i], MPI_LONG, proc, msg_tag, 
                MPI_COMM_WORLD, &(A.recv_comm->req[i]));
    }

    // Wait to receive values
    // until I have received fewer than the number of global indices I am waiting on
    if (A.send_comm->size_msgs)
    {
        A.send_comm->idx.resize(A.send_comm->size_msgs);
        recv_buf.resize(A.send_comm->size_msgs);
    }
    ctr = 0;
    A.send_comm->ptr.push_back(0);
    while (ctr < A.send_comm->size_msgs)
    {
        // Wait for a message
        MPI_Probe(MPI_ANY_SOURCE, msg_tag, MPI_COMM_WORLD, &recv_status);

        // Get the source process and message size
        proc = recv_status.MPI_SOURCE;
        A.send_comm->procs.push_back(proc);
        MPI_Get_count(&recv_status, MPI_LONG, &count);
        A.send_comm->counts.push_back(count);

        // Receive the message, and add local indices to send_comm
        MPI_Recv(&(recv_buf[ctr]), count, MPI_LONG, proc, msg_tag, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        for (int i = 0; i < count; i++)
        {
            A.send_comm->idx[ctr+i] = (recv_buf[ctr+i] - A.first_col);
        }
        ctr += count;
        A.send_comm->ptr.push_back((U)(ctr));
    }
    
    // Set send sizes
    A.send_comm->n_msgs = A.send_comm->procs.size();

    if (A.send_comm->n_msgs)
        A.send_comm->req.resize(A.send_comm->n_msgs);

    if (A.recv_comm->n_msgs)
        MPI_Waitall(A.recv_comm->n_msgs, A.recv_comm->req.data(), MPI_STATUSES_IGNORE);
#ifdef PROFILE
tfinal = (MPI_Wtime() - t0);
MPI_Reduce(&tfinal, &t0, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
if (rank == 0) printf("Standard P2P time %e\n", t0);
#endif
}

// Must Form Recv Comm before Send!
template <typename U>
void form_send_comm_torsten_loc(ParMat<U>& A, MPIX_Comm* comm)
{
    int rank, num_procs, local_rank, PPN;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &num_procs);
    MPI_Comm_rank(comm->local_comm, &local_rank);
    MPI_Comm_size(comm->local_comm, &PPN);

    std::vector<long> recv_buf;
    std::vector<long> send_buf(A.recv_comm->size_msgs + 2*A.recv_comm->n_msgs);
    std::vector<int> sizes(PPN, 0);
    int start, end, proc, count, ctr, flag;
    int ibar = 0;
    MPI_Status recv_status;
    MPI_Request bar_req;

    // Send a message to every process that I will need data from
    // Tell them which global indices I need from them
    int msg_tag = 1234;
    int node = -1;
    if (A.recv_comm->n_msgs > 0)
    {
        node = A.recv_comm->procs[0] / PPN;
    }
    int n_sends = 0;
    int first = 0;
    int last = 0;
    for (int i = 0; i < A.recv_comm->n_msgs; i++)
    {
        proc = A.recv_comm->procs[i];
	if (proc/PPN != node)
	{
            MPI_Issend(&(send_buf[first]), last - first, MPI_LONG, node*PPN + local_rank,
			    msg_tag, MPI_COMM_WORLD, &(A.recv_comm->req[n_sends++]));
	    first = last;
	    node = proc/PPN;
	}
	send_buf[last++] = proc;
	send_buf[last++] = A.recv_comm->counts[i];
	for (int j = 0; j < A.recv_comm->counts[i]; j++)
	{
           send_buf[last++] = A.off_proc_columns[A.recv_comm->ptr[i]+j];
	}
    }

    if (node >= 0)
    {
        MPI_Issend(&(send_buf[first]), last - first, MPI_LONG, node*PPN + local_rank,
                msg_tag, MPI_COMM_WORLD, &(A.recv_comm->req[n_sends++]));
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
    int idx = 0;
    A.send_comm->ptr.push_back(0);
    while (idx < local_size_msgs)
    {
	A.send_comm->procs.push_back(recv_buf[idx++]);
	count = recv_buf[idx++];
	A.send_comm->counts.push_back(count);
	for (int i = 0; i < count; i++)
	{
	    A.send_comm->idx[ctr++] = (recv_buf[idx++] - A.first_col);
	}
	A.send_comm->ptr.push_back((U)(ctr));
    }
    
    // Set send sizes
    A.send_comm->n_msgs = A.send_comm->procs.size();
    A.send_comm->size_msgs = A.send_comm->ptr[A.send_comm->n_msgs];

    if (A.send_comm->n_msgs)
        A.send_comm->req.resize(A.send_comm->n_msgs);
    // SendComm: n_msgs, size_msgs, ptr, counts, procs, idx, req
}


// Must Form Recv Comm before Send!
template <typename U>
void form_send_comm_torsten(ParMat<U>& A)
{
    double t0, tfinal;
    int rank, num_procs;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &num_procs);

    std::vector<long> recv_buf;
    int start, end, proc, count, ctr, flag;
    int ibar = 0;
    MPI_Status recv_status;
    MPI_Request bar_req;

    // Allreduce to find size of data I will receive
    // Send a message to every process that I will need data from
    // Tell them which global indices I need from them
    int msg_tag = 72043;
    for (int i = 0; i < A.recv_comm->n_msgs; i++)
    {
        proc = A.recv_comm->procs[i];
        MPI_Issend(&(A.off_proc_columns[A.recv_comm->ptr[i]]), A.recv_comm->counts[i], MPI_LONG, proc, msg_tag, 
                MPI_COMM_WORLD, &(A.recv_comm->req[i]));
    }

    ctr = 0;
    A.send_comm->ptr.push_back(0);
    while (1)
    {
        // Wait for a message
        MPI_Iprobe(MPI_ANY_SOURCE, msg_tag, MPI_COMM_WORLD, &flag, &recv_status);
        if (flag)
        {
            // Get the source process and message size
            proc = recv_status.MPI_SOURCE;
            A.send_comm->procs.push_back(proc);
            MPI_Get_count(&recv_status, MPI_LONG, &count);
            A.send_comm->counts.push_back(count);
            if (count > recv_buf.size()) recv_buf.resize(count);

            // Receive the message, and add local indices to send_comm
            MPI_Recv(recv_buf.data(), count, MPI_LONG, proc, msg_tag, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            for (int i = 0; i < count; i++)
            {
                A.send_comm->idx.push_back(recv_buf[i] - A.first_col);
            }
            ctr += count;
            A.send_comm->ptr.push_back((U)(ctr));
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
            MPI_Testall(A.recv_comm->n_msgs, A.recv_comm->req.data(), &flag, MPI_STATUSES_IGNORE);
            if (flag)
            {
                ibar = 1;
                MPI_Ibarrier(MPI_COMM_WORLD, &bar_req);
            }    
        }
    }


    // Set send sizes
    A.send_comm->n_msgs = A.send_comm->procs.size();
    A.send_comm->size_msgs = ctr;
    if (A.send_comm->n_msgs)
        A.send_comm->req.resize(A.send_comm->n_msgs);
    if (A.send_comm->size_msgs)
        A.send_comm->idx.resize(A.send_comm->size_msgs);
}

template <typename U>
void form_comm(ParMat<U>& A)
{
    // Form Recv Side 
    form_recv_comm(A);

    // Form Send Side (Algorithm Options Here!)
    //form_send_comm_standard(A);
    //form_send_comm_torsten(A);
    form_send_comm_rma(A);
}


template <typename U, typename T>
void communicate(ParMat<T>& A, std::vector<U>& data, std::vector<U>& recvbuf, MPI_Datatype type)
{
    
	int rank; MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    int proc;
    T start, end;
    int tag = 2948;
    std::vector<U> sendbuf;
    double t0, tfinal;
    #ifdef PROFILE
    MPI_Barrier(MPI_COMM_WORLD);
    t0=MPI_Wtime();
    #endif
    if (A.send_comm->size_msgs)
        sendbuf.resize(A.send_comm->size_msgs);
    for (int i = 0; i < A.send_comm->n_msgs; i++)
    {
        proc = A.send_comm->procs[i];
        start = A.send_comm->ptr[i];
        end = A.send_comm->ptr[i+1];
        for (T j = start; j < end; j++)
        {
            sendbuf[j] = data[A.send_comm->idx[j]];
        }
        MPI_Isend(&(sendbuf[start]), (int)(end - start), type, proc, tag, 
                MPI_COMM_WORLD, &(A.send_comm->req[i]));
    }

    for (int i = 0; i < A.recv_comm->n_msgs; i++)
    {
        proc = A.recv_comm->procs[i];
        start = A.recv_comm->ptr[i];
        end = A.recv_comm->ptr[i+1];
        MPI_Irecv(&(recvbuf[start]), (int)(end - start), type, proc, tag,
                MPI_COMM_WORLD, &(A.recv_comm->req[i]));
    }

    if (A.send_comm->n_msgs)
        MPI_Waitall(A.send_comm->n_msgs, A.send_comm->req.data(), MPI_STATUSES_IGNORE);
    if (A.recv_comm->n_msgs)
    MPI_Waitall(A.recv_comm->n_msgs, A.recv_comm->req.data(), MPI_STATUSES_IGNORE);
    #ifdef PROFILE
    tfinal = (MPI_Wtime() - t0);
    MPI_Reduce(&tfinal, &t0, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
    if (rank == 0) printf("Communicate time %e\n", t0);
    #endif
}

template <typename U, typename T>
void communicate3_flip(ParMat<T>& A, std::vector<U>& sendbuf, std::vector<U>& recvbuf, MPI_Datatype type)
{
    int proc;
    T start, end;
    int tag = 2948;

    for (int i = 0; i < A.send_comm.n_msgs; i++)
    {
        proc = A.send_comm.procs[i];
        start = A.send_comm.ptr[i];
        end = A.send_comm.ptr[i+1];
        MPI_Recv_init(&(sendbuf[start]), (int)(end - start), type, proc, tag, 
                MPI_COMM_WORLD, &(A.send_comm.req[i]));
    }
    
    for (int i = 0; i < A.recv_comm.n_msgs; i++)
    {
        proc = A.recv_comm.procs[i];
        start = A.recv_comm.ptr[i];
        end = A.recv_comm.ptr[i+1];
        MPI_Send_init(&(recvbuf[start]), (int)(end - start), type, proc, tag,
                MPI_COMM_WORLD, &(A.recv_comm.req[i]));
    }

    if (A.send_comm.n_msgs)
        MPI_Startall(A.send_comm.n_msgs, A.send_comm.req.data());
    if (A.recv_comm.n_msgs)
    	MPI_Startall(A.recv_comm.n_msgs, A.recv_comm.req.data());
    
    if (A.send_comm.n_msgs)
    {
        MPI_Waitall(A.send_comm.n_msgs, A.send_comm.req.data(), MPI_STATUSES_IGNORE);
	for (int i = 0; i < A.send_comm.n_msgs; i++)
		MPI_Request_free(&(A.send_comm.req[i]));
    }
    if (A.recv_comm.n_msgs)
    {
    	MPI_Waitall(A.recv_comm.n_msgs, A.recv_comm.req.data(), MPI_STATUSES_IGNORE);
	for (int i = 0; i < A.recv_comm.n_msgs; i++)
		MPI_Request_free(&(A.recv_comm.req[i]));
    }
}

template <typename U, typename T>
void communicate3(ParMat<T>& A, std::vector<U>& sendbuf, std::vector<U>& recvbuf, MPI_Datatype type)
{
    int proc;
    T start, end;
    int tag = 2948;

    for (int i = 0; i < A.recv_comm.n_msgs; i++)
    {
        proc = A.recv_comm.procs[i];
        start = A.recv_comm.ptr[i];
        end = A.recv_comm.ptr[i+1];
        MPI_Recv_init(&(recvbuf[start]), (int)(end - start), type, proc, tag,
                MPI_COMM_WORLD, &(A.recv_comm.req[i]));
    }

    for (int i = 0; i < A.send_comm.n_msgs; i++)
    {
        proc = A.send_comm.procs[i];
        start = A.send_comm.ptr[i];
        end = A.send_comm.ptr[i+1];
        MPI_Send_init(&(sendbuf[start]), (int)(end - start), type, proc, tag, 
                MPI_COMM_WORLD, &(A.send_comm.req[i]));
    }

    if (A.recv_comm.n_msgs)
    	MPI_Startall(A.recv_comm.n_msgs, A.recv_comm.req.data());
    
    if (A.send_comm.n_msgs)
        MPI_Startall(A.send_comm.n_msgs, A.send_comm.req.data());
    
    if (A.recv_comm.n_msgs)
    {
    	MPI_Waitall(A.recv_comm.n_msgs, A.recv_comm.req.data(), MPI_STATUSES_IGNORE);
        for (int i = 0; i < A.recv_comm.n_msgs; i++)
            MPI_Request_free(&(A.recv_comm.req[i]));
    }

    if (A.send_comm.n_msgs)
    {
        MPI_Waitall(A.send_comm.n_msgs, A.send_comm.req.data(), MPI_STATUSES_IGNORE);
        for (int i = 0; i < A.send_comm.n_msgs; i++)
            MPI_Request_free(&(A.send_comm.req[i]));
    }

}

template <typename U, typename T>
void communicate2(ParMat<T>& A, std::vector<U>& sendbuf, std::vector<U>& recvbuf, MPI_Datatype type)
{
    int proc;
    T start, end;
    int tag = 2948;
    
    for (int i = 0; i < A.recv_comm.n_msgs; i++)
    {
        proc = A.recv_comm.procs[i];
        start = A.recv_comm.ptr[i];
        end = A.recv_comm.ptr[i+1];
        MPI_Irecv(&(recvbuf[start]), (int)(end - start), type, proc, tag,
                MPI_COMM_WORLD, &(A.recv_comm.req[i]));
    }
    
    for (int i = 0; i < A.send_comm.n_msgs; i++)
    {
        proc = A.send_comm.procs[i];
        start = A.send_comm.ptr[i];
        end = A.send_comm.ptr[i+1];
        MPI_Isend(&(sendbuf[start]), (int)(end - start), type, proc, tag, 
                MPI_COMM_WORLD, &(A.send_comm.req[i]));
    }

    if (A.recv_comm.n_msgs)
    	MPI_Waitall(A.recv_comm.n_msgs, A.recv_comm.req.data(), MPI_STATUSES_IGNORE);
    if (A.send_comm.n_msgs)
        MPI_Waitall(A.send_comm.n_msgs, A.send_comm.req.data(), MPI_STATUSES_IGNORE);
}

template <typename U, typename T>
void communicate(ParMat<T>& A, std::vector<U>& data, std::vector<U>& recvbuf, MPI_Datatype type)
{
    int proc;
    T start, end;
    int tag = 2948;
    
    for (int i = 0; i < A.recv_comm.n_msgs; i++)
    {
        proc = A.recv_comm.procs[i];
        start = A.recv_comm.ptr[i];
        end = A.recv_comm.ptr[i+1];
        MPI_Irecv(&(recvbuf[start]), (int)(end - start), type, proc, tag,
                MPI_COMM_WORLD, &(A.recv_comm.req[i]));
    }
    
    std::vector<U> sendbuf;
    if (A.send_comm.size_msgs)
        sendbuf.resize(A.send_comm.size_msgs);
    for (int i = 0; i < A.send_comm.n_msgs; i++)
    {
        proc = A.send_comm.procs[i];
        start = A.send_comm.ptr[i];
        end = A.send_comm.ptr[i+1];
        for (T j = start; j < end; j++)
        {
            sendbuf[j] = data[A.send_comm.idx[j]];
        }
        MPI_Isend(&(sendbuf[start]), (int)(end - start), type, proc, tag, 
                MPI_COMM_WORLD, &(A.send_comm.req[i]));
    }


    if (A.send_comm.n_msgs)
        MPI_Waitall(A.send_comm.n_msgs, A.send_comm.req.data(), MPI_STATUSES_IGNORE);
    if (A.recv_comm.n_msgs)
    	MPI_Waitall(A.recv_comm.n_msgs, A.recv_comm.req.data(), MPI_STATUSES_IGNORE);
}




#endif