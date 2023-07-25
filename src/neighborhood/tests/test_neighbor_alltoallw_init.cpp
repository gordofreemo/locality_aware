// EXPECT_EQ and ASSERT_EQ are macros
// EXPECT_EQ test execution and continues even if there is a failure
// ASSERT_EQ test execution and aborts if there is a failure
// The ASSERT_* variants abort the program execution if an assertion fails
// while EXPECT_* variants continue with the run.


#include "gtest/gtest.h"
#include "mpi_advance.h"
#include <mpi.h>
#include <math.h>
#include <stdlib.h>
#include <iostream>
#include <assert.h>
#include <vector>
#include <set>

#include "neighbor_data.hpp"


int main(int argc, char** argv)
{
    MPI_Init(&argc, &argv);
    ::testing::InitGoogleTest(&argc, argv);
    int temp=RUN_ALL_TESTS();
    MPI_Finalize();
    return temp;
} // end of main() //


TEST(RandomCommTest, TestsInTests)
{
    // Get MPI Information
    int rank, num_procs;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &num_procs);

    // Initial communication info (standard)
    int local_size = 10000; // Number of variables each rank stores
    MPIX_Data<MPI_Aint> send_data;
    MPIX_Data<MPI_Aint> recv_data;
    form_initial_communicator(local_size, &send_data, &recv_data);
    int int_size = sizeof(int);
    for (int i = 0; i < send_data.num_msgs; i++)
        send_data.indptr[i+1] *= int_size;
    for (int i = 0; i < recv_data.num_msgs; i++)
        recv_data.indptr[i+1] *= int_size;

    // Test correctness of communication
    std::vector<int> std_recv_vals(recv_data.size_msgs);
    std::vector<int> new_recv_vals(recv_data.size_msgs);
    std::vector<int> send_vals(local_size);
    int val = local_size*rank;
    for (int i = 0; i < local_size; i++)
    {
        send_vals[i] = val++;
    }

    std::vector<int> alltoallv_send_vals(send_data.size_msgs);
    for (int i = 0; i < send_data.size_msgs; i++)
        alltoallv_send_vals[i] = send_vals[send_data.indices[i]];

    MPI_Comm std_comm;
    std::vector<MPI_Datatype> sendtypes(num_procs, MPI_INT);
    std::vector<MPI_Datatype> recvtypes(num_procs, MPI_INT);

    MPI_Dist_graph_create_adjacent(MPI_COMM_WORLD,
            recv_data.num_msgs, 
            recv_data.procs.data(), 
            recv_data.counts.data(),
            send_data.num_msgs, 
            send_data.procs.data(),
            send_data.counts.data(),
            MPI_INFO_NULL, 
            0, 
            &std_comm);

    MPI_Neighbor_alltoallw(alltoallv_send_vals.data(), 
            send_data.counts.data(),
            send_data.indptr.data(), 
            sendtypes.data(),
            std_recv_vals.data(), 
            recv_data.counts.data(),
            recv_data.indptr.data(), 
            recvtypes.data(),
            std_comm);

/*

    // 2. Node-Aware Communication
    MPIX_Dist_graph_create_adjacent(MPI_COMM_WORLD,
            recv_data.num_msgs, 
            recv_data.procs.data(), 
            recv_data.counts.data(),
            send_data.num_msgs, 
            send_data.procs.data(),
            send_data.counts.data(),
            MPI_INFO_NULL, 
            0, 
            &neighbor_comm);
    update_locality(neighbor_comm, 4);
    MPIX_Neighbor_alltoallw_init(alltoallv_send_vals.data(), 
            send_data.counts.data(),
            send_data.indptr.data(), 
            sendtypes.data(),
            new_recv_vals.data(), 
            recv_data.counts.data(),
            recv_data.indptr.data(), 
            recvtypes.data(),
            neighbor_comm, 
            MPI_INFO_NULL,
            &neighbor_request);

    MPIX_Start(neighbor_request);
    MPIX_Wait(neighbor_request, &status);

    // 3. Compare std_recv_vals and nap_recv_vals
    for (int i = 0; i < recv_data.size_msgs; i++)
    {
        ASSERT_EQ(std_recv_vals[i], new_recv_vals[i]);
    }

    MPIX_Request_free(neighbor_request);
    MPIX_Comm_free(neighbor_comm);
    MPI_Comm_free(&std_comm);

*/
}

