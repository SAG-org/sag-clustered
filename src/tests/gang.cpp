#include "doctest.h"

#include <iostream>
#include <sstream>

#include "io.hpp"
#include "global/state.hpp"
#include "global/space.hpp"

using namespace NP;

TEST_CASE("[gang] uniproc vs gang") {
	Global::State_space<dtime_t>::Workload jobs{
			Job<dtime_t>{1, Interval<dtime_t>(0, 0), Interval<dtime_t>(1, 2), 10, 10, 0, 0},
			Job<dtime_t>{2, Interval<dtime_t>(10, 10), Interval<dtime_t>(1, 2), 20, 20, 1, 1},
			Job<dtime_t>{3, Interval<dtime_t>(20, 20), Interval<dtime_t>(1, 2), 30, 30, 2, 2},
			Job<dtime_t>{4, Interval<dtime_t>(30, 30), Interval<dtime_t>(1, 2), 40, 40, 3, 3},
			Job<dtime_t>{5, Interval<dtime_t>(40, 40), Interval<dtime_t>(1, 2), 50, 50, 4, 4},
			Job<dtime_t>{6, Interval<dtime_t>(50, 50), Interval<dtime_t>(1, 2), 60, 60, 5, 5},
			Job<dtime_t>{7, Interval<dtime_t>(0, 0), Interval<dtime_t>(7, 8), 30, 30, 6, 6},
			Job<dtime_t>{8, Interval<dtime_t>(30, 30), Interval<dtime_t>(7, 8), 60, 60, 7, 7},
	};

	Global::State_space<dtime_t>::Workload jobs_gang{
			Job<dtime_t>{1, Interval<dtime_t>(0, 0),
						 std::map<unsigned int, Interval<dtime_t>>{{2, Interval<dtime_t>(1, 2)}}, 10, 10, 0, 0},
			Job<dtime_t>{2, Interval<dtime_t>(10, 10),
						 std::map<unsigned int, Interval<dtime_t>>{{2, Interval<dtime_t>(1, 2)}}, 20, 20, 1, 1},
			Job<dtime_t>{3, Interval<dtime_t>(20, 20),
						 std::map<unsigned int, Interval<dtime_t>>{{2, Interval<dtime_t>(1, 2)}}, 30, 30, 2, 2},
			Job<dtime_t>{4, Interval<dtime_t>(30, 30),
						 std::map<unsigned int, Interval<dtime_t>>{{2, Interval<dtime_t>(1, 2)}}, 40, 40, 3, 3},
			Job<dtime_t>{5, Interval<dtime_t>(40, 40),
						 std::map<unsigned int, Interval<dtime_t>>{{2, Interval<dtime_t>(1, 2)}}, 50, 50, 4, 4},
			Job<dtime_t>{6, Interval<dtime_t>(50, 50),
						 std::map<unsigned int, Interval<dtime_t>>{{2, Interval<dtime_t>(1, 2)}}, 60, 60, 5, 5},
			Job<dtime_t>{7, Interval<dtime_t>(0, 0),
						 std::map<unsigned int, Interval<dtime_t>>{{2, Interval<dtime_t>(7, 8)}}, 30, 30, 6, 6},
			Job<dtime_t>{8, Interval<dtime_t>(30, 30),
						 std::map<unsigned int, Interval<dtime_t>>{{2, Interval<dtime_t>(7, 8)}}, 60, 60, 7, 7},
	};

	// compare the two
	auto space = Global::State_space<dtime_t>::explore(jobs);
	auto space_gang = Global::State_space<dtime_t>::explore(jobs_gang, 2);

	CHECK(space->is_schedulable());
	CHECK(space_gang->is_schedulable());

	// compare the response times
	for (int i = 0; i < jobs.size(); ++i) {
		auto ftimes = space->get_finish_times(jobs[i]);
		auto ftimes_gang = space_gang->get_finish_times(jobs_gang[i]);
		CHECK(ftimes.min() == ftimes_gang.min());
		CHECK(ftimes.max() == ftimes_gang.max());
	}

}

TEST_CASE("[gang] global vs gang") {
	Global::State_space<dtime_t>::Workload jobs{
			Job<dtime_t>{1, Interval<dtime_t>(0, 0), Interval<dtime_t>(1, 2), 10, 10, 0, 0},
			Job<dtime_t>{2, Interval<dtime_t>(10, 10), Interval<dtime_t>(1, 2), 20, 20, 1, 1},
			Job<dtime_t>{3, Interval<dtime_t>(20, 20), Interval<dtime_t>(1, 2), 30, 30, 2, 2},
			Job<dtime_t>{4, Interval<dtime_t>(30, 30), Interval<dtime_t>(1, 2), 40, 40, 3, 3},
			Job<dtime_t>{5, Interval<dtime_t>(40, 40), Interval<dtime_t>(1, 2), 50, 50, 4, 4},
			Job<dtime_t>{6, Interval<dtime_t>(50, 50), Interval<dtime_t>(1, 2), 60, 60, 5, 5},
			Job<dtime_t>{7, Interval<dtime_t>(0, 0), Interval<dtime_t>(7, 8), 30, 30, 6, 6},
			Job<dtime_t>{8, Interval<dtime_t>(30, 30), Interval<dtime_t>(7, 8), 60, 60, 7, 7},
			Job<dtime_t>{9, Interval<dtime_t>(0, 0), Interval<dtime_t>(3, 13), 60, 60, 8, 8}
	};

	Global::State_space<dtime_t>::Workload jobs_gang{
			Job<dtime_t>{1, Interval<dtime_t>(0, 0),
						 std::map<unsigned int, Interval<dtime_t>>{{2, Interval<dtime_t>(1, 2)}}, 10, 10, 0, 0},
			Job<dtime_t>{2, Interval<dtime_t>(10, 10),
						 std::map<unsigned int, Interval<dtime_t>>{{2, Interval<dtime_t>(1, 2)}}, 20, 20, 1, 1},
			Job<dtime_t>{3, Interval<dtime_t>(20, 20),
						 std::map<unsigned int, Interval<dtime_t>>{{2, Interval<dtime_t>(1, 2)}}, 30, 30, 2, 2},
			Job<dtime_t>{4, Interval<dtime_t>(30, 30),
						 std::map<unsigned int, Interval<dtime_t>>{{2, Interval<dtime_t>(1, 2)}}, 40, 40, 3, 3},
			Job<dtime_t>{5, Interval<dtime_t>(40, 40),
						 std::map<unsigned int, Interval<dtime_t>>{{2, Interval<dtime_t>(1, 2)}}, 50, 50, 4, 4},
			Job<dtime_t>{6, Interval<dtime_t>(50, 50),
						 std::map<unsigned int, Interval<dtime_t>>{{2, Interval<dtime_t>(1, 2)}}, 60, 60, 5, 5},
			Job<dtime_t>{7, Interval<dtime_t>(0, 0),
						 std::map<unsigned int, Interval<dtime_t>>{{2, Interval<dtime_t>(7, 8)}}, 30, 30, 6, 6},
			Job<dtime_t>{8, Interval<dtime_t>(30, 30),
						 std::map<unsigned int, Interval<dtime_t>>{{2, Interval<dtime_t>(7, 8)}}, 60, 60, 7, 7},
			Job<dtime_t>{9, Interval<dtime_t>(0, 0),
						 std::map<unsigned int, Interval<dtime_t>>{{2, Interval<dtime_t>(3, 13)}}, 60, 60, 8, 8}

	};

	// compare the two
	auto space = Global::State_space<dtime_t>::explore(jobs, 2);
	auto space_gang = Global::State_space<dtime_t>::explore(jobs_gang, 4);

	CHECK(space->is_schedulable());
	CHECK(space_gang->is_schedulable());

	// compare the response times
	for (int i = 0; i < jobs.size(); ++i) {
		auto ftimes = space->get_finish_times(jobs[i]);
		auto ftimes_gang = space_gang->get_finish_times(jobs_gang[i]);
		CHECK(ftimes.min() == ftimes_gang.min());
		CHECK(ftimes.max() == ftimes_gang.max());
	}
}