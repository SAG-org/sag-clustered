#ifndef AFFINITY_HPP
#define AFFINITY_HPP

#include "jobs.hpp"

namespace NP {

	class InvalidAffinity : public std::exception
	{
	public:

		InvalidAffinity(const JobID& bad_id)
			: ref(bad_id)
		{}

		const JobID ref;

		virtual const char* what() const noexcept override
		{
			return "invalid affinity";
		}

	};

	template<class Time>
	void validate_affinities(const typename Job<Time>::Job_set jobs,
		unsigned int num_clusters)
	{
		for (const Job<Time>& j : jobs) {
			if (j.get_affinity() >= num_clusters || j.get_affinity() < 0)
				throw InvalidAffinity(j.get_id());
		}
	}

}

#endif