#include <iostream>
#include <ostream>
#include <cassert>
#include <algorithm>

#include <set>

#include "util.hpp"
#include "index_set.hpp"
#include "jobs.hpp"
#include "cache.hpp"

namespace NP {

	namespace Global {

	  // RV: Job_index is defined in jobs.hpp in the NP namespace.
	  // typedef std::size_t Job_index;
		typedef std::vector<Job_index> Job_precedence_set;

		template<class Time> class Schedule_node;

		template<class Time> class Schedule_state
		{
			private:

		  // RV: would another data structure improve access to job_finish_times?
			typedef typename std::unordered_map<Job_index, Interval<Time>> JobFinishTimes;
			JobFinishTimes job_finish_times;

			public:

			// initial state -- nothing yet has finished, nothing is running
			Schedule_state(unsigned int num_processors)
			: scheduled_jobs()
			, num_jobs_scheduled(0)
			, core_avail{num_processors, Interval<Time>(Time(0), Time(0))}
			, lookup_key{0x9a9a9a9a9a9a9a9aUL}
			{
				assert(core_avail.size() > 0);
			}

			// transition: new state by scheduling a job in an existing state,
			//             by replacing a given running job.
			Schedule_state(
				const Schedule_state& from,
				Job_index j,
				const Job_precedence_set& predecessors,
				Interval<Time> start_times,
				Interval<Time> finish_times,
				hash_value_t key)
			: num_jobs_scheduled(from.num_jobs_scheduled + 1)
			, scheduled_jobs{from.scheduled_jobs, j}
			, lookup_key{from.lookup_key ^ key}
			, job_finish_times{from.job_finish_times}
			{
			  // RV: check what changed in this function.
				auto est = start_times.min();
				auto lst = start_times.max();
				auto eft = finish_times.min();
				auto lft = finish_times.max();

				DM("est: " << est << std::endl
				<< "lst: " << lst << std::endl
				<< "eft: " << eft << std::endl
				<< "lft: " << lft << std::endl);

				std::vector<Time> ca, pa;

				pa.push_back(eft);
				ca.push_back(lft);

				// skip first element in from.core_avail
				for (int i = 1; i < from.core_avail.size(); i++) {
					pa.push_back(std::max(est, from.core_avail[i].min()));
					ca.push_back(std::max(est, from.core_avail[i].max()));
				}

				// update scheduled jobs
				// keep it sorted to make it easier to merge
				bool added_j = false;
				for (const auto& rj : from.certain_jobs) {
					auto x = rj.first;
					auto x_eft = rj.second.min();
					auto x_lft = rj.second.max();
					if (contains(predecessors, x)) {
						if (lst < x_lft) {
							auto pos = std::find(ca.begin(), ca.end(), x_lft);
							if (pos != ca.end())
								*pos = lst;
						}
					} else if (lst <= x_eft) {
						if (!added_j && rj.first > j) {
							// right place to add j
							certain_jobs.emplace_back(j, finish_times);
							added_j = true;
						}
						certain_jobs.emplace_back(rj);
					}
				}
				// if we didn't add it yet, add it at the back
				if (!added_j)
					certain_jobs.emplace_back(j, finish_times);


				// sort in non-decreasing order
				std::sort(pa.begin(), pa.end());
				std::sort(ca.begin(), ca.end());

				for (int i = 0; i < from.core_avail.size(); i++) {
					DM(i << " -> " << pa[i] << ":" << ca[i] << std::endl);
					core_avail.emplace_back(pa[i], ca[i]);
				}

				job_finish_times.emplace(j, finish_times);

				assert(core_avail.size() > 0);
				DM("*** new state: constructed " << *this << std::endl);
			}

			hash_value_t get_key() const
			{
				return lookup_key;
			}

			bool same_jobs_scheduled(const Schedule_state &other) const
			{
				return scheduled_jobs == other.scheduled_jobs;
			}

			bool can_merge_with(const Schedule_state<Time>& other) const
			{
				assert(core_avail.size() == other.core_avail.size());

				if (get_key() != other.get_key())
					return false;
				if (!same_jobs_scheduled(other))
					return false;
				for (int i = 0; i < core_avail.size(); i++)
					if (!core_avail[i].intersects(other.core_avail[i]))
						return false;
				//check if JobFinishTimes overlap
				for (const auto& rj : job_finish_times) {
					auto it = other.job_finish_times.find(rj.first);
					if (it != other.job_finish_times.end()) {
						if (!rj.second.intersects(it->second))
							return false;
					}
				}

				return true;
			}

			bool try_to_merge(const Schedule_state<Time>& other)
			{
				if (!can_merge_with(other))
					return false;

				for (int i = 0; i < core_avail.size(); i++)
					core_avail[i] |= other.core_avail[i];

				// vector to collect joint certain jobs
				std::vector<std::pair<Job_index, Interval<Time>>> new_cj;

				// walk both sorted job lists to see if we find matches
				auto it = certain_jobs.begin();
				auto jt = other.certain_jobs.begin();
				while (it != certain_jobs.end() &&
				       jt != other.certain_jobs.end()) {
					if (it->first == jt->first) {
						// same job
						new_cj.emplace_back(it->first, it->second | jt->second);
						it++;
						jt++;
					} else if (it->first < jt->first)
						it++;
					else
						jt++;
				}
				// move new certain jobs into the state
				certain_jobs.swap(new_cj);

				// merge job_finish_times
				for (const auto& rj : other.job_finish_times) {
					auto it = job_finish_times.find(rj.first);
					if (it != job_finish_times.end()) {
						it->second.widen(rj.second);
					}
				}

				DM("+++ merged " << other << " into " << *this << std::endl);

				return true;
			}

			const unsigned int number_of_scheduled_jobs() const
			{
				return num_jobs_scheduled;
			}

			Interval<Time> core_availability() const
			{
				assert(core_avail.size() > 0);
				return core_avail[0];
			}

			bool get_finish_times(Job_index j, Interval<Time> &ftimes) const
			{
				for (const auto& rj : certain_jobs) {
					// check index
					if (j == rj.first) {
						ftimes = rj.second;
						return true;
					}
					// Certain_jobs is sorted in order of increasing job index.
					// If we see something larger than 'j' we are not going
					// to find it. For large processor counts, it might make
					// sense to do a binary search instead.
					if (j < rj.first)
						return false;
				}
				return false;
			}

			const bool job_incomplete(Job_index j) const
			{
				return !scheduled_jobs.contains(j);
			}

			const bool job_ready(const Job_precedence_set& predecessors) const
			{
				for (auto j : predecessors)
					if (!scheduled_jobs.contains(j))
						return false;
				return true;
			}

			friend std::ostream& operator<< (std::ostream& stream,
			                                 const Schedule_state<Time>& s)
			{
				stream << "Global::State(";
				for (const auto& a : s.core_avail)
					stream << "[" << a.from() << ", " << a.until() << "] ";
				stream << "(";
				for (const auto& rj : s.certain_jobs)
					stream << rj.first << "";
				stream << ") " << s.scheduled_jobs << ")";
				stream << " @ " << &s;
				return stream;
			}

			void print_vertex_label(std::ostream& out,
				const typename Job<Time>::Job_set& jobs) const
			{
				for (const auto& a : core_avail)
					out << "[" << a.from() << ", " << a.until() << "] ";
				out << "\\n";
				bool first = true;
				out << "{";
				for (const auto& rj : certain_jobs) {
					if (!first)
						out << ", ";
					out << "T" << jobs[rj.first].get_task_id()
					    << "J" << jobs[rj.first].get_job_id() << ":"
					    << rj.second.min() << "-" << rj.second.max();
					first = false;
				}
				out << "}";
			}

			// Functions for pathwise self-suspending exploration

		        // RV:  note that job_finish_times is an unordered_map<Job_index, Interval<Time>>.
		        //      similar to certain_jobs, it could use a vector<std::pair<Job_index,Interval<Time>>> .
		        //      Check the impact on memory and computation time.
		        //      Used operations:  find(), erase(), end().
			void del_pred(const Job_index pred_job)
			{
				job_finish_times.erase(pred_job);
			}

		  // RV:  job_finish_times has Job_index, not JobID. Unclear how this works.
			void widen_pathwise_job(const JobID pred_job, const Interval<Time> ft)
			{
				auto it = job_finish_times.find(pred_job);
				if (it != job_finish_times.end()) {
					(it->second).widen(ft);
				}
			}

		  // RV: Job_index instead of JobID?
			bool pathwisejob_exists(const JobID pred_job) const
			{
				auto it = job_finish_times.find(pred_job);
				if (it != job_finish_times.end()) {
					return true;
				}
				return false;
			}

			const Interval<Time>& get_pathwisejob_ft(const Job_index pathwise_job) const
			{
				return job_finish_times.find(pathwise_job)->second;
			}

		  // RV: Job_index instead of JobID?
		  //     This function would return the provided parameter, assuming it is found.
			const JobID& get_pathwisejob_job(const JobID& pathwise_job) const
			{
				return job_finish_times.find(pathwise_job)->first;
			}

			const JobFinishTimes& get_pathwise_jobs() const
			{
				return job_finish_times;
			}

			// End of functions for pathwise self-suspending exploration

			private:

			const unsigned int num_jobs_scheduled;

			// set of jobs that have been dispatched (may still be running)
			const Index_set scheduled_jobs;

			// imprecise set of certainly running jobs
			std::vector<std::pair<Job_index, Interval<Time>>> certain_jobs;

			// system availability intervals
			std::vector<Interval<Time>> core_avail;

			const hash_value_t lookup_key;

			// no accidental copies
			Schedule_state(const Schedule_state& origin)  = delete;
		};

	}
}
