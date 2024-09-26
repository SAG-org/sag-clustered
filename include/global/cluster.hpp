#ifndef GLOBAL_CLUSTER_H
#define GLOBAL_CLUSTER_H

#include <algorithm>
#include <cassert>
#include <iostream>
#include <ostream>
#include <vector>
#include <deque>

#include "config.h"
#include "cache.hpp"
#include "jobs.hpp"
#include "util.hpp"
#include "interval.hpp"

namespace NP {

	namespace Global {

		typedef std::vector<Job_index> Job_precedence_set;

		template<class Time> class Cluster_state
		{
		private:

			typedef std::vector<Interval<Time>> CoreAvailability;
			typedef std::vector<std::pair<const Job<Time>*, Interval<Time>>> Susp_list;
			typedef std::vector<Susp_list> Successors;
			typedef std::vector<Susp_list> Predecessors;
			typedef Interval<unsigned int> Parallelism;

			// system availability intervals
			CoreAvailability core_avail;

			// keeps track of the earliest time a job with at least one predecessor is certainly ready and certainly has enough free cores to start executing
			Time earliest_certain_successor_job_disptach;

			// keeps track of the earliest time a gang source job (a job with no predecessor that requires more than one core to execute) 
			// is certainly arrived and certainly has enough free cores to start executing
			Time earliest_certain_gang_source_job_disptach;

			struct Running_job {
				Job_index idx;
				Parallelism parallelism;
				Interval<Time> finish_time;

				Running_job(
					Job_index idx,
					Parallelism parallelism,
					Interval<Time> finish_time
				)
					: idx(idx),
					parallelism(parallelism),
					finish_time(finish_time)
				{}
			};

			// imprecise set of certainly running jobs, on how many cores they run, and when they should finish
			std::vector<Running_job> certain_jobs;

		public:

			// initial state -- nothing yet has finished, nothing is running
			Cluster_state(const unsigned int num_processors, const Time next_certain_gang_source_job_disptach)
				: core_avail{ num_processors, Interval<Time>(Time(0), Time(0)) }
				, certain_jobs{}
				, earliest_certain_successor_job_disptach{ Time_model::constants<Time>::infinity() }
				, earliest_certain_gang_source_job_disptach{ next_certain_gang_source_job_disptach }
			{
				assert(core_avail.size() > 0);
			}

			// transition: new state by scheduling a job 'j' on 'ncores' cores in an existing state 'from'
			Cluster_state(
				const Cluster_state& from,
				Job_index j,
				const Job_precedence_set& predecessors,
				Interval<Time> start_times,
				Interval<Time> finish_times,
				const Time next_certain_gang_source_job_disptach,
				unsigned int ncores = 1)
				: earliest_certain_gang_source_job_disptach(next_certain_gang_source_job_disptach)
				, earliest_certain_successor_job_disptach(Time_model::constants<Time>::infinity())
			{
				// update the set of certainly running jobs
				update_certainly_running_jobs(from, j, start_times, finish_times, ncores, predecessors);

				// calculate the cores availability intervals resulting from dispatching job j on ncores in state 'from'
				update_core_avail(from, j, predecessors, start_times, finish_times, ncores);

				assert(core_avail.size() > 0);
			}

			// copy constructor
			Cluster_state(const Cluster_state& origin)
				: certain_jobs(origin.certain_jobs)
				, core_avail(origin.core_avail)
				, earliest_certain_gang_source_job_disptach(origin.earliest_certain_gang_source_job_disptach)
				, earliest_certain_successor_job_disptach(origin.earliest_certain_successor_job_disptach)
			{
			}

			inline unsigned int num_cpus() const
			{
				return core_avail.size();
			}

			// returns the interval of time whitin which 'p' cores will become available
			Interval<Time> core_availability(unsigned long p = 1) const
			{
				assert(core_avail.size() > 0);
				assert(core_avail.size() >= p);
				assert(p > 0);
				return core_avail[p - 1];
			}

			// returns earliest time a cores may become available
			Time earliest_finish_time() const
			{
				return core_avail[0].min();
			}

			Time next_certain_gang_source_job_disptach() const
			{
				return earliest_certain_gang_source_job_disptach;
			}

			Time next_certain_successor_jobs_disptach() const
			{
				return earliest_certain_successor_job_disptach;
			}

			void set_earliest_certain_successor_job_disptach(Time next_certain_successor_job_disptach)
			{
				earliest_certain_successor_job_disptach = next_certain_successor_job_disptach;
			}

			bool core_avail_overlap(const CoreAvailability& other) const
			{
				assert(core_avail.size() == other.size());
				for (int i = 0; i < core_avail.size(); i++)
					if (!core_avail[i].intersects(other[i]))
						return false;
				return true;
			}

			// check if 'other' state can merge with this state
			bool can_merge_with(const Cluster_state<Time>& other) const
			{
				return core_avail_overlap(other.core_avail);
			}

			bool can_merge_with(const CoreAvailability& cav) const
			{
				return core_avail_overlap(cav);
			}

			// first check if 'other' state can merge with this state, then, if yes, merge 'other' with this state.
			bool try_to_merge(const Cluster_state<Time>& other)
			{
				if (!can_merge_with(other))
					return false;

				merge(other.core_avail, other.certain_jobs, other.earliest_certain_successor_job_disptach);

				DM("+++ merged " << other << " into " << *this << std::endl);
				return true;
			}

			void merge(const Cluster_state<Time>& other)
			{
				merge(other.core_avail, other.certain_jobs, other.earliest_certain_successor_job_disptach);
			}

			void merge(
				const CoreAvailability& cav,
				const std::vector<Running_job>& cert_j,
				Time ecsj_ready_time)
			{
				for (int i = 0; i < core_avail.size(); i++)
					core_avail[i] |= cav[i];

				// vector to collect joint certain jobs
				std::vector<Running_job> new_cj;

				// walk both sorted job lists to see if we find matches
				auto it = certain_jobs.begin();
				auto it_end = certain_jobs.end();
				auto jt = cert_j.begin();
				auto jt_end = cert_j.end();
				while (it != it_end &&
					jt != jt_end) {
					if (it->idx == jt->idx) {
						// same job
						new_cj.emplace_back(it->idx, it->parallelism | jt->parallelism, it->finish_time | jt->finish_time);
						it++;
						jt++;
					}
					else if (it->idx < jt->idx)
						it++;
					else
						jt++;
				}
				// move new certain jobs into the state
				certain_jobs.swap(new_cj);

				// update certain ready time of jobs with predecessors
				earliest_certain_successor_job_disptach = std::max(earliest_certain_successor_job_disptach, ecsj_ready_time);

				DM("+++ merged (cav,jft,cert_t) into " << *this << std::endl);
			}

			friend std::ostream& operator<< (std::ostream& stream,
				const Cluster_state<Time>& s)
			{
				stream << "Global::State(";
				for (const auto& a : s.core_avail)
					stream << "[" << a.from() << ", " << a.until() << "] ";
				stream << "(";
				for (const auto& rj : s.certain_jobs)
					stream << rj.idx << "";
				stream << ") " << ")";
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
					out << "T" << jobs[rj.idx].get_task_id()
						<< "J" << jobs[rj.idx].get_job_id() << ":"
						<< rj.finish_time.min() << "-" << rj.finish_time.max();
					first = false;
				}
				out << "}";
			}

		private:
			// update the list of jobs that are certainly running in the current system state
			void update_certainly_running_jobs(const Cluster_state& from,
				Job_index j, Interval<Time> start_times,
				Interval<Time> finish_times, unsigned int ncores,
				const Job_precedence_set& predecessors)
			{
				certain_jobs.reserve(from.certain_jobs.size() + 1);

				Time lst = start_times.max();
				int n_prec = 0;

				// update the set of certainly running jobs
				// keep them sorted to simplify merging
				bool added_j = false;
				for (const auto& rj : from.certain_jobs)
				{
					auto running_job = rj.idx;
					if (contains(predecessors, running_job))
					{
						n_prec += rj.parallelism.min(); // keep track of the number of predecessors of j that are certainly running
					}
					else if (lst <= rj.finish_time.min())
					{
						if (!added_j && running_job > j)
						{
							// right place to add j
							Parallelism p(ncores, ncores);
							certain_jobs.emplace_back(j, p, finish_times);
							added_j = true;
						}
						certain_jobs.emplace_back(rj);
					}
				}
				// if we didn't add it yet, add it at the back
				if (!added_j)
				{
					Parallelism p(ncores, ncores);
					certain_jobs.emplace_back(j, p, finish_times);
				}
			}

			// update the core availability resulting from scheduling job j on m cores in state 'from'
			void update_core_avail(const Cluster_state& from, const Job_index j, const Job_precedence_set& predecessors,
				const Interval<Time> start_times, const Interval<Time> finish_times, const unsigned int m)
			{
				int n_cores = from.core_avail.size();
				core_avail.reserve(n_cores);

				auto est = start_times.min();
				auto lst = start_times.max();
				auto eft = finish_times.min();
				auto lft = finish_times.max();

				int n_prec = 0;

				// Compute the number of cores certainly used by active predecessors.
				// certain_jobs is sorted. If predecessors is sorted, some improvement is possible.
				for (const auto& rj : certain_jobs) {
					if (contains(predecessors, rj.idx)) {
						n_prec += rj.parallelism.min();
					}
				}

				// compute the cores availability intervals
				Time* ca = new Time[n_cores];
				Time* pa = new Time[n_cores];
				unsigned int ca_idx = 0, pa_idx = 0;

				// Keep pa and ca sorted, by adding the value at the correct place.
				bool eft_added_to_pa = false;
				bool lft_added_to_ca = false;

				// note, we must skip the first ncores elements in from.core_avail
				if (n_prec > m) {
					// if there are n_prec predecessors running, n_prec cores must be available when j starts
					for (int i = m; i < n_prec; i++) {
						pa[pa_idx] = est; pa_idx++; //pa.push_back(est); // TODO: GN: check whether we can replace by est all the time since predecessors must possibly be finished by est to let j start
						ca[ca_idx] = std::min(lst, std::max(est, from.core_avail[i].max())); ca_idx++; //ca.push_back(std::min(lst, std::max(est, from.core_avail[i].max())));
					}
				}
				else {
					n_prec = m;
				}
				for (int i = n_prec; i < from.core_avail.size(); i++) {
					if (!eft_added_to_pa && eft < from.core_avail[i].min())
					{
						// add the finish time of j ncores times since it runs on ncores
						for (int p = 0; p < m; p++) {
							pa[pa_idx] = eft; pa_idx++; //pa.push_back(eft);
						}
						eft_added_to_pa = true;
					}
					pa[pa_idx] = std::max(est, from.core_avail[i].min()); pa_idx++; //pa.push_back(std::max(est, from.core_avail[i].min()));
					if (!lft_added_to_ca && lft < from.core_avail[i].max()) {
						// add the finish time of j ncores times since it runs on ncores
						for (int p = 0; p < m; p++) {
							ca[ca_idx] = lft; ca_idx++; //ca.push_back(lft);
						}
						lft_added_to_ca = true;
					}
					ca[ca_idx] = std::max(est, from.core_avail[i].max()); ca_idx++; //ca.push_back(std::max(est, from.core_avail[i].max()));
				}
				if (!eft_added_to_pa) {
					// add the finish time of j ncores times since it runs on ncores
					for (int p = 0; p < m; p++) {
						pa[pa_idx] = eft; pa_idx++; //pa.push_back(eft);
					}
				}
				if (!lft_added_to_ca) {
					// add the finish time of j ncores times since it runs on ncores
					for (int p = 0; p < m; p++) {
						ca[ca_idx] = lft; ca_idx++; //ca.push_back(lft);
					}
				}

				for (int i = 0; i < from.core_avail.size(); i++)
				{
					DM(i << " -> " << pa[i] << ":" << ca[i] << std::endl);
					core_avail.emplace_back(pa[i], ca[i]);
				}
				delete[] pa;
				delete[] ca;
			}
		};
	}
}

#endif