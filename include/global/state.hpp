#ifndef GLOBAL_STATE_HPP
#define GLOBAL_STATE_HPP
#include <algorithm>
#include <cassert>
#include <iostream>
#include <ostream>

#include <vector>
#include <deque>

#include "config.h"
#include "cache.hpp"
#include "index_set.hpp"
#include "jobs.hpp"
#include "statistics.hpp"
#include "util.hpp"

namespace NP {

	namespace Global {

		typedef Index_set Job_set;
		typedef std::vector<Job_index> Job_precedence_set;

		template<class Time> class Schedule_node;

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

		template<class Time> class Schedule_state
		{
			typedef std::vector<std::pair<Job_index, Interval<Time>>> JobFinishTimes;
			typedef std::vector<std::pair<const Job<Time>*, Interval<Time>>> Susp_list;
			typedef std::vector<Susp_list> Successors;
			typedef std::vector<Susp_list> Predecessors;

		private:
			std::vector<Cluster_state<Time>> clusters;
			// job_finish_times holds the finish times of all the jobs that still have an unscheduled successor
			JobFinishTimes job_finish_times;

		public:
			// initial state -- nothing yet has finished, nothing is running
			Schedule_state(const std::vector<unsigned int>& num_cpus, const std::vector<Time>& next_certain_gang_job_release)
			{
				assert(num_cpus.size() > 0);
				clusters.reserve(num_cpus.size());
				for (int i=0; i < num_cpus.size(); i++)
				{
					clusters.emplace_back(num_cpus[i], next_certain_gang_job_release[i]);
				}
			}

			// transition: new state by scheduling a job 'j' on 'ncores' cores in an existing state 'from'
			Schedule_state(
				const Schedule_state& from,
				const Job<Time>& j,
				const Job_precedence_set& predecessors,
				Interval<Time> start_times,
				Interval<Time> finish_times,
				const Job_set& scheduled_jobs,
				const Successors& successors_of,
				const Predecessors& predecessors_of,
				const Time next_certain_gang_source_job_disptach,
				unsigned int ncores = 1)
			{
				clusters.reserve(from.clusters.size());
				for (int i = 0; i < from.clusters.size(); i++) {
					if (i == j.get_affinity())
						clusters.emplace_back(from.cluster(i), j.get_job_index(), predecessors, start_times, finish_times, next_certain_gang_source_job_disptach, ncores);
					else
						clusters.emplace_back(from.cluster(i));
				}

				// save the job finish time of every job with a successor that is not executed yet in the current state
				update_job_finish_times(from, j, start_times, finish_times, successors_of, predecessors_of, scheduled_jobs);
			}

			const Cluster_state<Time>& cluster(unsigned int cluster_id) const
			{
				assert(cluster_id < clusters.size());
				return clusters[cluster_id];
			}

			// writes the finish time interval of job 'j' in 'ftimes' if the finish time of 'j' is known. Returns false if the finish time of 'j' is not known.
			bool get_finish_times(Job_index j, Interval<Time>& ftimes) const
			{
				int offset = jft_find(j);
				if (offset < job_finish_times.size() && job_finish_times[offset].first == j)
				{
					ftimes = job_finish_times[offset].second;
					return true;
				}
				else {
					ftimes = Interval<Time>{ 0, Time_model::constants<Time>::infinity() };
					return false;
				}
			}

			// check if 'other' state can merge with this state
			bool can_merge_with(const Schedule_state<Time>& other, bool useJobFinishTimes = false) const
			{
				for (int i = 0; i < clusters.size(); i++) {
					if (!clusters[i].can_merge_with(other.clusters[i]))
						return false;
				}
				if (useJobFinishTimes)
					return check_finish_times_overlap(other.job_finish_times);
				else
					return true;
			}

			// first check if 'other' state can merge with this state, then, if yes, merge 'other' with this state.
			bool try_to_merge(const Schedule_state<Time>& other, bool useJobFinishTimes = false)
			{
				if (!can_merge_with(other, useJobFinishTimes))
					return false;

				for (int i = 0; i < clusters.size(); i++)
					clusters[i].merge(other.clusters[i]);

				// merge job_finish_times
				widen_finish_times(other.job_finish_times);

				DM("+++ merged " << other << " into " << *this << std::endl);
				return true;
			}

		private:
			// update the list of finish times of jobs with successors w.r.t. the previous system state
			// and calculate the earliest time a job with precedence constraints will become ready to dispatch
			void update_job_finish_times(const Schedule_state& from,
				const Job<Time>& j, Interval<Time> start_times,
				Interval<Time> finish_times,
				const Successors& successors_of,
				const Predecessors& predecessors_of,
				const Job_set& scheduled_jobs)
			{
				unsigned int affinity = j.get_affinity();
				Job_index j_idx = j.get_job_index();
				Time lst = start_times.max();
				Time lft = finish_times.max();

				job_finish_times.reserve(from.job_finish_times.size() + 1);
				bool added_j = false;
				std::vector<Time> earliest_certain_successor_job_disptach (clusters.size(), Time_model::constants<Time>::infinity());
				for (const auto& ft : from.job_finish_times)
				{
					auto job = ft.first;
					auto job_eft = ft.second.min();
					auto job_lft = ft.second.max();
					// if there is a single core, then we know that 
					// jobs that were disptached in the past cannot have 
					// finished later than when our new job starts executing
					if (clusters[affinity].num_cpus() == 1)
					{
						if (job_lft > lst)
							job_lft = lst;
					}

					if (!added_j && job > j_idx)
					{
						bool successor_pending = false;
						for (const auto& succ : successors_of[j_idx])
						{
							successor_pending = true;
							Time avail = clusters[affinity].core_availability(succ.first->get_min_parallelism()).max();
							Time ready_time = std::max(avail, succ.first->latest_arrival());
							bool ready = true;
							for (const auto& pred : predecessors_of[succ.first->get_job_index()])
							{
								Interval<Time> ftimes(0, 0);
								auto from_job = pred.first->get_job_index();
								if (from_job == j_idx)
								{
									Time susp_max = pred.second.max();
									ready_time = std::max(ready_time, lft + susp_max);
								}
								else if (scheduled_jobs.contains(from_job) && from.get_finish_times(from_job, ftimes))
								{
									Time susp_max = pred.second.max();
									ready_time = std::max(ready_time, ftimes.max() + susp_max);
								}
								else
								{
									ready = false;
									break;
								}
							}
							if (ready)
							{
								earliest_certain_successor_job_disptach[succ.first->get_affinity()] =
									std::min(earliest_certain_successor_job_disptach[succ.first->get_affinity()], ready_time);
							}
						}
						if (successor_pending)
							job_finish_times.push_back(std::make_pair(j_idx, finish_times));
						added_j = true;
					}

					bool successor_pending = false;
					for (const auto& succ : successors_of[job]) {
						auto to_job = succ.first->get_job_index();
						if (!scheduled_jobs.contains(to_job))
						{
							successor_pending = true;
							Time avail = clusters[affinity].core_availability(succ.first->get_min_parallelism()).max();
							Time ready_time = std::max(avail, succ.first->latest_arrival());
							bool ready = true;
							for (const auto& pred : predecessors_of[to_job])
							{
								auto from_job = pred.first->get_job_index();
								Interval<Time> ftimes(0, 0);
								if (scheduled_jobs.contains(from_job) && from.get_finish_times(from_job, ftimes))
								{
									Time susp_max = pred.second.max();
									ready_time = std::max(ready_time, ftimes.max() + susp_max);
								}
								else
								{
									ready = false;
									break;
								}
							}
							if (ready)
							{
								earliest_certain_successor_job_disptach[succ.first->get_affinity()] =
									std::min(earliest_certain_successor_job_disptach[succ.first->get_affinity()], ready_time);
							}
						}
					}
					if (successor_pending)
						job_finish_times.push_back(std::make_pair(job, std::make_pair(job_eft, job_lft)));
				}

				if (!added_j)
				{
					bool successor_pending = false;
					for (const auto& succ : successors_of[j_idx])
					{
						successor_pending = true;
						Time avail = clusters[affinity].core_availability(succ.first->get_min_parallelism()).max();
						Time ready_time = std::max(avail, succ.first->latest_arrival());
						bool ready = true;
						for (const auto& pred : predecessors_of[succ.first->get_job_index()])
						{
							auto from_job = pred.first->get_job_index();
							Interval<Time> ftimes(0, 0);
							if (from_job == j_idx)
							{
								Time susp_max = pred.second.max();
								ready_time = std::max(ready_time, lft + susp_max);
							}
							else if (scheduled_jobs.contains(from_job) && from.get_finish_times(from_job, ftimes))
							{
								Time susp_max = pred.second.max();
								ready_time = std::max(ready_time, ftimes.max() + susp_max);
							}
							else
							{
								ready = false;
								break;
							}
						}
						if (ready)
						{
							earliest_certain_successor_job_disptach[succ.first->get_affinity()] =
								std::min(earliest_certain_successor_job_disptach[succ.first->get_affinity()], ready_time);
						}
					}
					if (successor_pending)
						job_finish_times.push_back(std::make_pair(j_idx, finish_times));
				}
				// update earliest successor job ready times on each cluster
				for (int i=0; i<clusters.size(); i++)
					clusters[i].set_earliest_certain_successor_job_disptach(earliest_certain_successor_job_disptach[i]);
			}

			// Check whether the job_finish_times overlap.
			bool check_finish_times_overlap(const JobFinishTimes& from_pwj) const
			{
				bool allJobsIntersect = true;
				// The JobFinishTimes vectors are sorted.
				// Check intersect for matching jobs.
				auto from_it = from_pwj.begin();
				auto state_it = job_finish_times.begin();
				while (from_it != from_pwj.end() &&
					state_it != job_finish_times.end())
				{
					if (from_it->first == state_it->first)
					{
						if (!from_it->second.intersects(state_it->second))
						{
							allJobsIntersect = false;
							break;
						}
						from_it++;
						state_it++;
					}
					else if (from_it->first < state_it->first)
						from_it++;
					else
						state_it++;
				}
				return allJobsIntersect;
			}

			void widen_finish_times(const JobFinishTimes& from_pwj)
			{
				// The JobFinishTimes vectors are sorted.
				// Assume check_overlap() is true.
				auto from_it = from_pwj.begin();
				auto state_it = job_finish_times.begin();
				while (from_it != from_pwj.end() &&
					state_it != job_finish_times.end())
				{
					if (from_it->first == state_it->first)
					{
						state_it->second.widen(from_it->second);
						from_it++;
						state_it++;
					}
					else if (from_it->first < state_it->first)
						from_it++;
					else
						state_it++;
				}
			}

			// Find the offset in the JobFinishTimes vector where the index j should be located.
			int jft_find(const Job_index j) const
			{
				int start = 0;
				int end = job_finish_times.size();
				while (start < end) {
					int mid = (start + end) / 2;
					if (job_finish_times[mid].first == j)
						return mid;
					else if (job_finish_times[mid].first < j)
						start = mid + 1;  // mid is too small, mid+1 might fit.
					else
						end = mid;
				}
				return start;
			}

			// no accidental copies
			Schedule_state(const Schedule_state& origin) = delete;
		};

		template<class Time> class Schedule_node
		{
		private:

			typedef typename std::vector<Interval<Time>> CoreAvailability;
			std::vector<Time> earliest_pending_release;
			std::vector<Time> next_certain_successor_jobs_disptach;
			std::vector<Time> next_certain_source_job_release;
			std::vector<Time> next_certain_sequential_source_job_release;
			std::vector<Time> next_certain_gang_source_job_disptach;

			Job_set scheduled_jobs;
			hash_value_t lookup_key;
			std::vector<Interval<Time>> finish_time;
			std::vector<Time> latest_core_availability;
			std::vector<unsigned int> num_cpus; //number of cpus in each cluster
			unsigned int num_jobs_scheduled;

			// no accidental copies
			Schedule_node(const Schedule_node& origin) = delete;

			typedef Schedule_state<Time> State;

			/*struct eft_compare
			{
				bool operator() (State* x, State* y) const
				{
					return x->earliest_finish_time() < y->earliest_finish_time();
				}
			};

			typedef typename std::multiset<State*, eft_compare> State_ref_queue;*/
			typedef std::deque<State*> State_ref_queue;
			State_ref_queue states;

		public:

			// initial node
			Schedule_node(const std::vector<unsigned int>& num_cores)
				: lookup_key{ 0 }
				, num_cpus(num_cores)
				, finish_time(num_cores.size(), { 0,0 })
				, latest_core_availability(num_cores.size(), 0)
				, num_jobs_scheduled(0)
				, earliest_pending_release(num_cores.size(), 0)
				, next_certain_successor_jobs_disptach(num_cores.size(), Time_model::constants<Time>::infinity())
				, next_certain_source_job_release(num_cores.size(), Time_model::constants<Time>::infinity())
				, next_certain_sequential_source_job_release(num_cores.size(), Time_model::constants<Time>::infinity())
				, next_certain_gang_source_job_disptach(num_cores.size(), Time_model::constants<Time>::infinity())
			{
			}

			Schedule_node(
				const std::vector<unsigned int>& num_cores,
				const std::vector<Time>& next_earliest_release
			)
				: lookup_key{ 0 }
				, num_cpus(num_cores)
				, finish_time{ 0,0 }
				, latest_core_availability(num_cores.size(), 0)
				, num_jobs_scheduled(0)
				, earliest_pending_release{ next_earliest_release }
				, next_certain_successor_jobs_disptach(num_cores.size(), Time_model::constants<Time>::infinity())
				, next_certain_source_job_release(num_cores.size(), Time_model::constants<Time>::infinity())
				, next_certain_sequential_source_job_release(num_cores.size(), Time_model::constants<Time>::infinity())
				, next_certain_gang_source_job_disptach(num_cores.size(), Time_model::constants<Time>::infinity())
			{
			}

			Schedule_node(
				const std::vector<unsigned int>& num_cores,
				const std::vector<Time>& next_earliest_release,
				const std::vector<Time>& next_certain_source_job_release // the next time a job without predecessor is certainly released
			)
				: lookup_key{ 0 }
				, num_cpus(num_cores)
				, finish_time{ 0,0 }
				, latest_core_availability(num_cores.size(), 0)
				, num_jobs_scheduled(0)
				, earliest_pending_release{ next_earliest_release }
				, next_certain_successor_jobs_disptach(num_cores.size(), Time_model::constants<Time>::infinity())
				, next_certain_source_job_release{ next_certain_source_job_release }
				, next_certain_sequential_source_job_release(num_cores.size(), Time_model::constants<Time>::infinity())
				, next_certain_gang_source_job_disptach(num_cores.size(), Time_model::constants<Time>::infinity())
			{
			}

			Schedule_node(
				const std::vector<unsigned int>& num_cores,
				const std::vector<Time>& next_earliest_release,
				const std::vector<Time>& next_certain_source_job_release, // the next time a job without predecessor is certainly released
				const std::vector<Time>& next_certain_sequential_source_job_release // the next time a job without predecessor that can execute on a single core is certainly released
			)
				: lookup_key{ 0 }
				, num_cpus(num_cores)
				, finish_time(num_cores.size(), { 0,0 })
				, latest_core_availability(num_cores.size(), 0)
				, num_jobs_scheduled(0)
				, earliest_pending_release{ next_earliest_release }
				, next_certain_successor_jobs_disptach(num_cores.size(), Time_model::constants<Time>::infinity())
				, next_certain_source_job_release{ next_certain_source_job_release }
				, next_certain_sequential_source_job_release{ next_certain_sequential_source_job_release }
				, next_certain_gang_source_job_disptach(num_cores.size(), Time_model::constants<Time>::infinity())
			{
			}

			// transition: new node by scheduling a job 'j' in an existing node 'from'
			Schedule_node(
				const Schedule_node& from,
				const Job<Time>& j,
				std::size_t idx,
				const Time next_earliest_release,
				const Time next_certain_source_job_release, // the next time a job without predecessor is certainly released
				const Time next_certain_sequential_source_job_release // the next time a job without predecessor that can execute on a single core is certainly released
			)
				: scheduled_jobs{ from.scheduled_jobs, idx }
				, lookup_key{ from.next_key(j).second}
				, num_cpus(from.num_cpus)
				, num_jobs_scheduled(from.num_jobs_scheduled + 1)
				, finish_time(from.num_cpus.size(), { 0, Time_model::constants<Time>::infinity() })
				, latest_core_availability(from.num_cpus.size(), Time_model::constants<Time>::infinity()) // set to infinity because depends on states and there is no state in the node yet
				, earliest_pending_release{ from.earliest_pending_release }
				, next_certain_source_job_release{ from.next_certain_source_job_release }
				, next_certain_successor_jobs_disptach(from.num_cpus.size(), Time_model::constants<Time>::infinity()) // set to infinity because depends on states and there is no state in the node yet
				, next_certain_sequential_source_job_release{ from.next_certain_sequential_source_job_release }
				, next_certain_gang_source_job_disptach(from.num_cpus.size(), Time_model::constants<Time>::infinity()) // set to infinity because depends on states and there is no state in the node yet
			{
				earliest_pending_release[j.get_affinity()] = next_earliest_release;
				this->next_certain_source_job_release[j.get_affinity()] = next_certain_source_job_release;
				this->next_certain_sequential_source_job_release[j.get_affinity()] = next_certain_sequential_source_job_release;
			}

			// transition: new node by scheduling a set of jobs in an existing node 'from'
			Schedule_node(
				const Schedule_node& from,
				const std::vector<Job_index>& set,
				const std::vector<Time>& next_earliest_release,
				const std::vector<Time>& next_certain_source_job_release, // the next time a job without predecessor is certainly released
				const std::vector<Time>& next_certain_sequential_source_job_release // the next time a job without predecessor that can execute on a single core is certainly released
			)
				: scheduled_jobs{ from.scheduled_jobs, set }
				, lookup_key{ from.next_key(j) }
				, num_cpus(from.num_cpus)
				, num_jobs_scheduled(from.num_jobs_scheduled + set.size())
				, finish_time{ 0, Time_model::constants<Time>::infinity() }
				, latest_core_availability(num_cpus.size(), Time_model::constants<Time>::infinity()) // set to infinity because depends on states and there is no state in the node yet
				, earliest_pending_release{ next_earliest_release }
				, next_certain_source_job_release{ next_certain_source_job_release }
				, next_certain_successor_jobs_disptach(num_cpus.size(), Time_model::constants<Time>::infinity()) // set to infinity because depends on states and there is no state in the node yet
				, next_certain_sequential_source_job_release{ next_certain_sequential_source_job_release }
				, next_certain_gang_source_job_disptach(num_cpus.size(), Time_model::constants<Time>::infinity()) // set to infinity because depends on states and there is no state in the node yet
			{
			}

			~Schedule_node()
			{
				for (State* s : states)
					delete s;
			}

			const unsigned int number_of_scheduled_jobs() const
			{
				return num_jobs_scheduled;
			}

			Time earliest_job_release(const unsigned int cluster) const
			{
				return earliest_pending_release[cluster];
			}

			Time get_next_certain_source_job_release(const unsigned int cluster) const
			{
				return next_certain_source_job_release[cluster];
			}

			Time get_next_certain_sequential_source_job_release(const unsigned int cluster) const
			{
				return next_certain_sequential_source_job_release[cluster];
			}

			Time next_certain_job_ready_time(const unsigned int cluster) const
			{
				return std::min(next_certain_successor_jobs_disptach[cluster],
					std::min(next_certain_sequential_source_job_release[cluster],
						next_certain_gang_source_job_disptach[cluster]));
			}

			std::pair<unsigned int, hash_value_t> get_key() const
			{
				return std::make_pair(num_jobs_scheduled, lookup_key);
			}

			const Job_set& get_scheduled_jobs() const
			{
				return scheduled_jobs;
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

			const bool job_dependent_on_other_cluster(const Job<Time>& j, const Job_precedence_set& predecessors, const typename Job<Time>::Job_set& jobs, const Time latest_start) const
			{
				unsigned int aff = j.get_affinity();
				for (auto p : predecessors)
				{
					unsigned int aff_pred = jobs[p].get_affinity();
					// check if the predecessor is on a different cluster, 
					// was not dispatched yet, 
					// and may possibly finish before something must start on the same cluster as j
					if (aff_pred != aff
						&& latest_start >= finish_time[aff_pred].min() + jobs[p].least_exec_time(jobs[p].get_max_parallelism())
						&& !scheduled_jobs.contains(p))
					{
						return true;
					}
				}
				return false;
			}

			bool matches(const Schedule_node& other) const
			{
				return lookup_key == other.lookup_key &&
					scheduled_jobs == other.scheduled_jobs;
			}

			std::pair<unsigned int, hash_value_t> next_key(const Job<Time>& j) const
			{
				return std::make_pair(num_jobs_scheduled+1, lookup_key ^ j.get_key());
			}

			//  finish_range / finish_time contains information about the
			//     earliest and latest core availability for core 0.
			//     whenever a state is changed (through merge) or added,
			//     that interval should be adjusted.
			const Interval<Time>& finish_range(unsigned int cluster_id) const
			{
				return finish_time[cluster_id];
			}

			Time get_latest_core_availability(unsigned int cluster_id) const
			{
				assert(cluster_id < latest_core_availability.size());
				return latest_core_availability[cluster_id];
			}

			void add_state(State* s)
			{
				// Update finish_time
				if (states.empty()) {
					for (int i = 0; i < latest_core_availability.size(); i++) {
						finish_time[i] = s->cluster(i).core_availability();
						latest_core_availability[i] = s->cluster(i).core_availability(num_cpus[i]).max();
						next_certain_successor_jobs_disptach[i] = s->cluster(i).next_certain_successor_jobs_disptach();
						next_certain_gang_source_job_disptach[i] = s->cluster(i).next_certain_gang_source_job_disptach();
					}
				}
				else {
					for (int i = 0; i < latest_core_availability.size(); i++) {
						finish_time[i].widen(s->cluster(i).core_availability());
						latest_core_availability[i] = std::max(latest_core_availability[i], s->cluster(i).core_availability(num_cpus[i]).max());
						next_certain_successor_jobs_disptach[i] = std::max(next_certain_successor_jobs_disptach[i], s->cluster(i).next_certain_successor_jobs_disptach());
						next_certain_gang_source_job_disptach[i] = std::max(next_certain_gang_source_job_disptach[i], s->cluster(i).next_certain_gang_source_job_disptach());
					}
				}
				states.push_back(s);
			}

			friend std::ostream& operator<< (std::ostream& stream,
				const Schedule_node<Time>& n)
			{
				stream << "Node(" << n.states.size() << ")";
				return stream;
			}

			//return the number of states in the node
			int states_size() const
			{
				return states.size();
			}

			/*const State* get_first_state() const
			{
				auto first = states.begin();
				return *first;
			}

			const State* get_last_state() const
			{
				auto last = --(states.end());
				return *last;
			}*/

			const State_ref_queue* get_states() const
			{
				return &states;
			}

			// try to merge state 's' with up to 'budget' states already recorded in this node. 
			// The option 'useJobFinishTimes' controls whether or not the job finish time intervals of jobs 
			// with pending successors must overlap to allow two states to merge. Setting it to true should 
			// increase accurracy of the analysis but increases runtime significantly.
			bool merge_states(const Schedule_state<Time>& s, bool useJobFinishTimes = false, int budget = 1)
			{
				// try to merge with up to 'budget' states if possible.
				int merge_budget = budget;

				std::deque<State*>::iterator last_state_merged;
				//State* last_state_merged;
				bool result = false;
				for (auto state_it = states.begin(); state_it != states.end(); ++state_it)
				{
					State* state = *state_it;
					if (result == false)
					{
						if (state->try_to_merge(s, useJobFinishTimes))
						{
							for (int i = 0; i < num_cpus.size(); i++) {
								// Update the node finish_time
								finish_time[i].widen(s.cluster(i).core_availability());
								latest_core_availability[i] = std::max(latest_core_availability[i], s.cluster(i).core_availability(num_cpus[i]).max());
								//update the certain next job ready time
								next_certain_successor_jobs_disptach[i] = std::max(next_certain_successor_jobs_disptach[i], s.cluster(i).next_certain_successor_jobs_disptach());
								next_certain_gang_source_job_disptach[i] = std::max(next_certain_gang_source_job_disptach[i], s.cluster(i).next_certain_gang_source_job_disptach());
							}
							result = true;

							// Try to merge with a few more states.
							merge_budget--;
							if (merge_budget == 0)
								break;

							last_state_merged = state_it;
						}
					}
					else // if we already merged with one state at least
					{
						if (state->try_to_merge(**last_state_merged, useJobFinishTimes))
						{
							// the state was merged => we can thus remove the old one from the list of states
							states.erase(last_state_merged);
							delete *last_state_merged;

							// Try to merge with a few more states.
							// std::cerr << "Merged with " << merge_budget << " of " << states.size() << " states left.\n";
							merge_budget--;
							if (merge_budget == 0)
								break;

							last_state_merged = state_it;
						}
					}
				}

				return result;
			}
		};

	}
}

#endif