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
#include "global/cluster.hpp"


namespace NP {

	namespace Global {

		typedef Index_set Dispatched_job_set;
		typedef std::vector<Job_index> Job_precedence_set;

		template<class Time> class Schedule_state
		{
			typedef const Job<Time>* Job_ref;
			typedef std::vector<std::pair<Job_ref, Interval<Time>>> JobFinishTimes;
			typedef std::vector<std::pair<Job_ref, Interval<Time>>> Susp_list;
			typedef std::vector<Susp_list> Successors;
			typedef std::vector<Susp_list> Predecessors;

		private:
			std::vector<Cluster_state<Time>> clusters;
			// job_finish_times holds the finish times of all the jobs that still have an unscheduled successor
			JobFinishTimes job_finish_times;

		public:
			// initial state -- nothing yet has finished, nothing is running
			Schedule_state(const std::vector<unsigned int>& num_cpus)
			{
				assert(num_cpus.size() > 0);
				clusters.reserve(num_cpus.size());
				for (int i = 0; i < num_cpus.size(); i++)
				{
					clusters.emplace_back(num_cpus[i], Time_model::constants<Time>::infinity());
				}
			}

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
				const Dispatched_job_set& scheduled_jobs,
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

			// transition: new state by scheduling a set of jobs in an existing state
			Schedule_state(
				const Schedule_state& from,
				const std::vector<const Job<Time>*>& j_set,
				const std::vector<Interval<Time>>& start_times,
				const std::vector<Interval<Time>>& finish_times,
				const std::vector<unsigned int>& ncores,
				const Dispatched_job_set& scheduled_jobs,
				const std::vector<Job_precedence_set>& predecessors,
				const Successors& successors_of,
				const Predecessors& predecessors_of,
				const std::vector<Time>& next_certain_gang_source_job_disptach)
			{
				assert(j_set.size() == from.clusters.size());
				assert(start_times.size() == from.clusters.size());
				assert(finish_times.size() == from.clusters.size());
				assert(next_certain_gang_source_job_disptach.size() == from.clusters.size());
				assert(ncores.size() == from.clusters.size());

				clusters.reserve(from.clusters.size());
				for (int i = 0; i < from.clusters.size(); i++)
				{
					const Job<Time>* j = j_set[i];
					if (j == NULL)
						clusters.push_back(Cluster_state<Time>(from.cluster(i)));
					else
					{
						Job_index j_idx = j->get_job_index();
						// check if j has precedence constraints
						const Job_precedence_set& pred = predecessors.size() > j_idx ? predecessors[j_idx] : Job_precedence_set{};
						clusters.push_back(Cluster_state<Time>(from.cluster(i), j_idx, pred, start_times[i], finish_times[i], next_certain_gang_source_job_disptach[i], ncores[i]));
					}
				}
				assert(clusters.size() == from.clusters.size());

				// save the job finish time of every job with a successor that is not executed yet in the current state
				update_job_finish_times(from, j_set, start_times, finish_times, successors_of, predecessors_of, scheduled_jobs);

				DM("*** new state: constructed " << *this << std::endl);
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
				if (offset < job_finish_times.size() && job_finish_times[offset].first->get_job_index() == j)
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
				const std::vector<const Job<Time>*>& j_set, const std::vector<Interval<Time>> start_times,
				const std::vector<Interval<Time>> finish_times,
				const Successors& successors_of,
				const Predecessors& predecessors_of,
				const Dispatched_job_set& scheduled_jobs)
			{
				job_finish_times.reserve(job_finish_times.size() + clusters.size());
				job_finish_times.assign(from.job_finish_times.begin(), from.job_finish_times.end());

				for (unsigned int affinity = 0; affinity < clusters.size(); ++affinity) {
					const Job<Time>* j = j_set[affinity];
					if (j == NULL)
						continue;

					Job_index j_idx = j->get_job_index();

					JobFinishTimes job_finish_times_temp;
					job_finish_times_temp.reserve(job_finish_times.size() + clusters.size());

					Time lst = start_times[affinity].max();
					Time lft = finish_times[affinity].max();

					bool added_j = false;
					std::vector<Time> earliest_certain_successor_job_disptach(clusters.size(), Time_model::constants<Time>::infinity());
					for (const auto& ft : job_finish_times)
					{
						auto job_ref = ft.first;
						auto job = job_ref->get_job_index();
						auto job_eft = ft.second.min();
						auto job_lft = ft.second.max();
						// if there is a single core, then we know that 
						// jobs that were disptached in the past cannot have 
						// finished later than when our new job starts executing
						if (job_ref->get_affinity() == affinity && clusters[affinity].num_cpus() == 1)
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
								Time avail = clusters[succ.first->get_affinity()].core_availability(succ.first->get_min_parallelism()).max();
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
								job_finish_times_temp.emplace_back(j, finish_times[affinity]);
							added_j = true;
						}

						bool successor_pending = false;
						for (const auto& succ : successors_of[job]) {
							auto to_job = succ.first->get_job_index();
							if (!scheduled_jobs.contains(to_job))
							{
								successor_pending = true;
								Time avail = clusters[succ.first->get_affinity()].core_availability(succ.first->get_min_parallelism()).max();
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
							job_finish_times_temp.emplace_back(job_ref, Interval<Time>(job_eft, job_lft));
					}

					if (!added_j)
					{
						bool successor_pending = false;
						for (const auto& succ : successors_of[j_idx])
						{
							successor_pending = true;
							Time avail = clusters[succ.first->get_affinity()].core_availability(succ.first->get_min_parallelism()).max();
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
							job_finish_times_temp.emplace_back(j, finish_times[affinity]);
					}
					job_finish_times.swap(job_finish_times_temp);

					// update earliest successor job ready times on each cluster
					for (int i = 0; i < clusters.size(); i++)
						clusters[i].set_earliest_certain_successor_job_disptach(earliest_certain_successor_job_disptach[i]);
				}
			}
			
			
			// update the list of finish times of jobs with successors w.r.t. the previous system state
			// and calculate the earliest time a job with precedence constraints will become ready to dispatch
			void update_job_finish_times(const Schedule_state& from,
				const Job<Time>& j, Interval<Time> start_times,
				Interval<Time> finish_times,
				const Successors& successors_of,
				const Predecessors& predecessors_of,
				const Dispatched_job_set& scheduled_jobs)
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
					auto job_ref = ft.first;
					auto job = job_ref->get_job_index();
					auto job_eft = ft.second.min();
					auto job_lft = ft.second.max();
					// if there is a single core, then we know that 
					// jobs that were disptached in the past cannot have 
					// finished later than when our new job starts executing
					if (job_ref->get_affinity() == affinity && clusters[affinity].num_cpus() == 1)
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
							Time avail = clusters[succ.first->get_affinity()].core_availability(succ.first->get_min_parallelism()).max();
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
							job_finish_times.emplace_back(&j, finish_times);
						added_j = true;
					}

					bool successor_pending = false;
					for (const auto& succ : successors_of[job]) {
						auto to_job = succ.first->get_job_index();
						if (!scheduled_jobs.contains(to_job))
						{
							successor_pending = true;
							Time avail = clusters[succ.first->get_affinity()].core_availability(succ.first->get_min_parallelism()).max();
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
						job_finish_times.emplace_back(job_ref, Interval<Time>(job_eft, job_lft));
				}

				if (!added_j)
				{
					bool successor_pending = false;
					for (const auto& succ : successors_of[j_idx])
					{
						successor_pending = true;
						Time avail = clusters[succ.first->get_affinity()].core_availability(succ.first->get_min_parallelism()).max();
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
						job_finish_times.emplace_back(&j, finish_times);
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
					if (job_finish_times[mid].first->get_job_index() == j)
						return mid;
					else if (job_finish_times[mid].first->get_job_index() < j)
						start = mid + 1;  // mid is too small, mid+1 might fit.
					else
						end = mid;
				}
				return start;
			}

			// no accidental copies
			Schedule_state(const Schedule_state& origin) = delete;
		};
	}
}

#endif