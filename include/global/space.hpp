#ifndef GLOBAL_SPACE_H
#define GLOBAL_SPACE_H

#include <algorithm>
#include <deque>
#include <forward_list>
#include <map>
#include <unordered_map>
#include <vector>

#include <cassert>
#include <iostream>
#include <ostream>

#include "config.h"

#ifdef CONFIG_PARALLEL
#include "tbb/concurrent_hash_map.h"
#include "tbb/enumerable_thread_specific.h"
#include "tbb/parallel_for.h"
#include <atomic>
#endif

#include "problem.hpp"
#include "clock.hpp"

#include "global/state.hpp"

#define NOSUSP 0
#define GENERAL_SUSP 1
#define PATHWISE_SUSP 2

namespace NP {

	namespace Global {

		template<class Time> class State_space
		{
		public:

			typedef Scheduling_problem<Time> Problem;
			typedef typename Scheduling_problem<Time>::Workload Workload;
			typedef typename Scheduling_problem<Time>::Precedence_constraints Precedence_constraints;
			typedef typename Scheduling_problem<Time>::Abort_actions Abort_actions;
			typedef Schedule_state<Time> State;
			typedef typename std::vector<Interval<Time>> CoreAvailability;

			typedef Schedule_node<Time> Node;

			static State_space* explore(
				const Problem& prob,
				const Analysis_options& opts)
			{
				State_space* s = new State_space(prob.jobs, prob.prec, prob.aborts, prob.num_processors[0],
					opts.timeout, opts.max_depth, opts.early_exit, opts.use_supernodes);
				s->be_naive = opts.be_naive;
				s->cpu_time.start();
				s->explore();
				s->cpu_time.stop();
				return s;
			}

			// convenience interface for tests
			static State_space* explore_naively(
				const Workload& jobs,
				unsigned int num_cpus = 1)
			{
				Problem p{ jobs, num_cpus };
				Analysis_options o;
				o.be_naive = true;
				return explore(p, o);
			}

			// convenience interface for tests
			static State_space* explore(
				const Workload& jobs,
				unsigned int num_cpus = 1)
			{
				Problem p{ jobs, num_cpus };
				Analysis_options o;
				return explore(p, o);
			}

			// return the BCRT and WCRT of job j 
			Interval<Time> get_finish_times(const Job<Time>& j) const
			{
				return get_finish_times(j.get_job_index());
			}

			Interval<Time> get_finish_times(Job_index j) const
			{
				if (rta[j].valid) {
					return rta[j].rt;
				}
				else {
					return Interval<Time>{0, Time_model::constants<Time>::infinity()};
				}
			}

			bool is_schedulable() const
			{
				return !aborted && !observed_deadline_miss;
			}

			bool was_timed_out() const
			{
				return timed_out;
			}

			//currently unused, only required to compile nptest.cpp correctly
			unsigned long number_of_nodes() const
			{
				return num_nodes;
			}

			unsigned long number_of_states() const
			{
				return num_states;
			}

			unsigned long number_of_edges() const
			{
				return num_edges;
			}

			unsigned long max_exploration_front_width() const
			{
				return width;
			}

			double get_cpu_time() const
			{
				return cpu_time;
			}

			typedef std::deque<Node> Nodes;
			typedef std::deque<State> States;

#ifdef CONFIG_PARALLEL
			typedef tbb::enumerable_thread_specific< Nodes > Split_nodes;
			typedef std::deque<Split_nodes> Nodes_storage;
#else
			typedef std::deque< Nodes > Nodes_storage;
#endif

#ifdef CONFIG_COLLECT_SCHEDULE_GRAPH

			struct Edge {
				const Job<Time>* scheduled;
				const Node* source;
				const Node* target;
				const Interval<Time> finish_range;
				const unsigned int parallelism;

				Edge(const Job<Time>* s, const Node* src, const Node* tgt,
					const Interval<Time>& fr, unsigned int parallelism = 1)
					: scheduled(s)
					, source(src)
					, target(tgt)
					, finish_range(fr)
					, parallelism(parallelism)
				{
				}

				bool deadline_miss_possible() const
				{
					return scheduled->exceeds_deadline(finish_range.upto());
				}

				Time earliest_finish_time() const
				{
					return finish_range.from();
				}

				Time latest_finish_time() const
				{
					return finish_range.upto();
				}

				Time earliest_start_time() const
				{
					return finish_range.from() - scheduled->least_exec_time();
				}

				Time latest_start_time() const
				{
					return finish_range.upto() - scheduled->maximal_exec_time();
				}

				unsigned int parallelism_level() const
				{
					return parallelism;
				}
			};

			const std::deque<Edge>& get_edges() const
			{
				return edges;
			}

			const Nodes_storage& get_nodes() const
			{
				return nodes_storage;
			}


#endif
		private:

			typedef Node* Node_ref;
			typedef typename std::forward_list<Node_ref> Node_refs;
			typedef State* State_ref;
			typedef typename std::forward_list<State_ref> State_refs;

#ifdef CONFIG_PARALLEL
			typedef tbb::concurrent_hash_map<hash_value_t, Node_refs> Nodes_map;
			typedef typename Nodes_map::accessor Nodes_map_accessor;
#else
			typedef std::unordered_map<hash_value_t, Node_refs> Nodes_map;
#endif


			typedef const Job<Time>* Job_ref;
			typedef std::multimap<Time, Job_ref> By_time_map;

			// Similar to uni/space.hpp, make Response_times a vector of intervals.

			// typedef std::unordered_map<Job_index, Interval<Time> > Response_times;
			struct Response_time_item {
				bool valid;
				Interval<Time> rt;

				Response_time_item()
					: valid(false)
					, rt(0, 0)
				{
				}
			};
			typedef std::vector<Response_time_item> Response_times;



#ifdef CONFIG_COLLECT_SCHEDULE_GRAPH
			std::deque<Edge> edges;
#endif
			// Similar to uni/space.hpp, make rta a 

			Response_times rta;

#ifdef CONFIG_PARALLEL
			tbb::enumerable_thread_specific<Response_times> partial_rta;
#endif

			bool aborted;
			bool timed_out;
			bool observed_deadline_miss;
			bool early_exit;

			const unsigned int max_depth;

			bool be_naive;

			const Workload& jobs;

			// not touched after initialization
			By_time_map _successor_jobs_by_latest_arrival;
			By_time_map _sequential_source_jobs_by_latest_arrival;
			By_time_map _gang_source_jobs_by_latest_arrival;
			By_time_map _jobs_by_earliest_arrival;
			By_time_map _jobs_by_deadline;
			std::vector<Job_precedence_set> _predecessors;

			// use these const references to ensure read-only access
			const By_time_map& successor_jobs_by_latest_arrival;
			const By_time_map& sequential_source_jobs_by_latest_arrival;
			const By_time_map& gang_source_jobs_by_latest_arrival;
			const By_time_map& jobs_by_earliest_arrival;
			const By_time_map& jobs_by_deadline;
			const std::vector<Job_precedence_set>& predecessors;

			typedef std::vector<std::pair<Job_ref, Interval<Time>>> Suspensions_list;

			// not touched after initialization
			std::vector<Suspensions_list> _predecessors_suspensions;
			std::vector<Suspensions_list> _successors;
			// use these const references to ensure read-only access
			const std::vector<Suspensions_list>& predecessors_suspensions;
			const std::vector<Suspensions_list>& successors;

			// list of actions when a job is aborted
			std::vector<const Abort_action<Time>*> abort_actions;

			Nodes_storage nodes_storage;
			Nodes_map nodes_by_key;

#ifdef CONFIG_PARALLEL
			std::atomic_ulong num_nodes, num_states, num_edges;
#else
			unsigned long num_nodes, num_states, num_edges;
#endif
			// updated only by main thread
			unsigned long current_job_count, width;



#ifdef CONFIG_PARALLEL
			tbb::enumerable_thread_specific<unsigned long> edge_counter;
#endif
			Processor_clock cpu_time;
			const double timeout;
			const unsigned int num_cpus;
			bool use_supernodes = true;

			State_space(const Workload& jobs,
				const Precedence_constraints& edges,
				const Abort_actions& aborts,
				unsigned int num_cpus,
				double max_cpu_time = 0,
				unsigned int max_depth = 0,
				bool early_exit = true,
				bool use_supernodes = true)
				: jobs(jobs)
				, aborted(false)
				, timed_out(false)
				, observed_deadline_miss(false)
				, be_naive(false)
				, timeout(max_cpu_time)
				, max_depth(max_depth)
				, num_nodes(0)
				, num_states(0)
				, num_edges(0)
				, width(0)
				, rta(jobs.size())
				, current_job_count(0)
				, num_cpus(num_cpus)
				, successor_jobs_by_latest_arrival(_successor_jobs_by_latest_arrival)
				, sequential_source_jobs_by_latest_arrival(_sequential_source_jobs_by_latest_arrival)
				, gang_source_jobs_by_latest_arrival(_gang_source_jobs_by_latest_arrival)
				, jobs_by_earliest_arrival(_jobs_by_earliest_arrival)
				, jobs_by_deadline(_jobs_by_deadline)
				, _predecessors(jobs.size())
				, predecessors(_predecessors)
				, _predecessors_suspensions(jobs.size())
				, _successors(jobs.size())
				, predecessors_suspensions(_predecessors_suspensions)
				, successors(_successors)
				, early_exit(early_exit)
				, abort_actions(jobs.size(), NULL)
				, use_supernodes(use_supernodes)
#ifdef CONFIG_PARALLEL
				, partial_rta(jobs.size())
#endif
			{
				for (const auto& e : edges) {
					_predecessors_suspensions[e.get_toIndex()].push_back({ &jobs[e.get_fromIndex()], e.get_suspension() });
					_predecessors[e.get_toIndex()].push_back(e.get_fromIndex());
					_successors[e.get_fromIndex()].push_back({ &jobs[e.get_toIndex()], e.get_suspension() });
				}

				for (const Job<Time>& j : jobs) {
					if (_predecessors_suspensions[j.get_job_index()].size() > 0) {
						_successor_jobs_by_latest_arrival.insert({ j.latest_arrival(), &j });
					}
					else if (j.get_min_parallelism() == 1) {
						_sequential_source_jobs_by_latest_arrival.insert({ j.latest_arrival(), &j });
					}
					else {
						_gang_source_jobs_by_latest_arrival.insert({ j.latest_arrival(), &j });
					}
					_jobs_by_earliest_arrival.insert({ j.earliest_arrival(), &j });
					_jobs_by_deadline.insert({ j.get_deadline(), &j });
				}

				for (const Abort_action<Time>& a : aborts) {
					const Job<Time>& j = lookup<Time>(jobs, a.get_id());
					abort_actions[j.get_job_index()] = &a;
				}
			}

		private:

			void count_edge()
			{
#ifdef CONFIG_PARALLEL
				edge_counter.local()++;
#else
				num_edges++;
#endif
			}

			static Time max_deadline(const Workload& jobs)
			{
				Time dl = 0;
				for (const auto& j : jobs)
					dl = std::max(dl, j.get_deadline());
				return dl;
			}

			void update_finish_times(Response_times& r, const Job_index id,
				Interval<Time> range)
			{
				if (!r[id].valid) {
					r[id].valid = true;
					r[id].rt = range;
				}
				else {
					r[id].rt |= range;
				}
				DM("RTA " << id << ": " << r[id].rt << std::endl);
			}

			void update_finish_times(
				Response_times& r, const Job<Time>& j, Interval<Time> range)
			{
				update_finish_times(r, j.get_job_index(), range);
				if (j.exceeds_deadline(range.upto())) {
					observed_deadline_miss = true;

					if (early_exit)
						aborted = true;
				}
			}

			void update_finish_times(const Job<Time>& j, Interval<Time> range)
			{
				Response_times& r =
#ifdef CONFIG_PARALLEL
					partial_rta.local();
#else
					rta;
#endif
				update_finish_times(r, j, range);
			}


			std::size_t index_of(const Job<Time>& j) const
			{
				return j.get_job_index();// (std::size_t)(&j - &(jobs[0]));
			}
			const Job<Time>& reverse_index_of(std::size_t index) const
			{
				return jobs[index];
			}

			const Job_precedence_set& predecessors_of(const Job<Time>& j) const
			{
				return predecessors[j.get_job_index()];
			}

			// Check if any job is guaranteed to miss its deadline in any state in the new node
			void check_for_deadline_misses(const Node& old_n, const Node& new_n)
			{
				auto check_from = old_n.get_first_state()->core_availability().min();

				// check if we skipped any jobs that are now guaranteed
				// to miss their deadline
				for (auto it = jobs_by_deadline.lower_bound(check_from);
					it != jobs_by_deadline.end(); it++) {
					const Job<Time>& j = *(it->second);
					auto pmin = j.get_min_parallelism();
					auto earliest = new_n.get_last_state()->core_availability(pmin).min();
					if (j.get_deadline() < earliest) {
						if (unfinished(new_n, j)) {
							DM("deadline miss: " << new_n << " -> " << j << std::endl);
							// This job is still incomplete but has no chance
							// of being scheduled before its deadline anymore.
							observed_deadline_miss = true;
							// if we stop at the first deadline miss, abort and create node in the graph for explanation purposes
							if (early_exit) 
							{
								aborted = true;
								// create a dummy node for explanation purposes
								auto frange = new_n.get_last_state()->core_availability(pmin) + j.get_cost(pmin);
								Node& next =
									new_node(new_n, j, j.get_job_index(), 0, 0, 0);
								//const CoreAvailability empty_cav = {};
								State& next_s = new_state(*new_n.get_last_state(), j.get_job_index(), predecessors_of(j), frange, frange, new_n.get_scheduled_jobs(), successors, predecessors_suspensions, 0, pmin);
								next.add_state(&next_s);
								num_states++;

								// update response times
								update_finish_times(j, frange);
#ifdef CONFIG_COLLECT_SCHEDULE_GRAPH
								edges.emplace_back(&j, &new_n, &next, frange, pmin);
#endif
								count_edge();
							}
							break;
						}
					}
					else
						// deadlines now after the next earliest finish time
						break;
				}
			}

			void make_initial_node(unsigned num_cores)
			{
				// construct initial state
				nodes_storage.emplace_back();

				Time next_certain_seq_release = Time_model::constants<Time>::infinity();
				if (!sequential_source_jobs_by_latest_arrival.empty())
					next_certain_seq_release = sequential_source_jobs_by_latest_arrival.begin()->first;

				Time next_certain_gang_release = Time_model::constants<Time>::infinity();
				if (!gang_source_jobs_by_latest_arrival.empty())
					next_certain_gang_release = gang_source_jobs_by_latest_arrival.begin()->first;

				Time next_certain_release = std::min(next_certain_seq_release, next_certain_gang_release);

				Node& n = new_node(num_cores, jobs_by_earliest_arrival.begin()->first, next_certain_release, next_certain_seq_release);
				State& s = new_state(num_cores, next_certain_gang_release);
				n.add_state(&s);
				num_states++;
			}

			Nodes& nodes()
			{
#ifdef CONFIG_PARALLEL
				return nodes_storage.back().local();
#else
				return nodes_storage.back();
#endif
			}

			template <typename... Args>
			Node_ref alloc_node(Args&&... args)
			{
				nodes().emplace_back(std::forward<Args>(args)...);
				Node_ref n = &(*(--nodes().end()));

				// make sure we didn't screw up...
				auto njobs = n->number_of_scheduled_jobs();
				assert(
					(!njobs && num_states == 0) // initial state
					|| (njobs == current_job_count + 1) // normal State
					|| (njobs == current_job_count + 2 && aborted) // deadline miss
				);

				return n;
			}

			template <typename... Args>
			State& new_state(Args&&... args)
			{
				return *(new State(std::forward<Args>(args)...));
			}


			template <typename... Args>
			void new_or_merge_state(Node& n, Args&&... args)
			{
				// create a new state.
				State& new_s = new_state(std::forward<Args>(args)...);

				// try to merge the new state with existing states in node n.
				if (!(n.get_states()->empty()) && n.merge_states(new_s, false))
					delete& new_s; // if we could merge no need to keep track of the new state anymore
				else
				{
					n.add_state(&new_s); // else add the new state to the node
					num_states++;
				}
			}


#ifdef CONFIG_PARALLEL
			//warning  "Parallel code is not updated for supernodes."

			// make node available for fast lookup
			void insert_cache_node(Nodes_map_accessor& acc, Node_ref n)
			{
				assert(!acc.empty());

				Node_refs& list = acc->second;
				list.push_front(n);
			}

			template <typename... Args>
			Node& new_node_at(Nodes_map_accessor& acc, Args&&... args)
			{
				assert(!acc.empty());
				Node_ref n = alloc_node(std::forward<Args>(args)...);
				DM("new node - global " << n << std::endl);
				// add node to nodes_by_key map.
				insert_cache_node(acc, n);
				num_nodes++;
				return *n;
			}

			template <typename... Args>
			Node& new_node(Args&&... args)
			{
				Nodes_map_accessor acc;
				Node_ref n = alloc_node(std::forward<Args>(args)...);
				while (true) {
					if (nodes_by_key.find(acc, n->get_key()) || nodes_by_key.insert(acc, n->get_key())) {
						DM("new node - global " << n << std::endl);
						// add node to nodes_by_key map.
						insert_cache_node(acc, n);
						num_nodes++;
						return *n;
					}
				}
			}

#else
			void cache_node(Node_ref n)
			{
				// create a new list if needed, or lookup if already existing
				auto res = nodes_by_key.emplace(
					std::make_pair(n->get_key(), Node_refs()));

				auto pair_it = res.first;
				Node_refs& list = pair_it->second;

				list.push_front(n);
			}

			template <typename... Args>
			Node& new_node(Args&&... args)
			{
				Node_ref n = alloc_node(std::forward<Args>(args)...);
				DM("new node - global " << n << std::endl);
				// add node to nodes_by_key map.
				cache_node(n);
				num_nodes++;
				return *n;
			}
#endif

			void check_cpu_timeout()
			{
				if (timeout && get_cpu_time() > timeout) {
					aborted = true;
					timed_out = true;
				}
			}

			void check_depth_abort()
			{
				if (max_depth && current_job_count > max_depth)
					aborted = true;
			}

			bool unfinished(const Node& n, const Job<Time>& j) const
			{
				return n.job_incomplete(j.get_job_index());
			}

			// Check wether a job is ready (not dspatched yet and all its predecessors are completed).
			bool ready(const Node& n, const Job<Time>& j) const
			{
				return unfinished(n, j) && n.job_ready(predecessors_of(j));
			}

			bool all_jobs_scheduled(const Node& n) const
			{
				return n.number_of_scheduled_jobs() == jobs.size();
			}

			// assumes j is ready
			Interval<Time> ready_times(const State& s, const Job<Time>& j) const
			{
				Interval<Time> r = j.arrival_window();
				for (const auto& pred : predecessors_suspensions[j.get_job_index()])
				{
					auto pred_idx = pred.first->get_job_index();
					auto pred_susp = pred.second;
					Interval<Time> ft{ 0, 0 };
					//if (!s.get_finish_times(pred_idx, ft))
					//	ft = get_finish_times(jobs[pred_idx]);
					s.get_finish_times(pred_idx, ft);
					r.lower_bound(ft.min() + pred_susp.min());
					r.extend_to(ft.max() + pred_susp.max());
				}
				return r;
			}

			// assumes j is ready
			Interval<Time> ready_times(
				const State& s, const Job<Time>& j,
				const Job_precedence_set& disregard,
				const unsigned int ncores = 1) const
			{
				Time avail_min = s.earliest_finish_time();
				Interval<Time> r = j.arrival_window();

				// if the minimum parallelism of j is more than ncores, then 
				// for j to be released and have its successors completed 
				// is not enough to interfere with a lower priority job.
				// It must also have enough cores free.
				if (j.get_min_parallelism() > ncores)
				{
					// max {rj_max,Amax(sjmin)}
					r.extend_to(s.core_availability(j.get_min_parallelism()).max());
				}

				for (const auto& pred : predecessors_suspensions[j.get_job_index()])
				{
					auto pred_idx = pred.first->get_job_index();
					// skip if part of disregard
					if (contains(disregard, pred_idx))
						continue;

					// if there is no suspension time and there is a single core, then
					// predecessors are finished as soon as the processor becomes available
					auto pred_susp = pred.second;
					if (num_cpus == 1 && pred_susp.max() == 0)
					{
						r.lower_bound(avail_min);
						r.extend_to(avail_min);
					}
					else
					{
						Interval<Time> ft{ 0, 0 };
						//if (!s.get_finish_times(pred_idx, ft))
						//	ft = get_finish_times(jobs[pred_idx]);
						s.get_finish_times(pred_idx, ft);
						r.lower_bound(ft.min() + pred_susp.min());
						r.extend_to(ft.max() + pred_susp.max());
					}
				}
				return r;
			}

			Time latest_ready_time(const State& s, const Job<Time>& j) const
			{
				return ready_times(s, j).max();
			}

			Time latest_ready_time(
				const State& s, Time earliest_ref_ready,
				const Job<Time>& j_hp, const Job<Time>& j_ref,
				const unsigned int ncores = 1) const
			{
				auto rt = ready_times(s, j_hp, predecessors_of(j_ref), ncores);
				return std::max(rt.max(), earliest_ref_ready);
			}

			Time earliest_ready_time(const State& s, const Job<Time>& j) const
			{
				return ready_times(s, j).min(); // std::max(s.core_availability().min(), j.arrival_window().min());
			}

			// Find next time by which a sequential source job (i.e., 
			// a job without predecessors that can execute on a single core) 
			// of higher priority than the reference_job
			// is certainly released in any state in the node 'n'. 
			Time next_certain_higher_priority_seq_source_job_release(
				const Node& n,
				const Job<Time>& reference_job,
				Time until = Time_model::constants<Time>::infinity()) const
			{
				Time when = until;

				// a higher priority source job cannot be released before 
				// a source job of any priority is released
				Time t_earliest = n.get_next_certain_source_job_release();

				for (auto it = sequential_source_jobs_by_latest_arrival.lower_bound(t_earliest);
					it != sequential_source_jobs_by_latest_arrival.end(); it++)
				{
					const Job<Time>& j = *(it->second);

					// check if we can stop looking
					if (when < j.latest_arrival())
						break; // yep, nothing can lower 'when' at this point

					// j is not relevant if it is already scheduled or not of higher priority
					if (ready(n, j) && j.higher_priority_than(reference_job))
					{
						when = j.latest_arrival();
						// Jobs are ordered by latest_arrival, so next jobs are later. 
						// We can thus stop searching.
						break;
					}
				}
				return when;
			}

			// Find next time by which a gang source job (i.e., 
			// a job without predecessors that cannot execute on a single core) 
			// of higher priority than the reference_job
			// is certainly released in state 's' of node 'n'. 
			Time next_certain_higher_priority_gang_source_job_ready_time(
				const Node& n,
				const State& s,
				const Job<Time>& reference_job,
				const unsigned int ncores,
				Time until = Time_model::constants<Time>::infinity()) const
			{
				Time when = until;

				// a higher priority source job cannot be released before 
				// a source job of any priority is released
				Time t_earliest = n.get_next_certain_source_job_release();

				for (auto it = gang_source_jobs_by_latest_arrival.lower_bound(t_earliest);
					it != gang_source_jobs_by_latest_arrival.end(); it++)
				{
					const Job<Time>& j = *(it->second);

					// check if we can stop looking
					if (when < j.latest_arrival())
						break; // yep, nothing can lower 'when' at this point

					// j is not relevant if it is already scheduled or not of higher priority
					if (ready(n, j) && j.higher_priority_than(reference_job))
					{
						// if the minimum parallelism of j is more than ncores, then 
						// for j to be released and have its successors completed 
						// is not enough to interfere with a lower priority job.
						// It must also have enough cores free.
						if (j.get_min_parallelism() > ncores)
						{
							// max {rj_max,Amax(sjmin)}
							when = std::min(when, std::max(j.latest_arrival(), s.core_availability(j.get_min_parallelism()).max()));
							// no break as other jobs may require less cores to be available and thus be ready earlier
						}
						else
						{
							when = std::min(when, j.latest_arrival());
							// jobs are ordered in non-decreasing latest arrival order, 
							// => nothing tested after can be ready earlier than j
							// => we break
							break;
						}
					}
				}
				return when;
			}

			// Find next time by which a successor job (i.e., a job with predecessors) 
			// of higher priority than the reference_job
			// is certainly released in system state 's' at or before a time 'until'.
			Time next_certain_higher_priority_successor_job_ready_time(
				const Node& n,
				const State& s,
				const Job<Time>& reference_job,
				const unsigned int ncores,
				Time until = Time_model::constants<Time>::infinity()) const
			{
				auto ready_min = earliest_ready_time(s, reference_job);
				Time when = until;

				// a higer priority successor job cannot be ready before 
				// a job of any priority is released
				Time t_earliest = n.earliest_job_release();
				for (auto it = successor_jobs_by_latest_arrival.lower_bound(t_earliest);
					it != successor_jobs_by_latest_arrival.end(); it++)
				{
					const Job<Time>& j = *(it->second);

					// check if we can stop looking
					if (when < j.latest_arrival())
						break; // yep, nothing can lower 'when' at this point

					// j is not relevant if it is already scheduled or not of higher priority
					if (ready(n, j) && j.higher_priority_than(reference_job)) {
						// does it beat what we've already seen?
						when = std::min(when,
							latest_ready_time(s, ready_min, j, reference_job, ncores));
						// No break, as later jobs might have less suspension or require less cores to start executing.
					}
				}
				return when;
			}

			Time next_certain_higher_priority_successor_job_ready_time(
				const Node& n,
				const State& s,
				const Job<Time>& reference_job,
				Time until = Time_model::constants<Time>::infinity()) const
			{
				return next_certain_higher_priority_successor_job_ready_time(n, s, reference_job, 1, until);
			}

			// find next time by which a job is certainly ready in system state 's'
			Time next_certain_job_ready_time(const Node& n, const State& s) const
			{
				//TODO: may have to account for the number of available cores in a state
				Time t_ws = std::min(s.next_certain_gang_source_job_disptach(), s.next_certain_successor_jobs_disptach());
				Time t_wos = n.get_next_certain_sequential_source_job_release();
				return std::min(t_wos, t_ws);
			}

			Time earliest_job_abortion(const Abort_action<Time>& a)
			{
				return a.earliest_trigger_time() + a.least_cleanup_cost();
			}

			Time latest_job_abortion(const Abort_action<Time>& a)
			{
				return a.latest_trigger_time() + a.maximum_cleanup_cost();
			}

			// assumes j is ready
			// NOTE: we don't use Interval<Time> here because the
			//       Interval c'tor sorts its arguments.
			std::pair<Time, Time> start_times(
				const State& s, const Job<Time>& j, const Time t_wc, const Time t_high,
				const Time t_avail, const unsigned int ncores = 1) const
			{
				auto rt = earliest_ready_time(s, j);
				auto at = s.core_availability(ncores).min();
				Time est = std::max(rt, at);

				DM("rt: " << rt << std::endl
					<< "at: " << at << std::endl);

				Time lst = std::min(t_wc,
					std::min(t_high, t_avail) - Time_model::constants<Time>::epsilon());

				DM("est: " << est << std::endl);
				DM("lst: " << lst << std::endl);

				return { est, lst };
			}

			// Find the earliest possible job release of all jobs in a node except for the ignored job
			Time earliest_possible_job_release(
				const Node& n,
				const Job<Time>& ignored_job)
			{
				DM("      - looking for earliest possible job release starting from: "
					<< n.earliest_job_release() << std::endl);

				for (auto it = jobs_by_earliest_arrival.lower_bound(n.earliest_job_release());
					it != jobs_by_earliest_arrival.end(); 	it++)
				{
					const Job<Time>& j = *(it->second);

					DM("         * looking at " << j << std::endl);

					// skip if it is the one we're ignoring or if it was dispatched already
					if (&j == &ignored_job || !unfinished(n, j))
						continue;

					DM("         * found it: " << j.earliest_arrival() << std::endl);
					// it's incomplete and not ignored => found the earliest
					return j.earliest_arrival();
				}

				DM("         * No more future releases" << std::endl);
				return Time_model::constants<Time>::infinity();
			}

			// Find the earliest possible certain job release of all sequential source jobs 
			// (i.e., without predecessors and with minimum parallelism = 1) 
			// in a node except for the ignored job
			Time earliest_certain_sequential_source_job_release(
				const Node& n,
				const Job<Time>& ignored_job)
			{
				DM("      - looking for earliest certain source job release starting from: "
					<< n.get_next_certain_source_job_release() << std::endl);

				for (auto it = sequential_source_jobs_by_latest_arrival.lower_bound(n.get_next_certain_source_job_release());
					it != sequential_source_jobs_by_latest_arrival.end(); it++)
				{
					const Job<Time>* jp = it->second;
					DM("         * looking at " << *jp << std::endl);

					// skip if it is the one we're ignoring or the job was dispatched already
					if (jp == &ignored_job || !unfinished(n, *jp))
						continue;

					DM("         * found it: " << jp->latest_arrival() << std::endl);
					// it's incomplete and not ignored => found the earliest
					return jp->latest_arrival();
				}
				DM("         * No more future source job releases" << std::endl);
				return Time_model::constants<Time>::infinity();
			}

			// Find the earliest possible certain job release of all source jobs (i.e., without predecessors) 
			// in a node except for the ignored job
			Time earliest_certain_source_job_release(
				const Node& n,
				const Job<Time>& ignored_job)
			{
				DM("      - looking for earliest certain source job release starting from: "
					<< n.get_next_certain_source_job_release() << std::endl);

				Time rmax = earliest_certain_sequential_source_job_release(n, ignored_job);

				for (auto it = gang_source_jobs_by_latest_arrival.lower_bound(n.get_next_certain_source_job_release());
					it != gang_source_jobs_by_latest_arrival.end(); it++)
				{
					const Job<Time>* jp = it->second;
					DM("         * looking at " << *jp << std::endl);

					// skip if it is the one we're ignoring or the job was dispatched already
					if (jp == &ignored_job || !unfinished(n, *jp))
						continue;

					DM("         * found it: " << jp->latest_arrival() << std::endl);
					// it's incomplete and not ignored => found the earliest
					return std::min(rmax, jp->latest_arrival());
				}

				DM("         * No more future releases" << std::endl);
				return rmax;
			}

			// returns the earliest time a gang source job (i.e., a job without predecessors that requires more than one core to start executing)
			// is certainly released and has enough cores available to start executing
			Time earliest_certain_gang_source_job_disptach(
				const Node& n,
				const State& s,
				const Job<Time>& ignored_job)
			{
				DM("      - looking for earliest certain source job release starting from: "
					<< n.get_next_certain_source_job_release() << std::endl);

				Time rmax = Time_model::constants<Time>::infinity();

				for (auto it = gang_source_jobs_by_latest_arrival.lower_bound(n.get_next_certain_source_job_release());
					it != gang_source_jobs_by_latest_arrival.end(); it++)
				{
					const Job<Time>* jp = it->second;
					if (jp->latest_arrival() >= rmax)
						break;

					DM("         * looking at " << *jp << std::endl);

					// skip if it is the one we're ignoring or the job was dispatched already
					if (jp == &ignored_job || !unfinished(n, *jp))
						continue;

					DM("         * found it: " << jp->latest_arrival() << std::endl);
					// it's incomplete and not ignored 
					rmax = std::min(rmax,
						std::max(jp->latest_arrival(),
							s.core_availability(jp->get_min_parallelism()).max()));
				}

				DM("         * No more future releases" << std::endl);
				return rmax;
			}

			// Create a new state st, try to merge st in an existing node. If the merge is unsuccessful, create a new node and add st
			Node& dispatch_wo_supernodes(const Node& n, const State& s, const Job<Time>& j,
				const Interval<Time> start_times, const Interval<Time> finish_times, const unsigned int ncores)
			{
				// update the set of scheduled jobs
				Job_set sched_jobs{ n.get_scheduled_jobs(), j.get_job_index() };

				// create a new state resulting from scheduling j in state s.
				State& st = new_state(s, j.get_job_index(), predecessors_of(j),
					start_times, finish_times, sched_jobs, successors, predecessors_suspensions, earliest_certain_gang_source_job_disptach(n, s, j), ncores);

				bool found_match = false;
				hash_value_t key = n.next_key(j);
#ifdef CONFIG_PARALLEL
				Nodes_map_accessor acc;
				while (true)
				{
					// check if key exists
					if (nodes_by_key.find(acc, key))
					{
						for (Node_ref other : acc->second)
						{
							if (other->get_scheduled_jobs() != sched_jobs)
								continue;

							// If we have reached here, it means that we have found an existing node with the same 
							// set of scheduled jobs than the new state resuting from scheduling job j in system state s.
							// Thus, our new state can be added to that existing node.
							if (other->merge_states(st, false))
							{
								delete& st;
								return *other;
							}
						}
					}
					else if (nodes_by_key.insert(acc, key))
						break;

					// if we raced with concurrent creation, try again
				}

				// if we reached here, we could not merge with an existing state and we have the lock on the hash map
				Node& next_node = new_node_at(acc, n, j, j.get_job_index(),
					earliest_possible_job_release(n, j),
					earliest_certain_source_job_release(n, j),
					earliest_certain_sequential_source_job_release(n, j));
#else
				const auto pair_it = nodes_by_key.find(key);
				if (pair_it != nodes_by_key.end())
				{
					for (Node_ref other : pair_it->second)
					{
						if (other->get_scheduled_jobs() != sched_jobs)
							continue;

						// If we have reached here, it means that we have found an existing node with the same 
						// set of scheduled jobs than the new state resuting from scheduling job j in system state s.
						// Thus, our new state can be added to that existing node.
						if (other->merge_states(st, false))
						{
							delete& st;
							return *other;
						}
					}
				}

				Node& next_node = new_node(n, j, j.get_job_index(),
					earliest_possible_job_release(n, j),
					earliest_certain_source_job_release(n, j),
					earliest_certain_sequential_source_job_release(n, j));
#endif

				next_node.add_state(&st);
				num_states++;
				return next_node;
			}

			bool dispatch(const Node& n, const Job<Time>& j, Time t_wc_wos, Time t_high_wos)
			{
				// All states in node 'n' for which the job 'j' is eligible will 
				// be added to that same node. 
				// If such a node already exists, we keep a reference to it
				Node_ref next = nullptr;
				DM("--- global:dispatch() " << n << ", " << j << ", " << t_wc_wos << ", " << t_high_wos << std::endl);

				bool dispatched_one = false;

				// loop over all states in the node n
				const auto* n_states = n.get_states();
#ifdef CONFIG_PARALLEL
				Nodes_map_accessor acc;
#endif
				for (State* s : *n_states)
				{
					// check for all possible parallelism levels of the moldable gang job j (if j is not gang or not moldable than min_paralellism = max_parallelism).
					for (unsigned int p = j.get_max_parallelism(); p >= j.get_min_parallelism(); p--)
					{
						// Calculate t_wc and t_high
						Time t_wc = std::max(s->core_availability(p).max(), next_certain_job_ready_time(n, *s));

						Time t_high_succ = next_certain_higher_priority_successor_job_ready_time(n, *s, j, p, t_wc + 1);
						Time t_high_gang = next_certain_higher_priority_gang_source_job_ready_time(n, *s, j, p, t_wc + 1);
						Time t_high = std::min(t_high_wos, std::min(t_high_gang, t_high_succ));

						// If j can execute on ncores+k cores, then 
						// the scheduler will start j on ncores only if 
						// there isn't ncores+k cores available
						Time t_avail = Time_model::constants<Time>::infinity();
						if (p < j.get_max_parallelism())
							t_avail = s->core_availability(j.get_next_parallelism(p)).max();

						DM("=== t_high = " << t_high << ", t_wc = " << t_wc << std::endl);
						auto _st = start_times(*s, j, t_wc, t_high, t_avail, p);
						if (_st.first > t_wc || _st.first >= t_high || _st.first >= t_avail)
							continue; // nope, not next job that can be dispatched in state s, try the next state.

						//calculate the job finish time interval
						Interval<Time> ftimes;
						auto exec_time = j.get_cost(p);
						Time eft = _st.first + exec_time.min();
						Time lft = _st.second + exec_time.max();

						// check for possible abort actions
						auto j_idx = j.get_job_index();
						if (abort_actions[j_idx]) {
							auto lt = abort_actions[j_idx]->latest_trigger_time();
							// Rule: if we're certainly past the trigger, the job is
							//       completely skipped.
							if (_st.first >= lt) {
								// job doesn't even start, it is skipped immediately
								ftimes = Interval<Time>{ _st };
							}
							else {
								// The job can start its execution but we check
								// if the job must be aborted before it finishes
								auto eat = earliest_job_abortion(*abort_actions[j_idx]);
								auto lat = latest_job_abortion(*abort_actions[j_idx]);
								ftimes = Interval<Time>{ std::min(eft, eat), std::min(lft, lat) };
							}
						}
						else {
							// compute range of possible finish times
							ftimes = Interval<Time>{ eft, lft };
						}

						// yep, job j is a feasible successor in state s
						dispatched_one = true;						

						// update finish-time estimates
						update_finish_times(j, ftimes);

						if (use_supernodes == false)
						{
							next = &(dispatch_wo_supernodes(n, *s, j, Interval<Time>{_st}, ftimes, p));
						}
						else
						{
#ifdef CONFIG_PARALLEL
							// if we do not have a pointer to a node with the same set of scheduled job yet,
							// try to find an existing node with the same set of scheduled jobs. Otherwise, create one.
							if (next == nullptr || acc.empty())
							{
								auto next_key = n.next_key(j);
								Job_set new_sched_jobs{ n.get_scheduled_jobs(), j.get_job_index() };

								while (next == nullptr || acc.empty()) {
									// check if key exists
									if (nodes_by_key.find(acc, next_key)) {
										// If be_naive, a new node and a new state should be created for each new job dispatch.
										if (be_naive) {
											next = &(new_node_at(acc, n, j, j.get_job_index(), earliest_possible_job_release(n, j), earliest_certain_source_job_release(n, j), earliest_certain_sequential_source_job_release(n, j)));
										}
										else
										{
											for (Node_ref other : acc->second) {
												if (other->get_scheduled_jobs() == new_sched_jobs) {
													next = other;
													DM("=== dispatch: next exists." << std::endl);
													break;
												}
											}
											if (next == nullptr) {
												next = &(new_node_at(acc, n, j, j.get_job_index(), earliest_possible_job_release(n, j), earliest_certain_source_job_release(n, j), earliest_certain_sequential_source_job_release(n, j)));
											}
										}
									}
									if (next == nullptr) {
										if (nodes_by_key.insert(acc, next_key)) {
											next = &(new_node_at(acc, n, j, j.get_job_index(), earliest_possible_job_release(n, j), earliest_certain_source_job_release(n, j), earliest_certain_sequential_source_job_release(n, j)));
										}
									}
									// if we raced with concurrent creation, try again
								}
							}
							// If be_naive, a new node and a new state should be created for each new job dispatch.
							else if (be_naive) {
								// note that the accessor should be pointing on something at this point
								next = &(new_node_at(acc, n, j, j.get_job_index(), earliest_possible_job_release(n, j), earliest_certain_source_job_release(n, j), earliest_certain_sequential_source_job_release(n, j)));
							}
							assert(!acc.empty());
#else
							// If be_naive, a new node and a new state should be created for each new job dispatch.
							if (be_naive)
								next = &(new_node(n, j, j.get_job_index(), earliest_possible_job_release(n, j), earliest_certain_source_job_release(n, j), earliest_certain_sequential_source_job_release(n, j)));

							// if we do not have a pointer to a node with the same set of scheduled job yet,
							// try to find an existing node with the same set of scheduled jobs. Otherwise, create one.
							if (next == nullptr)
							{
								const auto pair_it = nodes_by_key.find(n.next_key(j));
								if (pair_it != nodes_by_key.end()) {
									Job_set new_sched_jobs{ n.get_scheduled_jobs(), j.get_job_index() };
									for (Node_ref other : pair_it->second) {
										if (other->get_scheduled_jobs() == new_sched_jobs)
										{
											next = other;
											DM("=== dispatch: next exists." << std::endl);
											break;
										}
									}
								}
								// If there is no node yet, create one.
								if (next == nullptr)
									next = &(new_node(n, j, j.get_job_index(), earliest_possible_job_release(n, j), earliest_certain_source_job_release(n, j), earliest_certain_sequential_source_job_release(n, j)));
							}
#endif
							// next should always exist at this point, possibly without states in it
							// create a new state resulting from scheduling j in state s on p cores and try to merge it with an existing state in node 'next'.							
							new_or_merge_state(*next, *s, j.get_job_index(), predecessors_of(j),
								Interval<Time>{_st}, ftimes, next->get_scheduled_jobs(), successors, predecessors_suspensions, earliest_certain_gang_source_job_disptach(n, *s, j), p);

							// make sure we didn't skip any jobs which would then certainly miss its deadline
							// only do that if we stop the analysis when a deadline miss is found 
							if (be_naive && early_exit) {
								check_for_deadline_misses(n, *next);
							}
						}

#ifdef CONFIG_COLLECT_SCHEDULE_GRAPH
						edges.emplace_back(&j, &n, next, ftimes);
#endif
						count_edge();
					}
				}

				// if we stop the analysis when a deadline miss is found, then check whether a job will certainly miss 
				// its deadline because of when the processors become free next.
				// if we are not using the naive exploration, we check for deadline misses only once per job dispatched
				if (early_exit && !be_naive && next != nullptr)
					check_for_deadline_misses(n, *next);

				return dispatched_one;
			}

			void explore(const Node& n)
			{
				bool found_one = false;

				DM("---- global:explore(node)" << n.finish_range() << std::endl);

				// (0) define the window of interest
				auto t_min = n.earliest_job_release();
				// latest time some unfinished job is certainly ready
				auto nxt_ready_job = n.next_certain_job_ready_time();
				// latest time all cores are certainly available
				auto avail_max = n.latest_core_availability();
				// latest time by which a work-conserving scheduler
				// certainly schedules some job
				auto upbnd_t_wc = std::max(avail_max, nxt_ready_job);

				DM(n << std::endl);
				DM("t_min: " << t_min << std::endl
					<< "nxt_ready_job: " << nxt_ready_job << std::endl
					<< "avail_max: " << avail_max << std::endl
					<< "upbnd_t_wc: " << upbnd_t_wc << std::endl);

				//check all jobs that may be eligible to be dispatched next
				for (auto it = jobs_by_earliest_arrival.lower_bound(t_min);
					it != jobs_by_earliest_arrival.end();
					it++)
				{
					const Job<Time>& j = *it->second;
					DM(j << " (" << index_of(j) << ")" << std::endl);
					// stop looking once we've left the window of interest
					if (j.earliest_arrival() > upbnd_t_wc)
						break;

					// Job could be not ready due to precedence constraints
					if (!ready(n, j))
						continue;

					// Since this job is released in the future, it better
					// be incomplete...
					assert(unfinished(n, j));

					Time t_high_wos = next_certain_higher_priority_seq_source_job_release(n, j, upbnd_t_wc + 1);
					// if there is a higher priority job that is certainly ready before job j is released at the earliest, 
					// then j will never be the next job dispached by the scheduler
					if (t_high_wos <= j.earliest_arrival())
						continue;
					found_one |= dispatch(n, j, upbnd_t_wc, t_high_wos);
				}

				// check for a dead end
				if (!found_one && !all_jobs_scheduled(n)) {
					// out of options and we didn't schedule all jobs
					observed_deadline_miss = true;
					aborted = true;
				}
			}

			// naive: no state merging
			void explore_naively()
			{
				be_naive = true;
				explore();
			}

			void explore()
			{
				make_initial_node(num_cpus);

				while (current_job_count < jobs.size()) {
					unsigned long n;
#ifdef CONFIG_PARALLEL
					const auto& new_nodes_part = nodes_storage.back();
					n = 0;
					for (const Nodes& new_nodes : new_nodes_part) {
						n += new_nodes.size();
					}
#else
					Nodes& exploration_front = nodes();
					n = exploration_front.size();
#endif
					if (n == 0)
					{
						aborted = true;
						break;
					}
					// allocate node space for next depth
					nodes_storage.emplace_back();

					// keep track of exploration front width
					width = std::max(width, n);

					check_depth_abort();
					check_cpu_timeout();
					if (aborted)
						break;

#ifdef CONFIG_PARALLEL

					parallel_for(new_nodes_part.range(),
						[&](typename Split_nodes::const_range_type& r) {
							for (auto it = r.begin(); it != r.end(); it++) {
								const Nodes& new_nodes = *it;
								auto s = new_nodes.size();
								tbb::parallel_for(tbb::blocked_range<size_t>(0, s),
									[&](const tbb::blocked_range<size_t>& r) {
										for (size_t i = r.begin(); i != r.end(); i++)
											explore(new_nodes[i]);
									});
							}
						});

#else
					for (const Node& n : exploration_front) {
						explore(n);
						check_cpu_timeout();
						if (aborted)
							break;
					}
#endif

					// clean up the state cache if necessary
					if (!be_naive)
						nodes_by_key.clear();

					current_job_count++;

#ifndef CONFIG_COLLECT_SCHEDULE_GRAPH
					// If we don't need to collect all nodes, we can remove
					// all those that we are done with, which saves a lot of
					// memory.
#ifdef CONFIG_PARALLEL
					parallel_for(nodes_storage.front().range(),
						[](typename Split_nodes::range_type& r) {
							for (auto it = r.begin(); it != r.end(); it++)
								it->clear();
						});
#endif
					nodes_storage.pop_front();
#endif

				}

#ifdef CONFIG_PARALLEL
				// propagate any updates to the response-time estimates
				for (auto& r : partial_rta) {
					for (int i = 0; i < r.size(); ++i) {
						if (r[i].valid)
							update_finish_times(rta, i, r[i].rt);
					}
				}
#endif


#ifndef CONFIG_COLLECT_SCHEDULE_GRAPH
				// clean out any remaining nodes
				while (!nodes_storage.empty()) {
#ifdef CONFIG_PARALLEL
					parallel_for(nodes_storage.front().range(),
						[](typename Split_nodes::range_type& r) {
							for (auto it = r.begin(); it != r.end(); it++)
								it->clear();
						});
#endif
					nodes_storage.pop_front();
				}
#endif


#ifdef CONFIG_PARALLEL
				for (auto& c : edge_counter)
					num_edges += c;
#endif
			}


#ifdef CONFIG_COLLECT_SCHEDULE_GRAPH
			friend std::ostream& operator<< (std::ostream& out,
				const State_space<Time>& space)
			{
				std::map<const Schedule_node<Time>*, unsigned int> node_id;
				unsigned int i = 0;
				out << "digraph {" << std::endl;
#ifdef CONFIG_PARALLEL
				for (const Split_nodes& nodes : space.get_nodes()) {
					for (const Schedule_node<Time>& n : tbb::flattened2d<Split_nodes>(nodes)) {
#else
				for (const auto& front : space.get_nodes()) {
					for (const Schedule_node<Time>& n : front) {
#endif
						/*node_id[&n] = i++;
						out << "\tS" << node_id[&n]
							<< "[label=\"S" << node_id[&n] << ": ";
						n.print_vertex_label(out, space.jobs);
						out << "\"];" << std::endl;*/
						node_id[&n] = i++;
						out << "\tN" << node_id[&n]
							<< "[label=\"N" << node_id[&n] << ": {";
						const auto* n_states = n.get_states();

						for (State* s : *n_states)
						{
							out << "[";
							s->print_vertex_label(out, space.jobs);
							//<< s->earliest_finish_time()
							//<< ", "
							//<< s->latest_finish_time()
							out << "]\\n";
						}
						out << "}"
							<< "\\nER=";
						if (n.earliest_job_release() ==
							Time_model::constants<Time>::infinity()) {
							out << "N/A";
						}
						else {
							out << n.earliest_job_release();
						}
						out << "\"];"
							<< std::endl;
					}
				}
				for (const auto& e : space.get_edges()) {
					out << "\tN" << node_id[e.source]
						<< " -> "
						<< "N" << node_id[e.target]
						<< "[label=\""
						<< "T" << e.scheduled->get_task_id()
						<< " J" << e.scheduled->get_job_id()
						<< "\\nDL=" << e.scheduled->get_deadline()
						<< "\\nES=" << e.earliest_start_time()
						<< "\\nLS=" << e.latest_start_time()
						<< "\\nEF=" << e.earliest_finish_time()
						<< "\\nLF=" << e.latest_finish_time()
						<< "\"";
					if (e.deadline_miss_possible()) {
						out << ",color=Red,fontcolor=Red";
					}
					out << ",fontsize=8" << "]"
						<< ";"
						<< std::endl;
					if (e.deadline_miss_possible()) {
						out << "N" << node_id[e.target]
							<< "[color=Red];"
							<< std::endl;
					}
				}
				out << "}" << std::endl;
				return out;
					}
#endif
				};

			}
		}

namespace std
{
	template<class Time> struct hash<NP::Global::Schedule_state<Time>>
	{
		std::size_t operator()(NP::Global::Schedule_state<Time> const& s) const
		{
			return s.get_key();
		}
	};
}


#endif
