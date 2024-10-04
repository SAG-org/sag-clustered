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

#include "global/node.hpp"

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
			typedef Cluster_state<Time> Clstr_state;
			typedef typename std::vector<Interval<Time>> CoreAvailability;

			typedef Schedule_node<Time> Node;

			static State_space* explore(
				const Problem& prob,
				const Analysis_options& opts)
			{
				State_space* s = new State_space(prob.jobs, prob.prec, prob.aborts, prob.num_processors,
					opts.timeout, opts.max_depth, opts.early_exit);
				s->be_naive = opts.be_naive;
				s->cpu_time.start();
				s->explore();
				s->cpu_time.stop();
				return s;
			}

			// convenience interface for tests
			static State_space* explore_naively(
				const Workload& jobs,
				const std::vector<unsigned int>& num_cpus = { 1 })
			{
				Problem p{ jobs, num_cpus };
				Analysis_options o;
				o.be_naive = true;
				return explore(p, o);
			}

			// convenience interface for tests
			static State_space* explore_naively(
				const Workload& jobs,
				const unsigned int num_cpus)
			{
				Problem p{ jobs, {num_cpus} };
				Analysis_options o;
				o.be_naive = true;
				return explore(p, o);
			}

			// convenience interface for tests
			static State_space* explore(
				const Workload& jobs,
				const std::vector<unsigned int>& num_cpus = { 1 })
			{
				Problem p{ jobs, num_cpus };
				Analysis_options o;
				return explore(p, o);
			}

			// convenience interface for tests
			static State_space* explore(
				const Workload& jobs,
				const unsigned int num_cpus)
			{
				Problem p{ jobs, {num_cpus} };
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
			typedef std::vector<Nodes> Nodes_storage;
#endif

#ifdef CONFIG_COLLECT_SCHEDULE_GRAPH

			struct Edge {
				const std::vector<const Job<Time>*> scheduled;
				const Node* source;
				const Node* target;
				const std::vector<Interval<Time>>  finish_range;
				const std::vector<unsigned int> parallelism;

				Edge(const std::vector<const Job<Time>*>& s, const Node* src, const Node* tgt,
					const std::vector<Interval<Time>>& fr, const std::vector<unsigned int> parallelism)
					: scheduled(s)
					, source(src)
					, target(tgt)
					, finish_range(fr)
					, parallelism(parallelism)
				{
				}

				bool deadline_miss_possible() const
				{
					bool deadline_miss = false;
					for (int i = 0; i < scheduled.size(); i++) {
						deadline_miss = deadline_miss || scheduled[i]->exceeds_deadline(finish_range[i].upto());
					}
					return deadline_miss;
				}

				Time earliest_finish_time(int i) const
				{
					return finish_range[i].from();
				}

				Time latest_finish_time(int i) const
				{
					return finish_range[i].upto();
				}

				Time earliest_start_time(int i) const
				{
					return finish_range[i].from() - scheduled[i]->least_exec_time();
				}

				Time latest_start_time(int i) const
				{
					return finish_range[i].upto() - scheduled[i]->maximal_exec_time();
				}

				unsigned int parallelism_level(int i) const
				{
					return parallelism[i];
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
			typedef std::unordered_map<std::pair<unsigned int, hash_value_t>, Node_refs> Nodes_map;
#endif


			typedef const Job<Time>* Job_ref;
			typedef std::multimap<Time, Job_ref> By_time_map;
			struct Time_bounds {
				Time t_high_upbnd;
				Time earliest_next_release;
				Time latest_next_source_job_release;
				Time latest_next_seq_source_job_release;
			};
			typedef std::pair<Job_ref, Time_bounds> Job_with_time_bounds;
			typedef std::deque<Job_with_time_bounds> Set_of_jobs_and_bounds; // set of jobs with associated timing bounds 

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
			std::vector<By_time_map> _successor_jobs_by_latest_arrival_by_cluster;
			std::vector<By_time_map> _sequential_source_jobs_by_latest_arrival_by_cluster;
			std::vector<By_time_map> _gang_source_jobs_by_latest_arrival_by_cluster;
			std::vector<By_time_map> _jobs_by_earliest_arrival_by_cluster;
			std::vector<By_time_map> _jobs_by_deadline;
			std::vector<Job_precedence_set> _predecessors;

			// use these const references to ensure read-only access
			const std::vector<By_time_map>& successor_jobs_by_latest_arrival_by_cluster;
			const std::vector<By_time_map>& sequential_source_jobs_by_latest_arrival_by_cluster;
			const std::vector<By_time_map>& gang_source_jobs_by_latest_arrival_by_cluster;
			const std::vector<By_time_map>& jobs_by_earliest_arrival_by_cluster;
			const std::vector<By_time_map>& jobs_by_deadline;
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
			const unsigned int num_clusters;
			const std::vector<unsigned int> num_cpus;

			State_space(const Workload& jobs,
				const Precedence_constraints& edges,
				const Abort_actions& aborts,
				const std::vector<unsigned int>& num_cpus,
				double max_cpu_time = 0,
				unsigned int max_depth = 0,
				bool early_exit = true)
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
				, num_clusters(num_cpus.size())
				, num_cpus(num_cpus)
				, _successor_jobs_by_latest_arrival_by_cluster(num_cpus.size())
				, _sequential_source_jobs_by_latest_arrival_by_cluster(num_cpus.size())
				, _gang_source_jobs_by_latest_arrival_by_cluster(num_cpus.size())
				, _jobs_by_earliest_arrival_by_cluster(num_cpus.size())
				, _jobs_by_deadline(num_cpus.size())
				, successor_jobs_by_latest_arrival_by_cluster(_successor_jobs_by_latest_arrival_by_cluster)
				, sequential_source_jobs_by_latest_arrival_by_cluster(_sequential_source_jobs_by_latest_arrival_by_cluster)
				, gang_source_jobs_by_latest_arrival_by_cluster(_gang_source_jobs_by_latest_arrival_by_cluster)
				, jobs_by_earliest_arrival_by_cluster(_jobs_by_earliest_arrival_by_cluster)
				, jobs_by_deadline(_jobs_by_deadline)
				, _predecessors(jobs.size())
				, predecessors(_predecessors)
				, _predecessors_suspensions(jobs.size())
				, _successors(jobs.size())
				, predecessors_suspensions(_predecessors_suspensions)
				, successors(_successors)
				, early_exit(early_exit)
				, abort_actions(jobs.size(), NULL)
#ifdef CONFIG_COLLECT_SCHEDULE_GRAPH
				, nodes_storage(jobs.size() + 1)
#else
				, nodes_storage(num_cpus.size() + 1)
#endif
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
						_successor_jobs_by_latest_arrival_by_cluster[j.get_affinity()].insert({ j.latest_arrival(), &j });
					}
					else if (j.get_min_parallelism() == 1) {
						_sequential_source_jobs_by_latest_arrival_by_cluster[j.get_affinity()].insert({ j.latest_arrival(), &j });
					}
					else {
						_gang_source_jobs_by_latest_arrival_by_cluster[j.get_affinity()].insert({ j.latest_arrival(), &j });
					}
					_jobs_by_earliest_arrival_by_cluster[j.get_affinity()].insert({ j.earliest_arrival(), &j });
					_jobs_by_deadline[j.get_affinity()].insert({ j.get_deadline(), &j });
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
				for(int i=0; i<num_clusters; i++) {
					auto check_from = old_n.finish_range(i).min();

					// check if we skipped any jobs that are now guaranteed
					// to miss their deadline
					for (auto it = jobs_by_deadline[i].lower_bound(check_from);
						it != jobs_by_deadline[i].end(); it++) 
					{
						const Job<Time>& j = *(it->second);
						auto pmin = j.get_min_parallelism();
						auto latest = new_n.finish_range(i).max();
						if (j.get_deadline() < latest) {
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
									auto frange = new_n.finish_range(i) + j.get_cost(pmin);
									Node& next =
										new_node(1, new_n, j, j.get_job_index(), 0, 0, 0);
									//const CoreAvailability empty_cav = {};
									State& next_s = new_state(*(new_n.get_states()->front()), j, predecessors_of(j), frange, frange, new_n.get_scheduled_jobs(), successors, predecessors_suspensions, 0, pmin);
									next.add_state(&next_s);
									num_states++;

									// update response times
									update_finish_times(j, frange);
	#ifdef CONFIG_COLLECT_SCHEDULE_GRAPH
									std::vector<Job_ref> missed_job(num_clusters, NULL);
									std::vector<Interval<Time>> ftimes(num_clusters, { 0,0 });
									std::vector<unsigned int> p(num_clusters, 0);
									missed_job[j.get_affinity()] = &j;
									ftimes[j.get_affinity()] = frange;
									p[j.get_affinity()] = pmin;
									edges.emplace_back(missed_job, &new_n, &next, ftimes, p);
	#endif
									count_edge();
								}
								break;
							}
						}
						else
							// deadlines now after the next latest finish time
							break;
					}
				}
			}

			void make_initial_node()
			{
				// construct initial state
				std::vector<Time> next_certain_seq_release_per_cluster (num_clusters, Time_model::constants<Time>::infinity());
				std::vector<Time> next_certain_gang_release_per_cluster (num_clusters, Time_model::constants<Time>::infinity());
				std::vector<Time> next_earliest_release_per_cluster(num_clusters, Time_model::constants<Time>::infinity());
				std::vector<Time> next_certain_release_per_cluster;
				next_certain_release_per_cluster.resize(num_clusters);				

				for (int i = 0; i < num_clusters; i++) {
					if (!sequential_source_jobs_by_latest_arrival_by_cluster[i].empty())
						next_certain_seq_release_per_cluster[i] = sequential_source_jobs_by_latest_arrival_by_cluster[i].begin()->first;
					if (!gang_source_jobs_by_latest_arrival_by_cluster[i].empty())
						next_certain_gang_release_per_cluster[i] = gang_source_jobs_by_latest_arrival_by_cluster[i].begin()->first;
					next_certain_release_per_cluster[i] = std::min(next_certain_seq_release_per_cluster[i], next_certain_gang_release_per_cluster[i]);
					if (!jobs_by_earliest_arrival_by_cluster[i].empty())
						next_earliest_release_per_cluster[i] = jobs_by_earliest_arrival_by_cluster[i].begin()->first;
				}

				Node& n = new_node(0, num_cpus, next_earliest_release_per_cluster, next_certain_release_per_cluster, next_certain_seq_release_per_cluster);
				State& s = new_state(num_cpus, next_certain_gang_release_per_cluster);
				n.add_state(&s);
				num_states++;
			}

			Nodes& nodes(const int depth=0)
			{
#ifdef CONFIG_PARALLEL
				return nodes_storage.back().local();
#else
				size_t layer = (current_job_count + depth) % nodes_storage.size();
				return nodes_storage[layer];
#endif
			}

			template <typename... Args>
			Node_ref alloc_node(const int depth, Args&&... args)
			{
				Nodes& n_storage = nodes(depth);
				n_storage.emplace_back(std::forward<Args>(args)...);
				Node_ref n = &(*(--n_storage.end()));

				// make sure we didn't screw up...
				assert(
					(n->number_of_scheduled_jobs() ==0 && num_states == 0) // initial state
					|| (n->number_of_scheduled_jobs() > current_job_count && num_states > 0) // normal State
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
			warning  "Parallel code is not updated for clusters."

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
			Node& new_node(const int n_jobs_dispatched, Args&&... args)
			{
				Node_ref n = alloc_node(n_jobs_dispatched, std::forward<Args>(args)...);
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

			// Check wether a job is ready (not dispatched yet and all its predecessors are completed).
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
				unsigned int affinity = j.get_affinity();
				const auto& cs = s.cluster(affinity);
				Time avail_min = cs.earliest_finish_time();
				Interval<Time> r = j.arrival_window();

				// if the minimum parallelism of j is more than ncores, then 
				// for j to be released and have its successors completed 
				// is not enough to interfere with a lower priority job.
				// It must also have enough cores free.
				if (j.get_min_parallelism() > ncores)
				{
					// max {rj_max,Amax(sjmin)}
					r.extend_to(cs.core_availability(j.get_min_parallelism()).max());
				}

				for (const auto& pred : predecessors_suspensions[j.get_job_index()])
				{
					auto pred_idx = pred.first->get_job_index();
					// skip if part of disregard
					if (contains(disregard, pred_idx))
						continue;

					// if the predecessor and the job j arre assigned on the same cluster and
					// there is no suspension time and there is a single core in the cluster, then
					// predecessors are finished as soon as the processor becomes available
					auto pred_susp = pred.second;
					unsigned int aff_pred = pred.first->get_affinity();
					if (aff_pred == affinity && num_cpus[affinity] == 1 && pred_susp.max() == 0)
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
				auto cluster_id = reference_job.get_affinity();

				Time when = until;

				// a higher priority source job cannot be released before 
				// a source job of any priority is released
				Time t_earliest = n.get_next_certain_source_job_release(cluster_id);

				for (auto it = sequential_source_jobs_by_latest_arrival_by_cluster[cluster_id].lower_bound(t_earliest);
					it != sequential_source_jobs_by_latest_arrival_by_cluster[cluster_id].end(); it++)
				{
					const Job<Time>& j = *(it->second);

					// check if we can stop looking
					if (when < j.latest_arrival())
						break; // yep, nothing can lower 'when' at this point

					// j is not relevant if it is already scheduled or not of higher priority
					if (j.higher_priority_than(reference_job) && ready(n, j))
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
				auto cluster_id = reference_job.get_affinity();
				const auto& cs = s.cluster(cluster_id);

				Time when = until;

				// a higher priority source job cannot be released before 
				// a source job of any priority is released
				Time t_earliest = n.get_next_certain_source_job_release(cluster_id);

				for (auto it = gang_source_jobs_by_latest_arrival_by_cluster[cluster_id].lower_bound(t_earliest);
					it != gang_source_jobs_by_latest_arrival_by_cluster[cluster_id].end(); it++)
				{
					const Job<Time>& j = *(it->second);

					// check if we can stop looking
					if (when < j.latest_arrival())
						break; // yep, nothing can lower 'when' at this point

					// j is not relevant if it is already scheduled or not of higher priority
					if (j.higher_priority_than(reference_job) && ready(n, j))
					{
						// if the minimum parallelism of j is more than ncores, then 
						// for j to be released and have its successors completed 
						// is not enough to interfere with a lower priority job.
						// It must also have enough cores free.
						if (j.get_min_parallelism() > ncores)
						{
							// max {rj_max,Amax(sjmin)}
							when = std::min(when, std::max(j.latest_arrival(), cs.core_availability(j.get_min_parallelism()).max()));
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
				auto cluster_id = reference_job.get_affinity();

				auto ready_min = earliest_ready_time(s, reference_job);
				Time when = until;

				// a higer priority successor job cannot be ready before 
				// a job of any priority is released
				Time t_earliest = n.earliest_job_release(cluster_id);
				for (auto it = successor_jobs_by_latest_arrival_by_cluster[cluster_id].lower_bound(t_earliest);
					it != successor_jobs_by_latest_arrival_by_cluster[cluster_id].end(); it++)
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
			/*Time next_certain_job_ready_time(const Node& n, const Clstr_state& s) const
			{
				//TODO: may have to account for the number of available cores in a state
				Time t_ws = std::min(s.next_certain_gang_source_job_disptach(), s.next_certain_successor_jobs_disptach());
				Time t_wos = n.get_next_certain_sequential_source_job_release();
				return std::min(t_wos, t_ws);
			}*/

			// find next time by which a job is certainly ready in system state 's' on cluster 'cluster_id'
			Time next_certain_job_ready_time(const Node& n, const State& s, const unsigned int cluster_id) const
			{
				const auto& cs = s.cluster(cluster_id);

				//TODO: may have to account for the number of available cores in a state
				Time t_ws = std::min(cs.next_certain_gang_source_job_disptach(), cs.next_certain_successor_jobs_disptach());
				Time t_wos = n.get_next_certain_sequential_source_job_release(cluster_id);
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
				const auto& cs = s.cluster(j.get_affinity());
				auto rt = earliest_ready_time(s, j);
				auto at = cs.core_availability(ncores).min();
				Time est = std::max(rt, at);

				DM("rt: " << rt << std::endl
					<< "at: " << at << std::endl);

				Time lst = std::min(t_wc,
					std::min(t_high, t_avail) - Time_model::constants<Time>::epsilon());

				DM("est: " << est << std::endl);
				DM("lst: " << lst << std::endl);

				return { est, lst };
			}

			// Find the earliest possible job release of all jobs on the same cluster than 'ignored_job' except for the ignored job
			Time earliest_possible_job_release(
				const Node& n,
				const Job<Time>& ignored_job)
			{
				DM("      - looking for earliest possible job release starting from: "
					<< n.earliest_job_release(cluster_id) << std::endl);

				unsigned int cluster_id = ignored_job.get_affinity();

				for (auto it = jobs_by_earliest_arrival_by_cluster[cluster_id].lower_bound(n.earliest_job_release(cluster_id));
					it != jobs_by_earliest_arrival_by_cluster[cluster_id].end(); 	it++)
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
			// on the same cluster as ignored_job except for the ignored job
			Time earliest_certain_sequential_source_job_release(
				const Node& n,
				const Job<Time>& ignored_job)
			{
				DM("      - looking for earliest certain source job release starting from: "
					<< n.get_next_certain_source_job_release(cluster_id) << std::endl);

				unsigned int cluster_id = ignored_job.get_affinity();

				for (auto it = sequential_source_jobs_by_latest_arrival_by_cluster[cluster_id].lower_bound(n.get_next_certain_source_job_release(cluster_id));
					it != sequential_source_jobs_by_latest_arrival_by_cluster[cluster_id].end(); it++)
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
			// on the same cluster as ignored_job except for the ignored job
			Time earliest_certain_source_job_release(
				const Node& n,
				const Job<Time>& ignored_job)
			{
				DM("      - looking for earliest certain source job release starting from: "
					<< n.get_next_certain_source_job_release(cluster_id) << std::endl);

				unsigned int cluster_id = ignored_job.get_affinity();

				Time rmax = earliest_certain_sequential_source_job_release(n, ignored_job);

				for (auto it = gang_source_jobs_by_latest_arrival_by_cluster[cluster_id].lower_bound(n.get_next_certain_source_job_release(cluster_id));
					it != gang_source_jobs_by_latest_arrival_by_cluster[cluster_id].end(); it++)
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
					<< n.get_next_certain_source_job_release(cluster_id) << std::endl);

				unsigned int cluster_id = ignored_job.get_affinity();

				Time rmax = Time_model::constants<Time>::infinity();

				for (auto it = gang_source_jobs_by_latest_arrival_by_cluster[cluster_id].lower_bound(n.get_next_certain_source_job_release(cluster_id));
					it != gang_source_jobs_by_latest_arrival_by_cluster[cluster_id].end(); it++)
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
							s.cluster(cluster_id).core_availability(jp->get_min_parallelism()).max()));
				}

				DM("         * No more future releases" << std::endl);
				return rmax;
			}
			
			struct Job_conf_and_timing_information {
				unsigned int parallelism;
				Interval<Time> start_time;
				Interval<Time> finish_time;
				Time earliest_next_release;
				Time latest_next_source_job_release;
				Time latest_next_seq_source_job_release;
				Time earliest_certain_gang_job_dispatch;
			};
			typedef std::vector<std::deque<std::pair<Job_ref, Job_conf_and_timing_information>>> Eligible_dispatch_conf; 

			inline void dispatch_independent_jobs(const Node& old_node, const State& old_state, const Eligible_dispatch_conf& disp_confs)
			{
				std::vector<Interval<Time>> stimes(num_clusters), ftimes(num_clusters);
				std::vector<Time> earliest_next_job_rel(num_clusters), next_source_job_rel(num_clusters), next_seq_source_job_rel(num_clusters), next_certain_gang_source_job_disptach(num_clusters);
				std::vector<unsigned int> n_cores(num_clusters);
				std::vector<Job_ref> dispatched_set(num_clusters);
				std::vector<Job_index> dispatched_idx(num_clusters, NULL_JOB_INDEX);

				dispatch_independent_jobs(old_node, old_state, disp_confs, NULL, dispatched_set, dispatched_idx, stimes, ftimes, n_cores,
					earliest_next_job_rel, next_source_job_rel, next_seq_source_job_rel, next_certain_gang_source_job_disptach, 0, 0);
			}

			void dispatch_independent_jobs(const Node& old_node, const State& old_state, const Eligible_dispatch_conf& disp_confs,
				Node* next_node, std::vector<Job_ref>& dispatched_set, std::vector<Job_index>& dispatched_idx, 
				std::vector<Interval<Time>>& stimes, std::vector<Interval<Time>>& ftimes, std::vector<unsigned int>& n_cores, 
				std::vector<Time>& next_job_rel, std::vector<Time>& next_source_job_rel, 
				std::vector<Time>& next_seq_source_job_rel, std::vector<Time>& next_certain_gang_source_job_disptach, 
				const unsigned int n_jobs_dispatched, const unsigned int cluster_id)
			{
				if (cluster_id == num_clusters) {
#ifdef CONFIG_PARALLEL							
#else
					// If be_naive, a new node and a new state should be created for each new job dispatch.
					if (be_naive) {
						std::vector<Job_index> idx_set;
						idx_set.reserve(n_jobs_dispatched);
						for (Job_index idx : dispatched_idx) {
							if (idx != NULL_JOB_INDEX)
								idx_set.push_back(idx);
						}
						next_node = &(new_node(n_jobs_dispatched, old_node, dispatched_set, idx_set, next_job_rel, next_source_job_rel, next_seq_source_job_rel));
					}
					else if (next_node == NULL) {
						std::vector<Job_index> idx_set;
						idx_set.reserve(n_jobs_dispatched);
						for (Job_index idx : dispatched_idx) {
							if (idx != NULL_JOB_INDEX)
								idx_set.push_back(idx);
						}

						const auto& pair_it = nodes_by_key.find(old_node.next_key(dispatched_set));
						if (pair_it != nodes_by_key.end()) {
							Dispatched_job_set new_sched_jobs(old_node.get_scheduled_jobs(), idx_set);
							for (Node_ref other : pair_it->second) {
								if (other->get_scheduled_jobs() == new_sched_jobs)
								{
									next_node = other;
									break;
								}
							}
						}
						// If there is no node yet, create one.
						if (next_node == NULL) {
							next_node = &(new_node(n_jobs_dispatched, old_node, dispatched_set, idx_set, next_job_rel, next_source_job_rel, next_seq_source_job_rel));
						}
					}					
#endif
					// next_node should always exist at this point, possibly without states in it
					// create a new state resulting from scheduling j in state s on p cores and try to merge it with an existing state in node 'next'.							
					new_or_merge_state(*next_node, old_state, dispatched_set, stimes, ftimes, n_cores,
						next_node->get_scheduled_jobs(), predecessors, successors, predecessors_suspensions, next_certain_gang_source_job_disptach);

					// make sure we didn't skip any jobs which would then certainly miss its deadline
					// only do that if we stop the analysis when a deadline miss is found 
					if (be_naive && early_exit) {
						check_for_deadline_misses(old_node, *next_node);
					}

#ifdef CONFIG_COLLECT_SCHEDULE_GRAPH
					edges.emplace_back(dispatched_set, &old_node, next_node, ftimes, n_cores);
#endif
					count_edge();
				}
				else if (disp_confs[cluster_id].empty()) {
					next_job_rel[cluster_id] = old_node.earliest_job_release(cluster_id);
					next_source_job_rel[cluster_id] = old_node.get_next_certain_source_job_release(cluster_id);
					next_seq_source_job_rel[cluster_id] = old_node.get_next_certain_sequential_source_job_release(cluster_id);
					next_certain_gang_source_job_disptach[cluster_id] = old_state.cluster(cluster_id).next_certain_gang_source_job_disptach();

					dispatch_independent_jobs(old_node, old_state, disp_confs, next_node, dispatched_set, dispatched_idx, stimes, ftimes, n_cores, next_job_rel,
						next_source_job_rel, next_seq_source_job_rel, next_certain_gang_source_job_disptach, n_jobs_dispatched, cluster_id + 1);
				}
				else {
					for (const auto& conf : disp_confs[cluster_id]) {
						dispatched_set[cluster_id] = conf.first;
						n_cores[cluster_id] = conf.second.parallelism;
						stimes[cluster_id] = conf.second.start_time;
						ftimes[cluster_id] = conf.second.finish_time;
						next_job_rel[cluster_id] = conf.second.earliest_next_release;
						next_source_job_rel[cluster_id] = conf.second.latest_next_source_job_release;
						next_seq_source_job_rel[cluster_id] = conf.second.latest_next_seq_source_job_release;
						next_certain_gang_source_job_disptach[cluster_id] = conf.second.earliest_certain_gang_job_dispatch;

						// if one of the jobs dispatched is different then a new node must be created (or found)
						if (dispatched_idx[cluster_id] != conf.first->get_job_index()) {
							dispatched_idx[cluster_id] = conf.first->get_job_index();
							next_node = NULL;
						}

						dispatch_independent_jobs(old_node, old_state, disp_confs, next_node, dispatched_set, dispatched_idx, stimes, ftimes, n_cores, next_job_rel,
							next_source_job_rel, next_seq_source_job_rel, next_certain_gang_source_job_disptach, n_jobs_dispatched + 1, cluster_id + 1);
					}
				}
			}

			bool explore_state(const Node& n, const Job_with_time_bounds& j, Time t_wc_wos)
			{
				DM("--- global:dispatch() " << n << ", " << j << ", " << t_wc_wos << ", " << t_high_wos << std::endl);
				std::vector<Set_of_jobs_and_bounds> j_set(num_clusters);
				std::vector<Time> t_wc(num_clusters);
				std::vector<bool> dispatch_on(num_clusters, false);
				auto affinity = j.first->get_affinity();
				j_set[affinity].push_back(j);
				t_wc[affinity] = t_wc_wos;
				dispatch_on[affinity] = true;
				return explore_states(n, j_set, dispatch_on, t_wc);
			}

			bool explore_states(const Node& n, const std::vector<Set_of_jobs_and_bounds>& eligible_jobs_per_cluster, const std::vector<bool>& dispatch_on, const std::vector<Time>& t_wc_wos)
			{
				bool dispatched_one = false;

				// loop over all states in the node n
				const auto* n_states = n.get_states();
				for (State* s : *n_states)
				{
					Eligible_dispatch_conf eligible_conf(num_clusters);
					bool found_eligible_conf = false;

					for (unsigned int affinity = 0; affinity < num_clusters; ++affinity) {
						if (dispatch_on[affinity] == false || eligible_jobs_per_cluster[affinity].empty())
							continue;
						
						const auto& cs = s->cluster(affinity);

						for (const auto& eligible_j : eligible_jobs_per_cluster[affinity]) {
							const Job_ref j = eligible_j.first;
							Time t_high_wos = eligible_j.second.t_high_upbnd;

							Time next_certain_gang_source_job_disptach = earliest_certain_gang_source_job_disptach(n, *s, *j);

							// check for all possible parallelism levels of the moldable gang job j (if j is not gang or not moldable than min_paralellism = max_parallelism).
							const auto& costs = j->get_all_costs();
							for (auto it = costs.rbegin(); it != costs.rend(); it++)
							{
								unsigned int p = it->first;
								// Calculate t_wc and t_high
								Time t_wc = std::max(cs.core_availability(p).max(), next_certain_job_ready_time(n, *s, affinity));

								Time t_high_succ = next_certain_higher_priority_successor_job_ready_time(n, *s, *j, p, t_wc + 1);
								Time t_high_gang = next_certain_higher_priority_gang_source_job_ready_time(n, *s, *j, p, t_wc + 1);
								Time t_high = std::min(t_high_wos, std::min(t_high_gang, t_high_succ));

								// If j can execute on ncores+k cores, then 
								// the scheduler will start j on ncores only if 
								// there isn't ncores+k cores available
								Time t_avail = Time_model::constants<Time>::infinity();
								if (p < j->get_max_parallelism())
									t_avail = cs.core_availability(std::prev(it)->first).max();

								DM("=== t_high = " << t_high << ", t_wc = " << t_wc << std::endl);
								auto _st = start_times(*s, *j, t_wc, t_high, t_avail, p);
								if (_st.first > t_wc || _st.first >= t_high || _st.first >= t_avail)
									continue; // nope, not next job that can be dispatched in state s, try the next state.

								//calculate the job finish time interval
								Interval<Time> ftimes;
								auto exec_time = it->second; // j->get_cost(p);
								Time eft = _st.first + exec_time.min();
								Time lft = _st.second + exec_time.max();

								// check for possible abort actions
								auto j_idx = j->get_job_index();
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
								found_eligible_conf = true;

								// update finish-time estimates
								update_finish_times(*j, ftimes);

								// save that 'j' can start executing with 'p' as its level of parallelism
								eligible_conf[affinity].emplace_back(j, 
									Job_conf_and_timing_information{p, Interval<Time>{_st}, ftimes, 
										eligible_j.second.earliest_next_release, eligible_j.second.latest_next_source_job_release, 
										eligible_j.second.latest_next_seq_source_job_release, next_certain_gang_source_job_disptach });
							}
						}
					}
					if (found_eligible_conf) {
						// dispatch all possible combinations of jobs and job parallelism on the different clusters
						dispatch_independent_jobs(n, *s, eligible_conf);
						dispatched_one = true;
					}
				}

				return dispatched_one;
			}

			void explore(const Node& n)
			{
				bool found_one = false;

				DM("---- global:explore(node)" << n.finish_range() << std::endl);

				// (0) define the time window of interest
				Time upbnd_t_wc_any = Time_model::constants<Time>::infinity();
				std::vector<Time> upbnd_t_wc_per_cluster;
				upbnd_t_wc_per_cluster.reserve(num_clusters);
				std::vector<Time> t_min;
				t_min.reserve(num_clusters);

				for (int i = 0; i < num_clusters; i++) {
					t_min.push_back(n.earliest_job_release(i));
					// latest time some unfinished job is certainly ready
					auto nxt_ready_job = n.next_certain_job_ready_time(i);
					// latest time all cores are certainly available
					auto avail_max = n.get_latest_core_availability(i);
					// latest time by which a work-conserving scheduler
					// certainly schedules some job
					upbnd_t_wc_per_cluster.push_back(std::max(avail_max, nxt_ready_job));
					upbnd_t_wc_any = std::min(upbnd_t_wc_any, upbnd_t_wc_per_cluster[i]);
				}

				std::vector<Set_of_jobs_and_bounds> eligible_jobs_per_cluster(num_clusters);
				std::vector<bool> is_independent(num_clusters, true);

				for (int cluster_id = 0; cluster_id < num_clusters; cluster_id++) {
					//check all jobs that may be eligible to be dispatched next
					for (auto it = jobs_by_earliest_arrival_by_cluster[cluster_id].lower_bound(t_min[cluster_id]);
						it != jobs_by_earliest_arrival_by_cluster[cluster_id].end();
						it++)
					{
						const Job<Time>& j = *it->second;
						DM(j << " (" << index_of(j) << ")" << std::endl);
						// stop looking once we've left the window of interest
						if (j.earliest_arrival() > upbnd_t_wc_per_cluster[cluster_id])
							break;

						// Job could be not ready due to precedence constraints
						if (ready(n, j)) {
							// Since this job is released in the future, it better
							// be incomplete...
							assert(unfinished(n, j));

							Time t_high_wos = next_certain_higher_priority_seq_source_job_release(n, j, upbnd_t_wc_per_cluster[cluster_id] + 1);
							// if there is a higher priority job that is certainly ready before job j is released at the earliest, 
							// then j will never be the next job dispached by the scheduler
							if (t_high_wos <= j.earliest_arrival())
								continue;
							
							// calculate lower and upper bounds on the next job releases if j gets dispatched
							Time earliest_next_job_rel = earliest_possible_job_release(n, j);
							Time latest_next_source_job_rel = earliest_certain_source_job_release(n, j);
							Time latest_next_seq_source_job_rel = earliest_certain_sequential_source_job_release(n, j);

							// add j to the list of eligible jobs together with relevant timing information
							eligible_jobs_per_cluster[cluster_id].emplace_back(&j, Time_bounds{ t_high_wos, earliest_next_job_rel, latest_next_source_job_rel, latest_next_seq_source_job_rel });
							found_one = true;
						}
						else if (is_independent[cluster_id] && n.job_dependent_on_other_cluster(j, predecessors_of(j), jobs, upbnd_t_wc_per_cluster[cluster_id])) {
							is_independent[cluster_id] = false;
						}
					}
				}
				// check for a dead end
				if (!found_one && !all_jobs_scheduled(n)) {
					// out of options and we didn't schedule all jobs
					observed_deadline_miss = true;
					aborted = true;
					return;
				}
				
				bool dispatched_one = false;
				// check whether there is at least one cluster that is independent of the others
				bool all_clusters_dependent = true;
				for (int i = 0; i < num_clusters; ++i) {
					if (is_independent[i] == true && !eligible_jobs_per_cluster[i].empty()) {
						all_clusters_dependent = false;
						break;
					}
				}
				// if some clusters are independent, dispatch jobs on those clusters
				if (!all_clusters_dependent)
					dispatched_one |= explore_states(n, eligible_jobs_per_cluster, is_independent, upbnd_t_wc_per_cluster);
				else 
				{
					for (int cluster_id = 0; cluster_id < num_clusters; cluster_id++) {
						// if the earliest time a job may start on the cluster  
						// is later than when a job certainly starts on any cluster, 
						// we do not dispatch anything on that cluster
						if (t_min[cluster_id] > upbnd_t_wc_any)
							continue;

						for (const auto& j : eligible_jobs_per_cluster[cluster_id]) {
							// if a job may start before any other job, we dispatch it
							if (j.first->earliest_arrival() <= upbnd_t_wc_any)
								dispatched_one |= explore_state(n, j, upbnd_t_wc_any);
						}
					}
				}

				// check for a dead end
				if (!dispatched_one && !all_jobs_scheduled(n)) {
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
				make_initial_node();

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
					// id there is no node to explore  at the current depth, 
					// check next depth until depth is equal to the number of jobs to schedule
					if (n == 0)
					{
						current_job_count++; // current_job_count is the depth of the graph we currently explore
						continue;
					}
					
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
					if (!be_naive) {
						// remove nodes in the current front in nodes_by_key
						for (const Node& n : exploration_front) {
							nodes_by_key.erase(n.get_key());
						}
					}

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
					exploration_front.clear();
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
				for (int i = 0; i < nodes_storage.size(); i++) {
#ifdef CONFIG_PARALLEL
					parallel_for(nodes_storage.front().range(),
						[](typename Split_nodes::range_type& r) {
							for (auto it = r.begin(); it != r.end(); it++)
								it->clear();
						});
#endif
					nodes_storage[i].clear();
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
							out << "]\\n";
						}
						out << "}"
							<< "\"];"
							<< std::endl;
					}
				}
				for (const auto& e : space.get_edges()) {
					out << "\tN" << node_id[e.source]
						<< " -> "
						<< "N" << node_id[e.target]
						<< "[label=\"";
					for (int i = 0; i < e.scheduled.size(); ++i) {
						out << "[T" << e.scheduled[i]->get_task_id()
							<< " J" << e.scheduled[i]->get_job_id()
							<< "\\nDL=" << e.scheduled[i]->get_deadline()
							<< "\\nES=" << e.earliest_start_time(i)
							<< "\\nLS=" << e.latest_start_time(i)
							<< "\\nEF=" << e.earliest_finish_time(i)
							<< "\\nLF=" << e.latest_finish_time(i)
							<< "]\n";
					}
					out << "\"";
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
