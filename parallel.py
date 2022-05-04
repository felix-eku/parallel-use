import argparse
import csv
from datetime import datetime, timedelta
from itertools import pairwise
from typing import Generator, Iterable, MutableSequence, NewType, Optional, Sequence, Type, TypeVar, Tuple

from attrs import define


date_format = "%Y%m%d %H:%M:%S"


OrderId = NewType('OrderId', int)
Seconds = NewType('Seconds', int)

O = TypeVar('O', bound='Order')
@define()
class Order:
    id: OrderId
    start: datetime
    duration: Seconds

    @classmethod
    def parse(cls: Type[O], row: Iterable[str]) -> O:
        row_iter = iter(row)
        id = OrderId(int(next(row_iter)))
        start = datetime.strptime(next(row_iter), date_format)
        duration = Seconds(int(next(row_iter)))
        return cls(id, start, duration)


def parse_input(input: str) -> list[Order]:
    """
    Parse orders from the csv-file `input`.

    Each row of `input` should contain the ID, preferred start time and duration of an order.
    """
    with open(input, newline='') as csvfile:
        csvreader = csv.reader(csvfile)
        return [Order.parse(row) for row in csvreader]


@define(init=False)
class Job:
    start: datetime | None
    orders: list[Order]
    duration: Seconds

    def __init__(self, orders: Iterable[Order], start: Optional[datetime] = None) -> None:
        self.start = start
        self.orders = sorted(orders, key=lambda order: order.start)
        self.duration = max((order.duration for order in self.orders), default=Seconds(0))

    def format(self) -> Generator[str, None, None]:
        assert self.start is not None, "Job needs to have a start date set."
        yield self.start.strftime(date_format)
        for order in self.orders:
            yield f"{order.id:d}"

    def check_orders_constraint(self, max_diff: timedelta) -> None:
        if self.orders and self.orders[-1].start - self.orders[0].start > max_diff:
            raise ValueError(
                f"The largest difference between the preferred starts of the orders is "
                f"{self.orders[-1].start - self.orders[0].start}, which is larger than {max_diff}.")


def naively_group_orders(orders: Iterable[Order], m: int, max_diff: timedelta) -> list[Job]:
    """
    Create a naive list of jobs.

    First sort `orders` by their preferred start.
    Try to suscessively take `m` orders from the front of the list of remaining orders
    and add them to the same job.
    If the restraint would be violated
    that the difference between the preferred start times of orders of the same job
    must be less than or equal to `max_diff`,
    take as many orders as possible, such that the restaint is still fulfilled.
    """
    orders = sorted(orders, key=lambda order: order.start)
    n = len(orders)

    jobs = list()  # type: list[Job]
    
    i = 0  # index of the first order included in the current job
    while i < n:
        j = min(i + m, n)  # index of the first excluded order
        latest_start = orders[i].start + max_diff
        while latest_start < orders[j-1].start:  # last included order is at j-1
            j -= 1
        jobs.append(Job(orders[i:j]))
        i = j

    return jobs


def group_orders_no_constraint(orders: Iterable[Order], m: int) -> list[Job]:
    """
    Group `orders` into jobs without considering the constraint.

    Return the list of jobs with the shortest possible makespan,
    if each Job can consist of up to `m` orders.
    """
    # The shortest makespan is obtained by grouping the m orders with the longest durations into a job
    # and then repeating this process for the remaining orders.
    orders = sorted(orders, key=lambda order: order.duration, reverse=True)
    return [Job(orders[i:i+m]) for i in range(0, len(orders), m)]


def group_orders(orders: Iterable[Order], m: int, max_diff: timedelta) -> list[Job]:
    """
    Group `orders` into jobs with up to `m` orders under the constraint 
    that the preferred start times of orders for the same job can at most differ by `max_diff`.

    First group orders into groups such that all orders in a group fulfill the constraint.
    Then use `group_orders_no_constraint` to further group these groups into jobs.
    """
    orders = sorted(orders, key=lambda order: order.start)
    jobs = list()  # type: list[Job]

    begin = 0
    while begin < len(orders):
        end = begin
        latest_start = orders[begin].start + max_diff
        if orders[-1].start <= latest_start:
            jobs.extend(group_orders_no_constraint(orders[begin:end], m))
            break
        while orders[end].start <= latest_start:
            end += 1
        if end - begin <= m:
            jobs.append(Job(orders[begin:end]))
        else:
            # Choose largest end such that the slice orders[begin:end] contains a multiple of m orders.
            end -= (end - begin) % m
            jobs.extend(group_orders_no_constraint(orders[begin:end], m))
        begin = end

    return jobs


def reduce_duration(job1: Job, job2: Job, m: int, max_diff: timedelta) -> Tuple[Job, Job] | Job | None:
    """
    Try to rearrange the orders between the jobs, such that their total duration decreases.

    Take into account that the maximum number of orders per job is `m`
    and that the preferred starts of orders of the same job can at most be `max_diff` apart.

    Return either a pair of jobs with shorter total duration or a single job if possible
    or None if the total duration cannot be decreased.
    """
    # The algorithm relies on the following observation:
    # The duration of the longer job is always the duration of the longest order.
    # Therefore the total duration of the jobs can only be decreased
    # by decreasing the duration of the shorter job.
    # To achieve this, at least the order with the longest duration of the shorter job
    # must be moved to the longer job (possibly exchanging it with an order with shorter duration).
    earliest_start = min(job1.orders[0].start, job2.orders[0].start)
    latest_start = max(job1.orders[-1].start, job2.orders[-1].start)
    if latest_start - earliest_start <= max_diff:
        # All orders can be arbitrarily moved between the two jobs
        # without violating the constraint.
        orders = job1.orders.copy()
        orders.extend(job2.orders)
        if len(orders) <= m:
            # All orders can be part of a single job.
            return Job(orders)
        orders.sort(key=lambda order: order.duration)
        if min(job1.duration, job2.duration) <= orders[-m-1].duration:
            # Rearranging the orders between jobs does not improve the total duration.
            return None
        # Grouping the m longest orders in one job results in the shortest duration for the other job.
        return Job(orders[:-m]), Job(orders[-m:])
    else:
        # Let job1 have the order with the earliest preferred start.
        # This implies that job2 has the order with the latest preferred start,
        # since the difference between earliest and latest preferred start is larger than k seconds
        # and therefore cannot occur between orders of the same job.
        if job1.orders[0].start > job2.orders[0].start:
            job1, job2 = job2, job1
        # Earliest preferred start of orders that could be part of job2.
        overlap_begin = latest_start - max_diff
        # Latest preferred start of orders that could be part of job1.
        overlap_end = earliest_start + max_diff
        if overlap_end < overlap_begin:
            # No orders can be rearranged between the jobs.
            return None
        # Linearly search for the indices of the orders that can be rearranged between the jobs.
        if job1.duration < job2.duration:
            for i, order in enumerate(job1.orders):
                if overlap_begin <= order.start:
                    begin = i
                    break
                elif order.duration >= job1.duration:
                    # The longest order of the shorter job cannot be moved to the other job.
                    # This implies that the total duration of the jobs cannot be reduced.
                    return None
            else:
                # This should be unreachable, 
                # since each job should contain an order that has the same duration as the job.
                return None
            for i, order in enumerate(reversed(job2.orders)):
                if order.start <= overlap_end:
                    end = len(job2.orders) - i  # end is exclusive and i counts from the end.
                    break
            else:
                if len(job2.orders) == m:
                    # No orders can be moved to the longer job.
                    return None
                end = 0
            # Number of (possible) orders in the longer job that can be rearranged.
            m_reduced = m - (len(job2.orders) - end)
        else:
            for i, order in enumerate(job1.orders):
                if overlap_begin <= order.start:
                    begin = i
                    break
            else:
                if len(job1.orders) == m:
                    # No orders can be moved to the longer job.
                    return None
                begin = len(job1.orders)
            for i, order in enumerate(reversed(job2.orders)):
                if order.start <= overlap_end:
                    end = len(job2.orders) - i  # end is exclusive and i counts from the end.
                    break
                elif order.duration >= job2.duration:
                    # The longest order of the shorter job cannot be moved to the other job.
                    # This implies that the total duration of the jobs cannot be reduced.
                    return None
            else:
                # This should be unreachable, 
                # since each job should contain an order that has the same duration as the job.
                return None
            # Number of (possible) orders in the longer job that can be rearranged.
            m_reduced = m - begin
        orders = job1.orders[begin:]
        orders.extend(job2.orders[:end])
        if len(orders) <= m_reduced:
            # All rearrangable orders can be part of the longer job.
            # This reduces the duration of the shorter job,
            # since the durations of the remaining orders of the shorter job are shorter than its duration.
            if job1.duration < job2.duration:
                orders.extend(job2.orders[end:])
                return Job(job1.orders[:begin]), Job(orders)
            else:
                orders.extend(job1.orders[:begin])
                return Job(orders), Job(job2.orders[end:])
        orders.sort(key=lambda order: order.duration)
        if job1.duration < job2.duration:
            if job1.duration <= orders[-m_reduced-1].duration:
                # Rearranging orders between the jobs does not decrease the durations.
                return None
            orders1 = job1.orders[:begin]
            orders1.extend(orders[:-m_reduced])
            orders2 = orders[-m_reduced:]
            orders2.extend(job2.orders[end:])
            return Job(orders1), Job(orders2)
        else:
            if job2.duration <= orders[-m_reduced-1].duration:
                # Rearranging orders between the jobs does not decrease the durations.
                return None
            orders1 = job1.orders[:begin]
            orders1.extend(orders[-m_reduced:])
            orders2 = orders[:-m_reduced]
            orders2.extend(job2.orders[end:])
            return Job(orders1), Job(orders2)


def improve_iteratively(jobs: MutableSequence[Job], m: int, max_diff: timedelta, max_iter: int = 5) -> None:
    """
    Iteratively decrease the total duration of the jobs.

    Iterate over all pairs of jobs with `reduce_duration`.
    If this returns job(s) with shorter duration(s) replace the pair.

    Repeat until no more durations are reduced or `max_iter` iterations have been performed.
    """
    for _ in range(max_iter):
        improved = False
        i = 0
        while i < len(jobs) - 1:
            j = i + 1
            while j < len(jobs):
                result = reduce_duration(jobs[i], jobs[j], m, max_diff)
                if result is None:
                    j += 1
                else:
                    improved = True
                    if isinstance(result, Job):
                        jobs[i] = result
                        del jobs[j]
                    else:
                        jobs[i], jobs[j] = result
                        j += 1
            i += 1
        if not improved:
            break


def determine_job_starts(jobs: Iterable[Job], start: datetime) -> None:
    """
    Set the start time of the `jobs`, such that the next starts when the previous ends.

    The start time of the first job is given by `start`.
    """
    for job in jobs:
        job.start = start
        start += timedelta(seconds=job.duration)


def format_output(jobs: Iterable[Job], output: str) -> None:
    """Format the `jobs` as csv-rows and write the resulting csv-file to `output`."""
    with open(output, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        for job in jobs:
            writer.writerow(job.format())
    

def main():
    parser = argparse.ArgumentParser(
        description="""Optimize the grouping of orders into jobs.

        Each job can consist of up to m orders that all start in parallel.
        The orders in the same job are restricted to have preferred start times
        which differ by no more than k seconds.
        Only one job can run at the same time.
        The goal is to minimize the total run time of all jobs.
        """
    )
    parser.add_argument("m", type=int, help="maximum number of orders per job")
    parser.add_argument("k", type=int, help="maximum time difference in seconds between preferred start times of orders in the same job")
    parser.add_argument("input", help="input csv-file containing the orders")
    parser.add_argument("output", help="output csv-file for the jobs")
    
    args = parser.parse_args()
    max_diff = timedelta(seconds=abs(args.k))

    orders = parse_input(args.input)
    jobs = group_orders(orders, args.m, max_diff)
    improve_iteratively(jobs, args.m, max_diff)
    determine_job_starts(jobs, jobs[0].orders[0].start)
    check_jobs(jobs, args.m, max_diff)
    format_output(jobs, args.output)


def check_jobs(jobs: Sequence[Job], m: int, max_diff: timedelta):
    if not jobs:
        return
    finish = None
    for job in jobs:
        if job.start is None:
            raise ValueError("Job.start is None.")
        if finish is not None and finish > job.start:
            raise ValueError(f"Previous job finishes at {finish} but this job already starts at {job.start}.")
        if len(job.orders) > m:
            raise ValueError(f"Job {job} has {len(job.orders)} orders, which is more than m = {m} orders.")
        if any(order.start > next_order.start for (order, next_order) in pairwise(job.orders)):
            raise ValueError(f"Job.orders {job.orders} are not sorted by preferred start.")
        job.check_orders_constraint(max_diff)
        if job.duration != max((order.duration for order in job.orders), default=Seconds(0)):
            raise ValueError(f"Job.duration {job.duration} is not the maximum of the durations of the orders {job.orders}.")
        finish = job.start + timedelta(seconds=job.duration)
            

if __name__ == "__main__":
    main()