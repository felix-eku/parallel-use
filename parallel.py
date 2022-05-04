import argparse
import csv
from datetime import datetime, timedelta
from itertools import islice
from typing import Generator, Iterable, NewType, Optional, Type, TypeVar, Tuple

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
    max_orders: int
    start: datetime | None
    orders: list[Order]
    duration: Seconds

    def __init__(self, m: int, orders: Iterable[Order], start: Optional[datetime] = None) -> None:
        assert m >= 1, "Maximum number of orders is non-positive."
        self.max_orders = m
        self.start = start
        self.orders = sorted(orders, key=lambda order: order.start)
        if self.max_orders < len(self.orders):
            raise ValueError(f"Job can only have {self.max_orders} orders, but got {len(self.orders)} orders.")
        self.duration = max(map(lambda order: order.duration, self.orders), default=Seconds(0))

    def format(self) -> Generator[str, None, None]:
        assert self.start is not None, "Job needs to have a start date set."
        yield self.start.strftime(date_format)
        for order in self.orders:
            yield f"{order.id:d}"


def naively_group_orders(orders: Iterable[Order], m: int, k: Seconds) -> list[Job]:
    """
    Create a naive list of jobs.

    First sort `orders` by their preferred start.
    Try to suscessively take `m` orders from the front of the list of remaining orders
    and add them to the same job.
    If the restraint that the difference between the preferred start times of orders of the same job
    must be less than or equal to `k` seconds would be violated,
    take as many orders as possible, such that the restaint is still fulfilled.
    """
    orders = sorted(orders, key=lambda order: order.start)
    max_start_diff = timedelta(seconds=abs(k))
    n = len(orders)

    jobs = list()
    
    i = 0  # index of the first order included in the current job
    while i < n:
        j = min(i + m, n)  # index of the first excluded order
        latest_start = orders[i].start + max_start_diff
        while latest_start < orders[j-1].start:  # last included order is at j-1
            j -= 1
        jobs.append(Job(m, orders[i:j]))
        i = j

    return jobs


def reduce_duration(job1: Job, job2: Job, m: int, k: Seconds) -> Tuple[Job, Job] | Job | None:
    """
    Try to rearrange the orders between the jobs, such that their total duration decreases.

    Take into account that the maximum number of orders per job is `m`
    and that the preferred starts of orders of the same job can at most be `k` seconds apart.

    Return either a pair of jobs with shorter total duration or a single job if possible
    or None if the total duration cannot be decreased.
    """
    # The algorithm relies on the following observation:
    # The duration of the longer job is always the duration of the longest order.
    # Therefore the total duration of the jobs can only be decreased
    # by decreasing the duration of the shorter job.
    # To achieve this, at least the order with the longest duration of the shorter job
    # must be moved to the longer job (possibly exchanging it with an order with shorter duration).
    max_start_diff = timedelta(seconds=abs(k))
    earliest_start = min(job1.orders[0].start, job2.orders[0].start)
    latest_start = max(job1.orders[-1].start, job2.orders[-1].start)
    if latest_start - earliest_start <= max_start_diff:
        # All orders can be arbitrarily moved between the two jobs
        # without violating the constraint.
        orders = job1.orders.copy()
        orders.extend(job2.orders)
        if len(orders) <= m:
            # All orders can be part of a single job.
            return Job(m, orders)
        orders.sort(key=lambda order: order.duration)
        if min(job1.duration, job2.duration) <= orders[-m-1].duration:
            # Rearranging the orders between jobs does not improve the total duration.
            return None
        # Grouping the m longest orders in one job results in the shortest duration for the other job.
        return Job(m, orders[:-m]), Job(m, orders[-m:])
    else:
        # Let job1 have the order with the earliest preferred start.
        # This implies that job2 has the order with the latest preferred start,
        # since the difference between earliest and latest preferred start is larger than k seconds
        # and therefore cannot occur between orders of the same job.
        if job1.orders[0].start > job2.orders[0].start:
            job1, job2 = job2, job1
        # Earliest preferred start of orders that could be part of job2.
        overlap_begin = latest_start - max_start_diff
        # Latest preferred start of orders that could be part of job1.
        overlap_end = earliest_start + max_start_diff
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
                    end = i + 1  # end is exclusive.
                    break
            else:
                end = 0
            # Number of (possible) orders in the longer job that can be rearranged.
            m_reduced = m - (len(job2.orders) - end)
        else:
            for i, order in enumerate(job1.orders):
                if overlap_begin <= order.start:
                    begin = i
                    break
            else:
                begin = len(job1.orders)
            for i, order in enumerate(reversed(job2.orders)):
                if order.start <= overlap_end:
                    end = i + 1  # end is exclusive.
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
                return Job(m, job1.orders[:begin]), Job(m, orders)
            else:
                orders.extend(job1.orders[:begin])
                return Job(m, orders), Job(m, job2.orders[end:])
        orders.sort(key=lambda order: order.duration)
        if job1.duration < job2.duration:
            if job1.duration <= orders[-m_reduced-1].duration:
                # Rearranging orders between the jobs does not decrease the durations.
                return None
            orders1 = job1.orders[:begin]
            orders1.extend(orders[:-m_reduced])
            orders2 = orders[-m_reduced:]
            orders2.extend(job2.orders[end:])
            return Job(m, orders1), Job(m, orders2)
        else:
            if job2.duration <= orders[-m_reduced-1].duration:
                # Rearranging orders between the jobs does not decrease the durations.
                return None
            orders1 = job1.orders[:begin]
            orders1.extend(orders[-m_reduced:])
            orders2 = orders[:-m_reduced]
            orders2.extend(job2.orders[end:])
            return Job(m, orders1), Job(m, orders2)


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

    orders = parse_input(args.input)
    jobs = naively_group_orders(orders, args.m, args.k)
    determine_job_starts(jobs, jobs[0].orders[0].start)
    format_output(jobs, args.output)


if __name__ == "__main__":
    main()