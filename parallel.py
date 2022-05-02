import argparse
import csv
from datetime import datetime, timedelta
from typing import Generator, Iterable, NewType, Optional, Type, TypeVar
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