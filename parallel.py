import argparse
import csv
from datetime import datetime
from typing import Iterable, NewType, Type, TypeVar
from attrs import define


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
        start = datetime.strptime(next(row_iter), "%Y%m%d %H:%M:%S")
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
    print(args.m, args.k, args.input, args.output)


if __name__ == "__main__":
    main()