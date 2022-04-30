import argparse


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