from numpy.random import default_rng
from datetime import datetime, timedelta
import numpy as np
import pandas as pd


def generate_orders(
    n: int, 
    start: datetime = None, step: timedelta = None, deviation: timedelta = None, 
    max_time: timedelta = None, seed: int = 0
):
    """
    Generate a pandas.DataFrame, the rows of which represent random orders.

    The index of the DataFrame represents the IDs of the orders,
    the column "preferred_start" the preferred start times
    and the column "processing_time" the processing times in seconds.
    """
    if start is None:
        start = datetime.now()
    if step is None:
        step = timedelta(hours=6)
    if deviation is None:
        deviation = timedelta(hours=4)
    if max_time is None:
        max_time = timedelta(hours=24)

    rng = default_rng(seed)
    return pd.DataFrame(
        {
            "preferred_start": start + step * np.arange(n) + deviation * rng.standard_normal(n),
            "processing_time": (max_time.total_seconds() * rng.random(n)).astype(int, copy=False),
        },
    )


def write_orders(orders: pd.DataFrame, filename: str):
    """Write the orders to a csv-file."""
    orders.to_csv(filename, header=False, index=True, date_format="%Y%m%d %H:%M:%S")
