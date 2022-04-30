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

    The index of the DataFrame represents the order IDs,
    the column "preferred_start" the preferred start time
    and the column "processing_time" the processing time.
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
            "processing_time": max_time * rng.random(n),
        }
    )
