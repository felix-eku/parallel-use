from numpy.random import default_rng
from datetime import datetime, timedelta
import numpy as np
import pandas as pd


def generate_orders(
    n: int, start: datetime, step: timedelta, deviation: timedelta, max_time: timedelta,
    seed: int = 0
):
    """
    Generate a pandas.DataFrame, the rows of which represent random orders.

    The index of the DataFrame represents the order IDs,
    the column "preferred_start" the preferred start time
    and the column "processing_time" the processing time.
    """
    rng = default_rng(seed)
    return pd.DataFrame(
        {
            "preferred_start": start + step * np.arange(n) + deviation * rng.standard_normal(n),
            "processing_time": max_time * rng.random(n),
        }
    )
