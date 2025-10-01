from __future__ import annotations

import math

from gcbmwalltowall.util.rasterbound import RasterBound


def __get_chunk_bounds(
    width, height: int, chunk_width: int, chunk_height: int, row: int, col: int
):
    """create a RasterBound object for the specified parameters

    Args:
        width (int): the entire raster width in pixels (x dimension)
        height (int): the entire raster height in pixels (y dimension)
        chunk_width (int): the maximum width of the chunk
        chunk_height (int): the maximum height of the chunk
        row (int): the row number in chunk coordinates
        col (int): the column number in chunk coordinates

    Raises:
        ValueError: thrown when the specified parameters do not correspond
            to a chunk within the raster height and width

    Returns:
        RasterBound: a RasterBound object specifying the rectangular subset
            of the raster.
    """
    x_off = col * chunk_width
    x_size = chunk_width if (width - x_off) > chunk_width else width - x_off
    y_off = row * chunk_height
    y_size = chunk_height if (height - y_off) > chunk_height else height - y_off
    return RasterBound(x_off, y_off, x_size, y_size)


def get_raster_chunks(width: int, height: int, chunk_width: int, chunk_height: int):
    """Generate descriptions of distinct rectangular sections for the
    specified raster dimensions, and maximum chunk size dimensions.
    Chunks are returned in row-major order.

    Args:
        width (int): the entire raster width in pixels (x dimension)
        height (int): the entire raster height in pixels (y dimension)
        chunk_width (int): the maximum width of chunks in the returned
            rectangles
        chunk_height (int): the maximum height of chunks in the returned
            rectangles

    Yields:
        RasterBound: Sequence of RasterBound objects describing distinct
            subsets of the raster area.  The area sum of all returned
            RasterBound objects is the area of the raster.
    """

    if width <= 0 or height <= 0 or chunk_width <= 0 or chunk_height <= 0:
        raise ValueError("parameters must be positive integers")
    n_cols = math.ceil(width / chunk_width)
    n_rows = math.ceil(height / chunk_height)
    for col in range(0, n_cols):
        for row in range(0, n_rows):
            yield __get_chunk_bounds(width, height, chunk_width, chunk_height, row, col)


def get_memory_limited_raster_chunks(
    n_rasters: int,
    width: int,
    height: int,
    memory_limit_MB: int,
    bytes_per_pixel: int = 4,
):
    """Call :py:func:`get_raster_chunks` so that the chunks returned describe
    rectangles that when loaded won't consume memory in excess of the
    specified memory limit.

    Args:
        n_rasters (int): the number of stacked rasters whose chunks will be
            loaded into memory
        width (int): the entire raster width in pixels (x dimension)
        height (int): the entire raster height in pixels (y dimension)
        memory_limit_MB (int): the maximum memory in megabytes that can be
            loaded for the raster stack
        bytes_per_pixel (int, optional): the number of bytes on each raster.
            Defaults to 4.

    Raises:
        ValueError: Negative or zero parameters

    Returns:
        sequence: the memory limited sequence of RasterBound objects.
    """
    divisor = n_rasters * bytes_per_pixel / 1e6
    if divisor <= 0:
        raise ValueError
    max_pixels = memory_limit_MB / divisor
    if max_pixels > (width * height):
        return get_raster_chunks(width, height, width, height)
    else:
        size = int(math.sqrt(max_pixels))
        return get_raster_chunks(width, height, size, size)
