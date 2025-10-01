from __future__ import annotations


class RasterBound:
    """structure describing a rectangular section of a raster.
    Uses upper left corner origin scheme that GDAL also uses.

    Args:
        x_off (int): the x pixel coordinate of the upper left corner
        y_off (int): the y pixel coordinate of the upper left corner
        x_size (int): the number of pixels on the x dimension. Also known as
            width, or the number of columns in the rectangle.
        y_size (int): the number of pixels on the y dimension. Also known as
            height, or the number of rows in the rectangle.

    """

    def __init__(self, x_off, y_off, x_size, y_size):
        self.__x_off = x_off
        self.__y_off = y_off
        self.__x_size = x_size
        self.__y_size = y_size

    def __str__(self):
        return (
            f"x_off: {self.__x_off}, "
            f"y_off: {self.__y_off}, "
            f"x_size: {self.__x_size}, "
            f"y_size: {self.__y_size}"
        )

    def __eq__(self, value):
        return (
            self.__x_off == value.__x_off
            and self.__y_off == value.__y_off
            and self.__x_size == value.__x_size
            and self.__y_size == value.__y_size
        )

    def __ne__(self, value):
        return not self.__eq__(value)

    @property
    def x_off(self):
        """Gets the x offset, also known as the column, of the upper left
        corner of this rectangle in raster pixel coordinates.
        """
        return self.__x_off

    @property
    def y_off(self):
        """Gets the y offset, also known as the row, of the upper left
        corner of this rectangle in raster pixel coordinates.

        Returns:
            int: the y offset value
        """
        return self.__y_off

    @property
    def x_size(self):
        """Get the x size, also known as the width or number of columns
        in this rectangle in number of raster pixels

        Returns:
            int: the x size value
        """
        return self.__x_size

    @property
    def y_size(self):
        """Get the y size, also known as the height or number of rows
        in this rectangle in number of raster pixels

        Returns:
            int: the y size value
        """
        return self.__y_size
