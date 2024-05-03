import numpy as np
from owslib.wcs import WebCoverageService
from owslib.util import Authentication
from shapely import wkt


# TODO Check how can it be improved
## TO READ WCS outputs
class WCS:
    """WCS object to get metadata etc and to get grid."""

    def __init__(self, host, layer, username, password):
        self.username = username
        self.password = password
        self.id = layer
        self.wcs = (
            WebCoverageService(
                host,
                version="1.0.0",
                auth=Authentication(username=self.username, password=self.password),
            )
            if self.password and self.username
            else WebCoverageService(host, version="1.0.0")
        )
        self.layer = self.wcs[self.id]
        self.cx, self.cy = map(int, self.layer.grid.highlimits)
        self.crs = self.layer.boundingboxes[0]["nativeSrs"]
        self.bbox = self.layer.boundingboxes[0]["bbox"]
        self.lx, self.ly, self.hx, self.hy = map(float, self.bbox)
        self.resx, self.resy = (self.hx - self.lx) / self.cx, (
            self.hy - self.ly
        ) / self.cy
        self.width = self.cx
        self.height = self.cy

    def getw(self, fn):
        """Downloads raster and returns filename of written GEOTIFF in the tmp dir."""
        gc = self.wcs.getCoverage(
            identifier=self.id,
            bbox=self.bbox,
            format="GeoTIFF",
            crs=self.crs,
            width=self.width,
            height=self.height,
        )
        f = open(fn, "wb")
        f.write(gc.read())
        f.close()
        return fn

    def getw_with_auth(self, fn):
        """Downloads raster and returns filename of written GEOTIFF in the tmp dir."""
        gc = self.wcs.getCoverage(
            identifier=self.id,
            bbox=self.bbox,
            format="GeoTIFF",
            crs=self.crs,
            width=self.width,
            height=self.height,
            auth=Authentication(username=self.username, password=self.password),
        )
        f = open(fn, "wb")
        f.write(gc.read())
        f.close()
        return fn


## TO handle transects
class LS:
    """Intersection on grid line"""

    def __init__(self, awkt, crs, host, layer, username, password, sampling=1):
        self.wwkt = awkt
        self.crs = crs
        self.gs = WCS(
            host, layer, username, password
        )  # Initiates WCS service to get some parameters about the grid.
        self.sampling = sampling

    def line(self):
        """Creates WCS parameters and sample coordinates for cells in raster based on line input."""
        self.ls = wkt.loads(self.wwkt)
        self.ax, self.ay, self.bx, self.by = self.ls.bounds
        # TODO http://stackoverflow.com/questions/13439357/extract-point-from-raster-in-gdal

        """if first x is larger than second, coordinates will be flipped during process of defining bounding box !!!!
           next lines introduce a boolean flip variable used in the last part of this process"""
        flipx = False
        flipy = False
        ax, bx = self.ls.coords.xy[0]
        ay, by = self.ls.coords.xy[1]

        if ax >= bx:
            flipx = True
        if ay >= by:
            flipy = True

        """get raster coordinates"""
        self.ax = (
            self.ax - self.gs.lx
        )  # coordinates minus coordinates of raster, start from 0,0
        self.ay = self.ay - self.gs.ly
        self.bx = self.bx - self.gs.lx
        self.by = self.by - self.gs.ly
        self.x1, self.y1 = int(self.ax // self.gs.resx), int(self.ay // self.gs.resy)
        self.x2, self.y2 = (
            int(self.bx // self.gs.resx) + 1,
            int(self.by // self.gs.resy) + 1,
        )
        self.gs.bbox = (
            self.x1 * self.gs.resx + self.gs.lx,
            self.y1 * self.gs.resy + self.gs.ly,
            self.x2 * self.gs.resx + self.gs.lx,
            self.y2 * self.gs.resy + self.gs.ly,
        )
        self.gs.width = abs(self.x2 - self.x1)  # difference of x cells
        self.gs.height = abs(self.y2 - self.y1)
        """ here we go back to our line again instead of calculating bbox for wcs request."""
        self.ax, self.bx = self.ls.coords.xy[0]
        self.ay, self.by = self.ls.coords.xy[1]

        # coordinates minus coordinates of raster, start from 0,0
        self.ax = self.ax - self.gs.lx
        self.ay = self.ay - self.gs.ly
        self.bx = self.bx - self.gs.lx
        self.by = self.by - self.gs.ly

        if flipx and flipy:  # who draws these lines?
            # top right to bottom left
            self.x2, self.y2 = int(self.bx // self.gs.resx), int(
                self.by // self.gs.resy
            )
            self.x1, self.y1 = (
                int(self.ax // self.gs.resx) + 1,
                int(self.ay // self.gs.resy) + 1,
            )
        elif flipx:
            # bottom right to top left
            self.x2, self.y1 = int(self.bx // self.gs.resx), int(
                self.ay // self.gs.resy
            )
            self.x1, self.y2 = (
                int(self.ax // self.gs.resx) + 1,
                int(self.by // self.gs.resy) + 1,
            )
        elif flipy:
            # top left to bottom right
            self.x1, self.y2 = int(self.ax // self.gs.resx), int(
                self.by // self.gs.resy
            )
            self.x2, self.y1 = (
                int(self.bx // self.gs.resx) + 1,
                int(self.ay // self.gs.resy) + 1,
            )
        else:
            # normal
            self.x1, self.y1 = int(self.ax // self.gs.resx), int(
                self.ay // self.gs.resy
            )
            self.x2, self.y2 = (
                int(self.bx // self.gs.resx) + 1,
                int(self.by // self.gs.resy) + 1,
            )

        # From upperright to lower left x values become negative
        # Subdivide the line into sampling points of the raster.
        # Takes longest dimension and uses number of cells * sampling as the
        # number of subdivisions.
        # Grid of subdivions is pixel grid - 0.5
        self.subdiv = int(max(self.gs.width, self.gs.height)) * self.sampling
        self.xlist = np.linspace(
            (self.ax / self.gs.resx) - min(self.x1, self.x2),
            (self.bx / self.gs.resx) - min(self.x1, self.x2),
            num=self.subdiv,
        )
        self.ylist = np.linspace(
            (self.ay / self.gs.resy) - min(self.y1, self.y2),
            (self.by / self.gs.resy) - min(self.y1, self.y2),
            num=self.subdiv,
        )

    def getraster(self, fname, all_box=False):
        """Returns values of line intersection on downlaoded geotiff from wcs."""
        if self.gs.username and self.gs.password:
            self.gs.getw_with_auth(fname)
        else:
            self.gs.getw(fname)
        return
