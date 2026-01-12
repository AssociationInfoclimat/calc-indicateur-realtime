"""
Indicateur RR spatialise : calcul des archives
Génère un fichier CSV directement insérable avec LOAD DATA INFILE dans V5_climato.indicateur_pluvio
"""

import datetime
import os
import statistics
import sys

import numpy as np
from osgeo import gdal
from osgeo import ogr

import logging

MEDIA_FS = "/media/datastore"


def boundingBoxToOffsets(
    bbox: tuple[float, float, float, float, float, float],
    geot: tuple[float, float, float, float, float, float],
) -> tuple[int, int, int, int]:
    # LOGGER.debug(f"boundingBoxToOffsets called with bbox={bbox} geot={geot}")
    col1 = int((bbox[0] - geot[0]) / geot[1])
    col2 = int((bbox[1] - geot[0]) / geot[1]) + 1
    row1 = int((bbox[3] - geot[3]) / geot[5])
    row2 = int((bbox[2] - geot[3]) / geot[5]) + 1
    offsets = (row1, row2, col1, col2)
    # LOGGER.debug(f"boundingBoxToOffsets returning offsets={offsets}")
    return offsets


def geotFromOffsets(
    row_offset: int,
    col_offset: int,
    geot: tuple[float, float, float, float, float, float],
) -> tuple[float, float, float, float, float, float]:
    # LOGGER.debug(f"geotFromOffsets called with row_offset={row_offset} col_offset={col_offset} geot={geot}")
    new_geot = (
        geot[0] + (col_offset * geot[1]),
        geot[1],
        0.0,
        geot[3] + (row_offset * geot[5]),
        0.0,
        geot[5],
    )
    # LOGGER.debug(f"geotFromOffsets computed new_geot={new_geot}")
    return new_geot


# Logging configuration (minimal, only configure when module executed as script)
LOG_FORMAT = "%(asctime)s %(levelname)s %(name)s: %(message)s"
LOG_DATEFMT = "%Y-%m-%d %H:%M:%S"
LOGGER = logging.getLogger(__name__)


def configure_logging(level: int = logging.INFO) -> None:
    """Configure logging to stderr with a reasonable default format.

    This function is intentionally not called at import time to avoid
    changing behavior for callers that import this module (tests, other code).
    Call it when executing as a script or from a test harness if logs are desired.
    """
    root_logger = logging.getLogger()
    # Ensure we don't emit logs to console; write to debug.log in CWD instead.
    # Remove existing handlers to avoid duplicate outputs (including console handlers).
    for h in list(root_logger.handlers):
        root_logger.removeHandler(h)

    # Create a file handler writing to ./debug.log (append mode)
    file_handler = logging.FileHandler("debug.log", mode="a", encoding="utf-8")
    formatter = logging.Formatter(LOG_FORMAT, datefmt=LOG_DATEFMT)
    file_handler.setFormatter(formatter)
    file_handler.setLevel(level)
    root_logger.addHandler(file_handler)
    root_logger.setLevel(level)
    LOGGER.setLevel(level)


def get_tiles_dir() -> str:
    root_path = MEDIA_FS if os.path.isdir(MEDIA_FS) else "/var/data"
    return f"{root_path}/tempsreel.infoclimat.net/tiles"


def calc_index_at_hour(year: int, month: int, day: int, hour: int):
    FILE_DIR = get_tiles_dir()

    stats = {}

    LOGGER.info(f"calc_index_at_hour at: {year:04d}-{month:02d}-{day:02d} {hour:02d}:00")

    # @TODO use comephore when available
    source = None
    srs = None
    in_file = f"{FILE_DIR}/{year:04d}/{month:02d}/{day:02d}/ac_yearly_comephore_{hour:02d}_v00.tif"
    LOGGER.info(f"trying input file (comephore): {in_file}")
    if os.path.isfile(in_file):
        source = "comephore"
        srs = 3857
    else:
        in_file = (
            f"{FILE_DIR}/{year:04d}/{month:02d}/{day:02d}/ac_yearly_radaricval_{hour:02d}_v00.tif"
        )
        LOGGER.info(f"trying input file (radaric): {in_file}")
        source = "radaric"
        srs = 4326

    if not os.path.isfile(in_file):
        LOGGER.warning(f"Input file not found: {in_file}")
        print(",")
        return None

    raster = gdal.Open(in_file, gdal.GA_ReadOnly)
    if raster is None:
        LOGGER.error(f"Failed to open raster file: {in_file}")
        print(",")
        return None

    # SRS dynamique car bug:
    # https://secure.infoclimat.fr/responsablestechnique/site-infoclimat/-/issues/410
    current_dir = os.path.dirname(__file__)
    shape_path = f"{current_dir}/../GEOFLA_DEPARTEMENT/geofla2016_departements_{srs:04d}_simplified.fgb"
    LOGGER.info(f"opening shape file: {shape_path}")
    shape = ogr.Open(shape_path)
    shape_layer = shape.GetLayerByIndex(0)

    mem_driver = ogr.GetDriverByName("MEM")
    mem_driver_gdal = gdal.GetDriverByName("MEM")

    geot = raster.GetGeoTransform()
    nodata = raster.GetRasterBand(1).GetNoDataValue()
    LOGGER.info(f"raster geotransform={geot} nodata={nodata}")

    # https://gis.stackexchange.com/questions/208441/zonal-statistics-of-a-polygon-and-assigning-mean-value-to-the-polygon
    # https://towardsdatascience.com/zonal-statistics-algorithm-with-python-in-4-steps-382a3b66648a

    dest_srs = ogr.osr.SpatialReference()
    dest_srs.ImportFromEPSG(srs)

    # loop sur chaque departement francais
    for ifeat in shape_layer:
        CODE_DEPT = ifeat.GetFieldAsString("CODE_DEPT")
        if ifeat.GetGeometryRef() is None:
            LOGGER.error(f"{CODE_DEPT} Error1: missing geometry")
            continue

        tmp_ds = mem_driver.CreateDataSource("")
        tmp_layer = tmp_ds.CreateLayer("polygons", dest_srs, ogr.wkbPolygon)
        tmp_layer.CreateFeature(ifeat.Clone())
        offsets = boundingBoxToOffsets(ifeat.GetGeometryRef().GetEnvelope(), geot)
        new_geot = geotFromOffsets(offsets[0], offsets[2], geot)

        tr_ds = mem_driver_gdal.Create(
            "",
            offsets[3] - offsets[2],
            offsets[1] - offsets[0],
            1,
            gdal.GDT_Byte,
        )
        tr_ds.SetGeoTransform(new_geot)

        gdal.RasterizeLayer(tr_ds, [1], tmp_layer, burn_values=[1])
        tr_array = tr_ds.ReadAsArray()

        r_array = raster.GetRasterBand(1).ReadAsArray(
            offsets[2],
            offsets[0],
            offsets[3] - offsets[2],
            offsets[1] - offsets[0],
        )

        if r_array is None:
            LOGGER.error(f"{CODE_DEPT} Error2: r_array is None")
            continue

        # should be `r_array == nodata` instead of `r_array > 50000` but there
        # is a strange bug.
        maskarray = np.ma.masked_array(
            r_array,
            mask=np.logical_or(r_array > 50000, np.logical_not(tr_array)),
        )

        if maskarray is None:
            LOGGER.error(f"{CODE_DEPT} Error3: maskarray is None")
            continue

        avg = maskarray.mean()
        stats[CODE_DEPT] = avg

    return {"stats": stats, "source": source}


def print_rr_at_datetime(
    dt: datetime.datetime,
    departments_codes: list[str],
    now: str,
) -> None:
    LOGGER.info(f"print_rr_at_datetime called for date={dt.isoformat()}")
    """
    Journée = 6Z J inclus à 6Z J+1 exclus
    en terme de fichiers, c'est la somme des accumulations horaires de 6h-7h, ..., 5h-6h(J+1)
    soit la somme des fichiers ac60radaricval_07_v00.tif (qui contient de 6h inclus à 7h exclus), ..., ac60radaricval_06_v00.tif(J+1) (qui contient de 5h inclus à 6h exclus)
    mais ici on utilise ac_yearly, qui contient l'accumulation de ces ac60radaricval depuis le début de l'année,
    donc ac_yearly_06_v00.tif contient toute l'accumulation AVANT 6h, et ac_yearly_06_v00.tif(J+1) contient toute l'accumulation AVANT 6h(J+1),
    or, toute l'accumulation AVANT 6h(J+1) est toute l'accumulation AVANT 6h + l'accumulation de 6h à 6h(J+1),
    et puisque c'est uniquement l'accumulation de 6h à 6h(J+1) qui nous intéresse, il faut soustraire de ac_yearly_06_v00.tif(J+1) l'accumulation AVANT 6h,
    donc en enlevant l'accumulation contenu dans ac_yearly_06_v00.tif
    """
    RR_start = None
    RR_end = None

    if dt.day == 31 and dt.month == 12:
        LOGGER.info(f"Handling year boundary special case for {dt}")
        """
        Le 31 décembre, on doit d'abord récupérer le cumul entre 6Z J
        et 0Z J+1, puis additionner au cumul jusque 6Z J+1.
        """
        datetime_start = datetime.datetime(
            dt.year,
            dt.month,
            dt.day,
            6,
            0,
            0,
            tzinfo=datetime.UTC,
        )
        datetime_end = datetime_start + datetime.timedelta(hours=24)
        datetime_new_year = datetime.datetime(
            dt.year + 1,
            1,
            1,
            0,
            0,
            0,
            tzinfo=datetime.UTC,
        )
        LOGGER.info(f"datetime_start={datetime_start} datetime_end={datetime_end} datetime_new_year={datetime_new_year}")
        # cumul entre 6Z J et 0Z J+1
        RR_start = calc_index_at_hour(
            datetime_start.year,
            datetime_start.month,
            datetime_start.day,
            datetime_start.hour,
        )
        RR_new_year = calc_index_at_hour(
            datetime_new_year.year,
            datetime_new_year.month,
            datetime_new_year.day,
            datetime_new_year.hour,
        )

        # cumul de 0Z à 6Z J+1
        RR_end = calc_index_at_hour(
            datetime_end.year,
            datetime_end.month,
            datetime_end.day,
            datetime_end.hour,
        )

        if RR_start is None or RR_new_year is None or RR_end is None:
            LOGGER.warning(f"One of RR_start/RR_new_year/RR_end is None, aborting for {dt}")
            return

        # TODO: clean up logic
        # print(datetime_start,datetime_new_year,datetime_end)
        # print(RR_start['source'],RR_new_year['source'],RR_end['source'])
        if RR_new_year["source"] != RR_start["source"]:
            LOGGER.info(f"Source changed from {RR_start['source']} to {RR_new_year['source']}")
        for dept in RR_start["stats"]:
            # print(dept, RR_start['stats'][dept], RR_new_year['stats'][dept], RR_end['stats'][dept])
            if RR_new_year["source"] != RR_start["source"]:
                # @TODO bugfix 31/12/2019 passage radaric-comephore
                RR_new_year["stats"][dept] = RR_end["stats"][dept]
            else:
                LOGGER.debug(f"start={RR_start['stats'][dept]} new_year={RR_new_year['stats'][dept]} end={RR_end['stats'][dept]}")
                RR_new_year["stats"][dept] = (
                    RR_new_year["stats"][dept] - RR_start["stats"][dept]
                ) + RR_end["stats"][dept]

            RR_start["stats"][dept] = 0
            RR_end["stats"][dept] = RR_new_year["stats"][dept]
    else:
        LOGGER.info(f"Handling standard date for {dt}")
        # dt += day1
        datetime_start = datetime.datetime(
            dt.year,
            dt.month,
            dt.day,
            6,
            0,
            0,
            tzinfo=datetime.UTC,
        )
        datetime_end = datetime_start + datetime.timedelta(hours=24)
        LOGGER.info(f"datetime_start={datetime_start} datetime_end={datetime_end}")
        RR_start = calc_index_at_hour(
            datetime_start.year,
            datetime_start.month,
            datetime_start.day,
            datetime_start.hour,
        )
        RR_end = calc_index_at_hour(
            datetime_end.year,
            datetime_end.month,
            datetime_end.day,
            datetime_end.hour,
        )

    # Si l'on est le 31 décembre, le fichier RR_end contient une valeur inférieure à RR_start
    # il faut donc prendre uniq

    if RR_start is None or RR_end is None:
        LOGGER.warning(f"RR_start or RR_end is None for date {dt}, skipping output")
        return

    LOGGER.info(f"Preparing CSV output for date {dt}")
    print(f"{dt.year},{dt.month},{dt.day},", end="")
    cumul_fr = []
    for code_dept in departments_codes:
        cumul_24h = round(RR_end["stats"][code_dept] - RR_start["stats"][code_dept])
        LOGGER.debug(f"dept={code_dept} cumul_24h={cumul_24h}")
        print(f"{cumul_24h},", end="")
        cumul_fr.append(cumul_24h)

    # cumul moyen sur tous les départements métropolitains
    print(f"{statistics.mean(cumul_fr)},", end="")
    print(f"{RR_start['source']},{now}")


def get_departments_codes() -> list[str]:
    current_dir = os.path.dirname(__file__)
    shape = ogr.Open(
        f"{current_dir}/../GEOFLA_DEPARTEMENT/geofla2016_departements_4326_simplified.fgb"
    )
    shape_layer = shape.GetLayerByIndex(0)
    codes = []
    for ifeat in shape_layer:
        codes.append(ifeat.GetFieldAsString("CODE_DEPT"))
    LOGGER.info(f"get_departments_codes found {len(codes)} departments")
    return codes


def get_rr_csv_header(departments_codes: list[str]) -> str:
    columns = [
        "annee",
        "mois",
        "jour",
        *[f"d{code}" for code in departments_codes],
        "FR",
        "source",
        "dh_maj",
    ]
    return ",".join(columns)


def print_rr_csv_header(departments_codes: list[str]) -> None:
    print(get_rr_csv_header(departments_codes))


def print_rr_csv(start_period: datetime.datetime, end_period: datetime.datetime) -> None:
    departments_codes = get_departments_codes()
    LOGGER.info(f"print_rr_csv start_period={start_period} end_period={end_period}")
    print_rr_csv_header(departments_codes)
    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    dt = start_period
    one_day = datetime.timedelta(days=1)
    while dt <= end_period:
        print_rr_at_datetime(dt, departments_codes, now)
        dt += one_day


def get_datetime_from_str(str_date: str) -> datetime.datetime:
    LOGGER.debug(f"Parsing date string: {str_date}")
    [YYYY, MM, DD] = str_date.split("-")
    period = datetime.datetime(year=int(YYYY), month=int(MM), day=int(DD))
    LOGGER.debug(f"Parsed date: {period}")
    return period


def get_datetime_interval_from_str(
    start: str, end: str
) -> tuple[datetime.datetime, datetime.datetime]:
    start_period = get_datetime_from_str(start)
    end_period = get_datetime_from_str(end)
    return (start_period, end_period)


def get_datetime_interval_from_sysargv() -> tuple[datetime.datetime, datetime.datetime]:
    return get_datetime_interval_from_str(sys.argv[1], sys.argv[2])


def main() -> None:
    LOGGER.info(f"main entry, argv={sys.argv}")
    (start_period, end_period) = get_datetime_interval_from_sysargv()
    LOGGER.info(f"Will print CSV for interval {start_period} - {end_period}")
    print_rr_csv(start_period, end_period)


if __name__ == "__main__":
    configure_logging()
    main()
