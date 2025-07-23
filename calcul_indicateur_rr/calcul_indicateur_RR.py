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

MEDIA_FS = "/media/datastore"


def boundingBoxToOffsets(
    bbox: tuple[float, float, float, float, float, float],
    geot: tuple[float, float, float, float, float, float],
) -> tuple[int, int, int, int]:
    col1 = int((bbox[0] - geot[0]) / geot[1])
    col2 = int((bbox[1] - geot[0]) / geot[1]) + 1
    row1 = int((bbox[3] - geot[3]) / geot[5])
    row2 = int((bbox[2] - geot[3]) / geot[5]) + 1
    return (row1, row2, col1, col2)


def geotFromOffsets(
    row_offset: int,
    col_offset: int,
    geot: tuple[float, float, float, float, float, float],
) -> tuple[float, float, float, float, float, float]:
    new_geot = (
        geot[0] + (col_offset * geot[1]),
        geot[1],
        0.0,
        geot[3] + (row_offset * geot[5]),
        0.0,
        geot[5],
    )
    return new_geot


def get_tiles_dir() -> str:
    root_path = MEDIA_FS if os.path.isdir(MEDIA_FS) else "/var/data"
    return f"{root_path}/tempsreel.infoclimat.net/tiles"


def calc_index_at_hour(year: int, month: int, day: int, hour: int):
    FILE_DIR = get_tiles_dir()

    stats = {}

    # @TODO use comephore when available
    source = None
    srs = None
    in_file = f"{FILE_DIR}/{year:04d}/{month:02d}/{day:02d}/ac_yearly_comephore_{hour:02d}_v00.tif"
    if os.path.isfile(in_file):
        source = "comephore"
        srs = 3857
    else:
        in_file = (
            f"{FILE_DIR}/{year:04d}/{month:02d}/{day:02d}/ac_yearly_radaricval_{hour:02d}_v00.tif"
        )
        source = "radaric"
        srs = 4326

    if not os.path.isfile(in_file):
        print(",")
        return None

    raster = gdal.Open(in_file, gdal.GA_ReadOnly)

    # SRS dynamique car bug:
    # https://secure.infoclimat.fr/responsablestechnique/site-infoclimat/-/issues/410
    current_dir = os.path.dirname(__file__)
    shape = ogr.Open(
        f"{current_dir}/../GEOFLA_DEPARTEMENT/geofla2016_departements_{srs:04d}_simplified.fgb"
    )
    shape_layer = shape.GetLayerByIndex(0)

    mem_driver = ogr.GetDriverByName("Memory")
    mem_driver_gdal = gdal.GetDriverByName("MEM")

    geot = raster.GetGeoTransform()
    nodata = raster.GetRasterBand(1).GetNoDataValue()

    # https://gis.stackexchange.com/questions/208441/zonal-statistics-of-a-polygon-and-assigning-mean-value-to-the-polygon
    # https://towardsdatascience.com/zonal-statistics-algorithm-with-python-in-4-steps-382a3b66648a

    dest_srs = ogr.osr.SpatialReference()
    dest_srs.ImportFromEPSG(srs)

    # loop sur chaque departement francais
    for ifeat in shape_layer:
        CODE_DEPT = ifeat.GetFieldAsString("CODE_DEPT")
        if ifeat.GetGeometryRef() is None:
            print(CODE_DEPT, "Error1")
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
            print(CODE_DEPT, "Error2")
            continue

        # should be `r_array == nodata` instead of `r_array > 50000` but there
        # is a strange bug.
        maskarray = np.ma.masked_array(
            r_array,
            mask=np.logical_or(r_array > 50000, np.logical_not(tr_array)),
        )

        if maskarray is None:
            print(CODE_DEPT, "Error3")
            continue

        avg = maskarray.mean()
        stats[CODE_DEPT] = avg

    return {"stats": stats, "source": source}


def print_rr_at_datetime(
    dt: datetime.datetime,
    departments_codes: list[str],
    now: str,
) -> None:
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
            return

        # TODO: clean up logic
        # print(datetime_start,datetime_new_year,datetime_end)
        # print(RR_start['source'],RR_new_year['source'],RR_end['source'])
        for dept in RR_start["stats"]:
            # print(dept, RR_start['stats'][dept], RR_new_year['stats'][dept], RR_end['stats'][dept])
            if RR_new_year["source"] != RR_start["source"]:
                # @TODO bugfix 31/12/2019 passage radaric-comephore
                RR_new_year["stats"][dept] = RR_end["stats"][dept]
            else:
                RR_new_year["stats"][dept] = (
                    RR_new_year["stats"][dept] - RR_start["stats"][dept]
                ) + RR_end["stats"][dept]

            RR_start["stats"][dept] = 0
            RR_end["stats"][dept] = RR_new_year["stats"][dept]
    else:
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
        return

    print(f"{dt.year},{dt.month},{dt.day},", end="")
    cumul_fr = []
    for code_dept in departments_codes:
        cumul_24h = round(RR_end["stats"][code_dept] - RR_start["stats"][code_dept])
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
    print_rr_csv_header(departments_codes)
    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    dt = start_period
    one_day = datetime.timedelta(days=1)
    while dt <= end_period:
        print_rr_at_datetime(dt, departments_codes, now)
        dt += one_day


def get_datetime_from_str(str_date: str) -> datetime.datetime:
    [YYYY, MM, DD] = str_date.split("-")
    period = datetime.datetime(year=int(YYYY), month=int(MM), day=int(DD))
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
    (start_period, end_period) = get_datetime_interval_from_sysargv()
    print_rr_csv(start_period, end_period)


if __name__ == "__main__":
    main()
