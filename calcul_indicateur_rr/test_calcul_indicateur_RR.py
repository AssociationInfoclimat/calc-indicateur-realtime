from calcul_indicateur_rr.calcul_indicateur_RR import get_datetime_interval_from_str, print_rr_csv


def main() -> None:
    (start_period, end_period) = get_datetime_interval_from_str("2018-12-31", "2019-01-02")
    print_rr_csv(start_period, end_period)
    (start_period, end_period) = get_datetime_interval_from_str("2020-12-31", "2021-01-02")
    print_rr_csv(start_period, end_period)


if __name__ == "__main__":
    main()
