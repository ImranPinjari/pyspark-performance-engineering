from pyspark.sql import DataFrame
from pyspark.sql.functions import col, to_date, lit


def separate_invalid_dates(dataframe: DataFrame, date_column: str):
    """
    Separate records with invalid dates.
    Accepted format: yyyy-MM-dd.
    Rejects malformed and impossible calendar dates.
    """

    parsed_date = to_date(col(date_column), "yyyy-MM-dd")

    valid_condition = (
        col(date_column).isNotNull()
        & col(date_column).rlike(r"^\d{4}-\d{2}-\d{2}$")
        & parsed_date.isNotNull()
    )

    valid = dataframe.filter(valid_condition)

    rejected = (
        dataframe.filter(~valid_condition)
        .withColumn("reject_reason", lit("INVALID_DATE_FORMAT"))
    )

    return valid, rejected
