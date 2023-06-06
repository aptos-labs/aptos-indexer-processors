from sqlalchemy import BigInteger, create_engine, DateTime, func, String
from sqlalchemy.orm import DeclarativeBase, mapped_column, Mapped
from datetime import datetime
from typing_extensions import Annotated

BigIntegerPrimaryKeyType = Annotated[int, mapped_column(BigInteger, primary_key=True)]
StringPrimaryKeyType = Annotated[str, mapped_column(String, primary_key=True)]
BigIntegerType = Annotated[int, mapped_column(BigInteger)]

TimestampType = Annotated[datetime, mapped_column(DateTime(timezone=True))]
InsertedAtType = Annotated[
    datetime, mapped_column(DateTime(timezone=True), default=func.now())
]
UpdatedAtType = Annotated[
    datetime,
    mapped_column(
        DateTime(timezone=True),
        default=func.now(),
        onupdate=func.now(),
    ),
]
