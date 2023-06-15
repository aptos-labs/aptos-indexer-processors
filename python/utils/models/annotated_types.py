from sqlalchemy import BigInteger, Boolean, DateTime, func, Numeric, String
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import mapped_column, Mapped
from datetime import datetime
from typing_extensions import Annotated

# To make pylint happy

# Primary key types
BigIntegerPrimaryKeyType = Mapped[
    Annotated[int, mapped_column(BigInteger, primary_key=True)]
]
NumericPrimaryKeyType = Mapped[
    Annotated[float, mapped_column(Numeric, primary_key=True)]
]
StringPrimaryKeyType = Mapped[Annotated[str, mapped_column(String, primary_key=True)]]

# Normal types
BigIntegerType = Mapped[Annotated[int, mapped_column(BigInteger)]]
BooleanType = Mapped[Annotated[bool, mapped_column(Boolean)]]
JsonType = Mapped[Annotated[str, mapped_column(JSONB)]]
NumericType = Mapped[Annotated[float, mapped_column(Numeric)]]
StringType = Mapped[Annotated[str, mapped_column(String)]]

# Nullable types
NullableNumericType = Mapped[Annotated[float, mapped_column(Numeric, nullable=True)]]
NullableStringType = Mapped[Annotated[str, mapped_column(String, nullable=True)]]

# Timestamp types
TimestampType = Mapped[Annotated[datetime, mapped_column(DateTime(timezone=True))]]
InsertedAtType = Mapped[
    Annotated[datetime, mapped_column(DateTime(timezone=True), default=func.now())]
]
UpdatedAtType = Mapped[
    Annotated[
        datetime,
        mapped_column(
            DateTime(timezone=True),
            default=func.now(),
            onupdate=func.now(),
        ),
    ]
]
