import datetime
import decimal
from sqlalchemy import UniqueConstraint, func
from sqlalchemy.orm import Mapped, mapped_column

from db.db import Base


class TradingResult(Base):
    __tablename__ = 'spimex_trading_results'

    exchange_product_id: Mapped[str]
    exchange_product_name: Mapped[str]
    oil_id: Mapped[str]
    delivery_basis_id: Mapped[str]
    delivery_basis_name: Mapped[str]
    delivery_type_id: Mapped[str]
    volume: Mapped[int]
    total: Mapped[decimal.Decimal]
    count: Mapped[int]
    date: Mapped[datetime.datetime]
    created_on: Mapped[datetime.datetime] = mapped_column(
        server_default=func.now(), default=func.now())
    updated_on: Mapped[datetime.datetime] = mapped_column(default=func.now(),
                                                          server_default=func.now(),
                                                          onupdate=func.now())

    def __repr__(self) -> str:
        return f'{self.exchange_product_id}'

    __table_args__ = (UniqueConstraint('date', 'exchange_product_id'),)
