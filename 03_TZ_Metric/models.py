from sqlalchemy import Column, Integer, BigInteger, Numeric, TIMESTAMP, text
from db import Base

class TVLMetrics(Base):
   __tablename__ = 'tvl_metrics'
   id = Column(Integer, primary_key=True, index=True)
   timestamp = Column(TIMESTAMP, nullable=False,server_default=text("CURRENT_TIMESTAMP"))
   tvl = Column(Numeric(30,6), nullable=False)
   share_price = Column(Numeric(30,18), nullable=False)
   block_number = Column(BigInteger, nullable=False)