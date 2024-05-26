from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, MetaData, Column, Integer, DateTime
from commons.configs import DB_HOST, DB_PASSWORD, DB_USERNAME, DB_PORT
from sqlalchemy.sql import func




dbparams = {
    'username': DB_USERNAME,
    'host': DB_HOST,
    'port': DB_PORT,
    'password': DB_PASSWORD,
    'dbname': 'uk-retail-data'
}
engine = create_engine("postgresql+psycopg2://{username}:{password}@{host}:{port}/{dbname}".format(**dbparams))
metadata = MetaData(schema='in_use')
metadata.reflect(engine, schema='in_use')


Base = automap_base(metadata=metadata)

class RetailStoreItems(Base):
    __tablename__ = "retail_store_items"
    __table_args__ = {'extend_existing': True}
    id = Column(Integer, primary_key=True)
    create_datetime = Column(DateTime(timezone=True), server_default=func.now())


# reflect the tables
Base.prepare(autoload_with=engine, schema="in_use")


if __name__ == "__main__":
    session = Session(engine)

    recs = session.query(RetailStoreItems).all()
    print(recs)
    session.close()