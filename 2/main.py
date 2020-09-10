from sqlalchemy import create_engine, String, Column, Integer, DateTime, Float, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship

import pandas as pd

ENERGY_CSV_FILEPATH = './energy.csv'
OPERATORS_CSV_FILEPATH = './operators.csv'
PERIODS_CSV_FILEPATH = './periods.csv'
REASONS_CSV_FILEPATH = './reasons.csv'

Base = declarative_base()

engine = create_engine('postgresql://german@localhost:5432/period_view')
Session_ = sessionmaker()
Session_.configure(bind=engine)
session = Session_()

class Energy(Base):
    __tablename__ = 'energy'

    id = Column(Integer, primary_key=True)
    endpoint_id = Column(Integer)
    event_time = Column(DateTime)
    kwh = Column(Float)

    def __repr__(self):
        return '<Energy(endpoint_id={self.endpoint_id}, event_time={self.event_time}, kwh={self.kwh})>'

    @classmethod
    def create_energy_from_csv(cls, filepath=''):
        energy_df = pd.read_csv(filepath, sep=';')
        energy = [
            Energy(
                endpoint_id=energy['endpoint_id'],
                event_time=energy['event_time'],
                kwh=energy['kwh'],
            ) for idx, energy in energy_df.iterrows()
        ]
        session.add_all(energy)
        return energy

class Operators(Base):
    __tablename__ = 'operators'

    id = Column(Integer, primary_key=True)
    endpoint_id = Column(Integer)
    login_time = Column(DateTime)
    logout_time = Column(DateTime)
    operator_name = Column(String(128))

    def __repr__(self):
        return '<Operator(endpoint_id={self.endpoint_id}, login_time={self.login_time}, logout_time={self.logout_time}, operator_time={self.operator_time})>'

    @classmethod
    def create_operators_from_csv(cls, filepath=''):
        operators_df = pd.read_csv(filepath, sep=';')
        operators = [
            Operators(
                endpoint_id=operator['endpoint_id'],
                login_time=operator['login_time'],
                logout_time=operator['logout_time'],
                operator_name=operator['operator_name']
            ) for idx, operator in operators_df.iterrows()
        ]
        session.add_all(operators)
        return operators

class Periods(Base):
    __tablename__ = 'periods'

    id = Column(Integer, primary_key=True)
    endpoint_id = Column(Integer)
    mode_start = Column(DateTime)
    mode_duration = Column(Integer)
    label = Column(String(64), default="Нет данных")

    def __repr__(self):
        return '<Period(endpoint_id={self.endpoint_id}, mode_start={self.mode_start}, mode_duration={self.mode_duration}, label={self.label})>'

    @classmethod
    def create_periods_from_csv(cls, filepath=''):
        periods_df = pd.read_csv(filepath, sep=';')
        periods = [
            Periods(
                endpoint_id=period['endpoint_id'],
                mode_start=period['mode_start'],
                mode_duration=period['mode_duration'],
                label=period['label']
            ) for idx, period in periods_df.iterrows()
        ]
        session.add_all(periods)
        return periods

class Reasons(Base):
    __tablename__ = 'reasons'

    id = Column(Integer, primary_key=True)
    endpoint_id = Column(Integer)
    event_time = Column(DateTime)
    reason = Column(String(128))

    def __repr__(self):
        return '<Reason(endpoint_id={self.endpoint_id}, event_time={self.event_time}, reason={self.reason})>'

    @classmethod
    def create_reasons_from_csv(cls, filepath=''):
        reasons_df = pd.read_csv(filepath, sep=';')
        reasons = [
            Reasons(
                endpoint_id=reason['endpoint_id'],
                event_time=reason['event_time'],
                reason=reason['reason']
            ) for idx, reason in reasons_df.iterrows()
        ]
        session.add_all(reasons)

        return reasons

class PeriodView(Base):
    __tablename__ = 'period_view'

    id = Column(Integer, primary_key=True)
    endpoint_id = Column(Integer, ForeignKey('periods.endpoint_id'))
    mode_start = Column(DateTime, ForeignKey('periods.mode_start'))
    mode_end = Column(DateTime)
    mode_duration = Column(DateTime, ForeignKey('periods.mode_duration'))
    label = Column(String(64), ForeignKey('periods.label'))
    reason = Column(String(128), ForeignKey('reasons.reason'))
    operator_name = Column(String(128), ForeignKey('operators.operator_name'))
    energy_sum = Column(Float, ForeignKey('energy.kwh'))

    periods = relationship("Periods", back_populates="periodViews")
    reasons = relationship("Reasons", back_populates="periodViews")
    operators = relationship("Operators", back_populates="periodViews")
    energy = relationship("Energy", back_populates="periodViews")

    @classmethod
    def create_periodview_from_dfs(cls, reasons, operators):
        return


reasons = Reasons.create_reasons_from_csv(REASONS_CSV_FILEPATH)
operators = Operators.create_operators_from_csv(OPERATORS_CSV_FILEPATH)
periods = Periods.create_periods_from_csv(PERIODS_CSV_FILEPATH)
energys = Energy.create_energy_from_csv(ENERGY_CSV_FILEPATH)


for energy in energys:

