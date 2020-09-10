from typing import List

from sqlalchemy import create_engine, String, Column, Integer, DateTime, Float, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship, Query

import pandas as pd
import logging
from datetime import datetime, timedelta

ENERGY_CSV_FILEPATH = './energy.csv'
OPERATORS_CSV_FILEPATH = './operators.csv'
PERIODS_CSV_FILEPATH = './periods.csv'
REASONS_CSV_FILEPATH = './reasons.csv'

logging.getLogger().addHandler(logging.StreamHandler())
logging.getLogger().setLevel(logging.INFO)

Base = declarative_base()

engine = create_engine('postgresql+psycopg2://german:german@localhost:5432/period_view')
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
                event_time=datetime.strptime(energy['event_time'], '%Y-%m-%d %H:%M:%S+03'),
                kwh=energy['kwh'],
            ) for idx, energy in energy_df.iterrows()
        ]
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
        operators_df.fillna(value='', inplace=True)
        operators = [Operators(
            endpoint_id=operator['endpoint_id'],
            login_time=datetime.strptime(operator['login_time'], '%Y-%m-%d %H:%M:%S.000000 +03:00'),
            logout_time=datetime.strptime(
                operator['logout_time'], '%Y-%m-%d %H:%M:%S.000000 +03:00'
            ) if operator['logout_time'] else None,
            operator_name=operator['operator_name']
        ) for idx, operator in operators_df.iterrows()]
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
                mode_start=datetime.strptime(period['mode_start'], '%Y-%m-%d %H:%M:%S+03'),
                mode_duration=period['mode_duration'],
                label=period['label']
            ) for idx, period in periods_df.iterrows()
        ]
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
                event_time=datetime.strptime(reason['event_time'], '%Y-%m-%d %H:%M:%S+03'),
                reason=reason['reason']
            ) for idx, reason in reasons_df.iterrows()
        ]

        return reasons

class PeriodView(Base):
    __tablename__ = 'period_view'

    id = Column(Integer, primary_key=True)
    endpoint_id = Column(Integer)
    mode_start = Column(DateTime)
    mode_end = Column(DateTime)
    mode_duration = Column(Integer)
    label = Column(String(64))
    reason = Column(String(128))
    operator_name = Column(String(128))
    energy_sum = Column(Float)

    @classmethod
    def create_periodviews(cls, reasons_list: List[Reasons], operators: List[Operators], periods: List[Periods],
                           energys: List[Energy]) -> List:
        operators = session.query(Operators)
        periods = session.query(Periods)

        period_views = list()
        sum_energy_per_period = 0
        last_time_energy = None
        reason_times = [reason.event_time for reason in reasons_list]
        for energy in sorted(energys, key=lambda energy: energy.event_time):
            if not last_time_energy:
                last_time_energy = energy.event_time

            if energy.event_time in reason_times:
                duration = energy.event_time - last_time_energy,
                operator = operators.filter(Operators.login_time <= last_time_energy).filter(
                    Operators.logout_time >= energy.event_time
                ).first()


                period_views.append(PeriodView(
                    endpoint_id=energy.endpoint_id,
                    mode_start=last_time_energy,
                    mode_end=energy.event_time,
                    mode_duration=duration[0].seconds // 60,
                    # label=periods.filter(
                    #     Periods.mode_start <= energy.event_time
                    # ).filter(
                    #     Periods.mode_start + timedelta(0, Periods.mode_duration*60) >= energy.event_time
                    # ).first().label,
                    operator_name=operator.operator_name if operator else '',
                    energy_sum=sum_energy_per_period,
                ))
                sum_energy_per_period = 0
                last_time_energy = energy.event_time
            sum_energy_per_period += energy.kwh
        return period_views


if __name__ == '__main__':
    reasons_list = Reasons.create_reasons_from_csv(REASONS_CSV_FILEPATH)
    logging.info('Reasons created {}'.format(len(reasons_list)))

    operators_list = Operators.create_operators_from_csv(OPERATORS_CSV_FILEPATH)
    logging.info('Operators created {}'.format(len(operators_list)))

    periods_list = Periods.create_periods_from_csv(PERIODS_CSV_FILEPATH)
    logging.info('Periods created {}'.format(len(periods_list)))

    energys_list = Energy.create_energy_from_csv(ENERGY_CSV_FILEPATH)
    logging.info('Energy created {}'.format(len(energys_list)))

    # session.add_all(reasons_list)
    # logging.info('Reasons add')
    # session.add_all(operators_list)
    # logging.info('Operators add')
    # session.add_all(periods_list)
    # logging.info('Periods add')
    # session.add_all(energys_list)
    # logging.info('Energy add')

    # Base.metadata.create_all(engine)

    #
    # session.commit()

    period_views = PeriodView.create_periodviews(reasons_list, operators_list, periods_list, energys_list)
    logging.info('Periods view created {}'.format(periods_list))
    session.add_all(period_views)
    logging.info('Periods view add')

    session.commit()
    session.close()
