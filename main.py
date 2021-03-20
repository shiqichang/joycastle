#!/usr/bin/python
# -*- coding: UTF-8 -*-
# @Author   : shiqi.chang
# @Filename : main.py
# @Software : PyCharm
import re
from datetime import date

import pandas as pd

from sqlalchemy import Column, Integer, String, Date, ForeignKey, Table
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship


# ------------------------------------------------------------
# 问题一
#
# 1. HTTPS 请求有多少个以 domain1.com 为域名
# 2. 给定一个日期 date, 计算当日所有请求中请求成功的比例
# ------------------------------------------------------------

class NginxLogStats(object):
    """ Nginx 日志统计 """

    def __init__(self):
        self.filename = "logs/nginx.log"

    def set_dataframe(self):
        data = []
        with open(self.filename, 'r') as f:
            for s in f:
                rr = re.compile(
                    r'\[(.*?):.*\].*HTTP/2.0\" (\d+).*https://(.*?)/\?')
                raw = rr.findall(s)[0]
                data.append({'date': raw[0], 'status_code': raw[1],
                             'domain': raw[2]})
        df = pd.DataFrame(data)
        return df

    def stats(self, date):
        df = self.set_dataframe()

        # 1. HTTPS 请求有多少个以 domain1.com 为域名
        cnt_dom = df[df['domain'] == 'domain1.com']['domain'].count()

        # 2. 给定一个日期 date, 计算当日所有请求中请求成功的比例
        df_dt = df[df['date'] == date]
        cnt_dt = df_dt['date'].count()
        cnt_sc = df_dt[df_dt['status_code'] == '200']['status_code'].count()
        rate = round(cnt_sc / cnt_dt, 2)

        return cnt_dom, rate


# ------------------------------------------------------------
# 问题二
#
# 编写 SQL, 查询有多少用户在2020年9月开启关卡数大于等于1000小于2000
# ------------------------------------------------------------

sql = """
SELECT COUNT(user_id) FROM (
    SELECT user_id, COUNT(event_timestamp) as cnt FROM event_log 
    WHERE event_timestamp >= '1598889600' AND event_timestamp < '1601481600' 
    GROUP BY user_id
) a WHERE a.cnt >= 1000 AND a.cnt < 2000"""


# ------------------------------------------------------------
# 问题三
#
# 根据规则建立国际象棋比赛数据模型
# ------------------------------------------------------------

Base = declarative_base()  # 基类


class Clubs(Base):
    """ 俱乐部表 """

    __tablename__ = 'clubs'  # 表名

    id = Column(Integer, primary_key=True, autoincrement=True)  # 主键ID, 自增长
    name = Column(String(32), nullable=False, comment='名称')
    address = Column(String(64), nullable=False, comment='地址')

    members = relationship('Members', backref='clubs')  # 一对多
    tournaments = relationship('Tournaments', backref='clubs')


class Players(Base):
    """ 棋手表 """

    __tablename__ = 'players'

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(32), nullable=False, comment='名称')
    address = Column(String(64), nullable=False, comment='地址')
    rank = Column(Integer, nullable=False, comment='排名')

    tournaments = relationship('Tournaments', backref='players')
    member = relationship('Members', backref='players', uselist=False)  # 一对一


class Members(Base):
    """ 会员表 """

    __tablename__ = 'members'

    id = Column(Integer, primary_key=True, autoincrement=True)

    club_id = Column(Integer, ForeignKey('clubs.id'))  # 外键关联 clubs 表
    player_id = Column(Integer, ForeignKey('players.id'))


players_tournaments = Table(  # 棋手和锦标赛的中间表，多对多
    'players_tournaments',
    Base.metadata,
    Column('player_id', Integer, ForeignKey('players.id')),
    Column('tournament_id', Integer, ForeignKey('tournaments.id'),
           nullable=True)
)


class Tournaments(Base):
    """ 锦标赛表 """

    __tablename__ = 'tournaments'

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(32), nullable=False, comment='名称')
    sponsors = Column(String(32), nullable=True, comment='赞助方')
    start_dt = Column(Date, default=date.today, comment='开始日期')
    end_dt = Column(Date, default=date.today, comment='结束日期')

    matches = relationship('Matches', backref='tournaments')
    players = relationship('Players', backref='tournaments',
                           secondary=players_tournaments)

    club_id = Column(Integer, ForeignKey('clubs.id'))


class Matches(Base):
    """ 比赛表 """

    __tablename__ = 'matches'

    id = Column(Integer, primary_key=True, autoincrement=True)

    tournament_id = Column(Integer, ForeignKey('tournaments.id'),
                           nullable=True)


if __name__ == '__main__':
    log_stats = NginxLogStats()
    print(log_stats.stats('27/Feb/2019'))
