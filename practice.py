from sqlalchemy import URL, Column, Integer, Date, create_engine ,func , String , Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker


url = 'mysql://root:1234@localhost/sql_exercise'

Base = declarative_base()
engine = create_engine(url)
Session = sessionmaker(bind=engine)

class Order(Base):
    __tablename__ = 'orders'

    ord_no = Column(Integer, primary_key=True)
    purch_amt = Column(Integer)
    ord_date = Column(Date)
    customer_id = Column(Integer)
    salesman_id = Column(Integer)

    def __init__(self, ord_no, purch_amt, ord_date, customer_id, salesman_id):
        self.ord_no = ord_no
        self.purch_amt = purch_amt
        self.ord_date = ord_date
        self.customer_id = customer_id
        self.salesman_id = salesman_id

class Salesman(Base):
    __tablename__ = 'salesman'

    salesman_id = Column(Integer, primary_key=True)
    name = Column(String(255))
    city = Column(String(255))
    commission = Column(Float)

    def __init__(self, name, city, commission):
        self.name = name
        self.city = city
        self.commission = commission


class EmployeeHistory(Base):
    __tablename__ = 'employee_history'

    employee_id = Column(Integer, primary_key=True)
    start_date = Column(Date)
    end_date = Column(Date)
    job_id = Column(String(50))
    department_id = Column(Integer)

    def __init__(self, employee_id, start_date, end_date, job_id, department_id):
        self.employee_id = employee_id
        self.start_date = start_date
        self.end_date = end_date
        self.job_id = job_id
        self.department_id = department_id
Base.metadata.create_all(engine)
session = Session()

# 1. 
# a) Write a SQL query for the provided table to retrieve the first five unique salespeople 
# IDs in order based on higher purchase amounts, where each salesperson's purchase amount should not exceed 2000


max_purch_order = session.query(Order.salesman_id, func.max(Order.purch_amt).label('max_purch')) \
    .group_by(Order.salesman_id) \
    .subquery()
    
max_purch = session.query(max_purch_order.c.salesman_id, max_purch_order.c.max_purch) \
    .filter(max_purch_order.c.max_purch <= 2000) \
    .order_by(max_purch_order.c.max_purch.desc()) \
    .limit(5) \
    .all()

min_purch_order = session.query(Order.salesman_id, func.min(Order.purch_amt).label('min_purch')) \
    .group_by(Order.salesman_id) \
    .subquery()
    
# b) Write a SQL query for the provided table to retrieve the first five unique salespeople IDs in order based on lower purchase amounts, 
# where each salesperson's purchase amount should exceed 100.

min_purch = session.query(min_purch_order.c.salesman_id, min_purch_order.c.min_purch) \
    .filter(min_purch_order.c.min_purch <= 100) \
    .order_by(min_purch_order.c.min_purch.desc()) \
    .limit(5) \
    .all()
# Printing the queried max_purch
for row in min_purch:
    # print(row)
    pass
session.commit()
session.close()


# 2. 
# a) Write a SQL query for the given table to retrieve details of salespeople with 
# commissions ranging from 0.10 to 0.12.(Begin and end values are included.) Return salesman_id, name, city, and commission.

commission_range = session.query(Salesman.salesman_id,Salesman.commission).filter(Salesman.commission.between(0.10,0.12)).all()
# print(commission_range)

# b) Write a SQL query for the given table to retrieve avg details of commissions 
# ranging from 0.12 to 0.14.(Begin and end values are included.)

commision_avg = session.query(func.avg(Salesman.commission)).filter(Salesman.commission.between(0.12,0.14)).all()
# print(commision_avg)

# 3.
# From the following table, write a SQL query to find those employees who 
# worked more than or equal to  two jobs in the past. Return employee id. 

more_than_twojob = session.query(EmployeeHistory.employee_id ,func.count(EmployeeHistory.job_id)).distinct().group_by(EmployeeHistory.employee_id).all()
print(more_than_twojob)

