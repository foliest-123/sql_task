from sqlalchemy import URL, Column, Integer, Date, create_engine ,func , String , Float ,or_ , and_
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker


url = 'mysql://root:1234@localhost/sql_exercise'

Base = declarative_base()
engine = create_engine(url)
Session = sessionmaker(bind=engine)


#customer Table
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

# salesman
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

class Customer(Base):
    __tablename__ = 'customer'
    customer_id = Column(Integer, primary_key=True)
    cust_name = Column(String(255))
    city = Column(String(255))
    grade = Column(Integer)
    salesman_id = Column(Integer)

    def __init__(self, customer_id, cust_name, city, grade, salesman_id):
        self.customer_id = customer_id
        self.cust_name = cust_name
        self.city = city
        self.grade = grade
        self.salesman_id = salesman_id


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

class Employee(Base):
    __tablename__ = 'employee'

    emp_idno = Column(Integer, primary_key=True)
    emp_fname = Column(String(255))
    emp_lname = Column(String(255))
    emp_dept = Column(Integer)

    def __init__(self, emp_idno, emp_fname, emp_lname, emp_dept):
        self.emp_idno = emp_idno
        self.emp_fname = emp_fname
        self.emp_lname = emp_lname
        self.emp_dept = emp_dept

class Department(Base):
    __tablename__ = 'department'

    dpt_code = Column(Integer, primary_key=True)
    dpt_name = Column(String(255))
    dpt_allotment = Column(Integer)

    def __init__(self, dpt_code, dpt_name, dpt_allotment):
        self.dpt_code = dpt_code
        self.dpt_name = dpt_name
        self.dpt_allotment = dpt_allotment

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

commision_avg = session.query(func.avg(Salesman.commission)).\
                filter(Salesman.commission.between(0.12,0.14)).all()
# print(commision_avg)

# 3.
# From the following table, write a SQL query to find those employees who 
# worked more than or equal to  two jobs in the past. Return employee id. 

more_than_twojobs = session.query(EmployeeHistory.employee_id).distinct().\
    group_by(EmployeeHistory.employee_id).\
    having(func.count(EmployeeHistory.job_id) >= 2).\
    all()
# print(more_than_twojobs)

# 4.a) Write a SQL statement to generate a list of salesmen who either serve one or 
# more customers or have not joined any customer yet. The customers may have placed one or more orders 
# with an order amount equal to or exceeding 2000, and they must have a grade. Alternatively, customers may 
# not have placed any order with the associated supplier.(Use joins)
# Return cust_name, cust city, grade, Salesman name, ord_no, ord_date, purch_amt


# Customer.cust_name, Customer.city, Customer.grade,Salesman.name, Order.ord_no, Order.ord_date,max_purch_order.c.max_purch

purch_amt_2000 =session.query(Customer.cust_name, Customer.city, Customer.grade,
                             Salesman.name, Order.ord_no, Order.ord_date,
                             func.max(Order.purch_amt)).\
                            outerjoin(Customer,Customer.customer_id == Order.customer_id ).\
                                outerjoin(Salesman , Salesman.salesman_id == Order.salesman_id)\
                                    .group_by(Customer.customer_id).\
                                    having(Customer.grade != None, func.max(Order.purch_amt >= 2000))
                                 

# for row in purch_amt_2000:
#     print(row)
    
    
# b) Write a SQL statement to generate a report with the customer name, city, order number, order date, and purchase amount for 
# customers on the list who must have a grade and placed one or more orders. 
# Additionally, include orders placed by customers who are neither on the 
# list nor have a grade.(Use joins)


distinct_cust_id = session.query(Order.customer_id).distinct().subquery()
distinct_cust_ids = session.query(Order.customer_id)\
    .distinct().subquery()

placed_order =session.query(
    Customer.cust_name,
    Customer.city,
    Order.ord_no,
    Order.ord_date,
    Order.purch_amt
).outerjoin(Customer, Order.customer_id == Customer.customer_id).filter(
    or_(
        and_(Customer.grade != None, Order.customer_id.in_(distinct_cust_id)),
        and_(Customer.grade == None, Order.customer_id.not_in(distinct_cust_id))
    )
)
# for row in placed_order :
#     print(row)
    

5. 
# a) From the following tables write a SQL query to find those employees 
# who work for the department where the departmental allotment amount is more
# than Rs. 50000. Return emp_fname and emp_lname.

allotment_dept = session.query(Employee.emp_fname , Employee.emp_lname).\
    join(Department , Employee.emp_dept == Department.dpt_code).\
        where(Department.dpt_allotment >= 50000).all()


distinct_allotment = session.query(func.distinct(Department.dpt_allotment)).order_by(Department.dpt_allotment).slice(1, 2).subquery()

second_lowamount = session.query(Employee.emp_fname , Employee.emp_lname).\
    join(Department , Employee.emp_dept == Department.dpt_code).\
        filter(Department.dpt_allotment == distinct_allotment).all()
print(second_lowamount)