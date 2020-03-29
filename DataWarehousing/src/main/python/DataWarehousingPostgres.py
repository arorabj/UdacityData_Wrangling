import configparser as cp
import sql
%load_ext sql

env='dev'
conf = cp.RawConfigParser()

conf.read('../../../resources/application.properties')
db_host = conf.get(env,'DBHOST')
db_user = conf.get(env,'USER')
db_password = conf.get(env,'PASSWORD')
db_schema = conf.get(env,'SCHEMA')
db_port = conf.get(env,'PORT')

conn_string = "postgresql://{}:{}@{}:{}/{}". format(db_user,db_password,db_host,db_port,db_schema)

print (conn_string)

%sql $conn_string


nStores = %sql select count(*) from store;
nFilms = %sql select count(*) from film;
nCustomers = %sql select count(*) from customer;
nRentals = %sql select count(*) from rental;
nPayment = %sql select count(*) from payment;
nStaff = %sql select count(*) from staff;
nCity = %sql select count(*) from city;
nCountry = %sql select count(*) from country;

print("nFilms\t\t=", nFilms[0][0])
print("nCustomers\t=", nCustomers[0][0])
print("nRentals\t=", nRentals[0][0])
print("nPayment\t=", nPayment[0][0])
print("nStaff\t\t=", nStaff[0][0])
print("nStores\t\t=", nStores[0][0])
print("nCities\t\t=", nCity[0][0])
print("nCountry\t\t=", nCountry[0][0])

%%sql
select min(payment_date) as start, max(payment_date) as end from payment;

%%sql
select district, count(*) n
from address
group by district
order by n desc


%%time
%%sql
SELECT month,country, sum(sales_amount) as revenue
FROM  factsales
join dimdate
on factsales.date_key=dimdate.date_key
join dimstore
on factsales.store_key=dimstore.store_key
group by grouping sets ((), month, country,(month, country)

%%time
%%sql
SELECT month,country, sum(sales_amount) as revenue
FROM  factsales
join dimdate
on factsales.date_key=dimdate.date_key
join dimstore
on factsales.store_key=dimstore.store_key
group by cube(month, country)
