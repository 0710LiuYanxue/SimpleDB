select count(id), sum(age), sum(score), avg(score), max(score), min(score) 
from t1 group by id % 3"