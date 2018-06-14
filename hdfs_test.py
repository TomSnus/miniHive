import luigi
import radb
import ra2mr
import sql2ra
import raopt
import sqlparse
# Take a relational algebra query...

raquery = radb.parse.one_statement_from_string("\project_{name} Person;")

dd = {}
dd["Person"] = {"name": "string", "age": "integer", "gender": "string"}
dd["Eats"] = {"name": "string", "pizza": "string"}
dd["Serves"] = {"pizzeria": "string", "pizza": "string", "price": "integer"}

stmt = sqlparse.parse("select distinct * from Person, Eats, Serves where Person.name = Eats.name and Eats.pizza = Serves.pizza and Person.age = 16 and Serves.pizzeria = 'Little Ceasars'")[0]
ra0 = sql2ra.translate(stmt)

ra1 = raopt.rule_break_up_selections(ra0)
ra2 = raopt.rule_push_down_selections(ra1, dd)

ra3 = raopt.rule_merge_selections(ra2)
ra4 = raopt.rule_introduce_joins(ra3)

# ... translate it into a luigi task encoding a MapReduce workflow...
task = ra2mr.task_factory(ra4, env=ra2mr.ExecEnv.HDFS)
# ... and run the task on Hadoop, using HDFS for input and output: # (for now, we are happy working with luigi's local scheduler).
luigi.build([task], local_scheduler=True)
