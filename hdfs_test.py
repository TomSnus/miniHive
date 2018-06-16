import luigi
import radb
import ra2mr
# Take a relational algebra query...

raquery = radb.parse.one_statement_from_string("\project_{name} Person;")
# ... translate it into a luigi task encoding a MapReduce workflow...
task = ra2mr.task_factory(raquery, env=ra2mr.ExecEnv.HDFS)
# ... and run the task on Hadoop, using HDFS for input and output: # (for now, we are happy working with luigi's local scheduler).
luigi.build([task], local_scheduler=True)
