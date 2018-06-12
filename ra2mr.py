from enum import Enum
import json
import luigi
import luigi.contrib.hadoop
import luigi.contrib.hdfs
from luigi.mock import MockTarget
import radb
import radb.ast
import radb.parse

'''
Control where the input data comes from, and where output data should go.
'''


class ExecEnv(Enum):
    LOCAL = 1  # read/write local files
    HDFS = 2  # read/write HDFS
    MOCK = 3  # read/write mock data to an in-memory file system.


'''
Switches between different execution environments and file systems.
'''


class OutputMixin(luigi.Task):
    exec_environment = luigi.EnumParameter(enum=ExecEnv, default=ExecEnv.HDFS)

    def get_output(self, fn):
        if self.exec_environment == ExecEnv.HDFS:
            return luigi.contrib.hdfs.HdfsTarget(fn)
        elif self.exec_environment == ExecEnv.MOCK:
            return MockTarget(fn)
        else:
            return luigi.LocalTarget(fn)


class InputData(OutputMixin):
    filename = luigi.Parameter()

    def output(self):
        return self.get_output(self.filename)


'''
Counts the number of steps / luigi tasks that we need for evaluating this query.
'''


def count_steps(raquery):
    assert (isinstance(raquery, radb.ast.Node))

    if (isinstance(raquery, radb.ast.Select) or isinstance(raquery, radb.ast.Project) or
            isinstance(raquery, radb.ast.Rename)):
        return 1 + count_steps(raquery.inputs[0])

    elif isinstance(raquery, radb.ast.Join):
        return 1 + count_steps(raquery.inputs[0]) + count_steps(raquery.inputs[1])

    elif isinstance(raquery, radb.ast.RelRef):
        return 1

    else:
        raise Exception("count_steps: Cannot handle operator " + str(type(raquery)) + ".")


class RelAlgQueryTask(luigi.contrib.hadoop.JobTask, OutputMixin):
    '''
    Each physical operator knows its (partial) query string.
    As a string, the value of this parameter can be searialized
    and shipped to the data node in the Hadoop cluster.
    '''
    querystring = luigi.Parameter()

    '''
    Each physical operator within a query has its own step-id.
    This is used to rename the temporary files for exhanging
    data between chained MapReduce jobs.
    '''
    step = luigi.IntParameter(default=1)

    '''
    In HDFS, we call the folders for temporary data tmp1, tmp2, ...
    In the local or mock file system, we call the files tmp1.tmp...
    '''

    def output(self):
        if self.exec_environment == ExecEnv.HDFS:
            filename = "tmp" + str(self.step)
        else:
            filename = "tmp" + str(self.step) + ".tmp"
        return self.get_output(filename)


'''
Given the radb-string representation of a relational algebra query,
this produces a tree of luigi tasks with the physical query operators.
'''


def task_factory(raquery, step=1, env=ExecEnv.HDFS):
    assert (isinstance(raquery, radb.ast.Node))

    if isinstance(raquery, radb.ast.Select):
        return SelectTask(querystring=str(raquery) + ";", step=step, exec_environment=env)

    elif isinstance(raquery, radb.ast.RelRef):
        filename = raquery.rel + ".json"
        return InputData(filename=filename, exec_environment=env)

    elif isinstance(raquery, radb.ast.Join):
        return JoinTask(querystring=str(raquery) + ";", step=step, exec_environment=env)

    elif isinstance(raquery, radb.ast.Project):
        return ProjectTask(querystring=str(raquery) + ";", step=step, exec_environment=env)

    elif isinstance(raquery, radb.ast.Rename):
        return RenameTask(querystring=str(raquery) + ";", step=step, exec_environment=env)

    else:
        # We will not evaluate the Cross product on Hadoop, too expensive.
        raise Exception("Operator " + str(type(raquery)) + " not implemented (yet).")


class JoinTask(RelAlgQueryTask):

    def requires(self):
        raquery = radb.parse.one_statement_from_string(self.querystring)
        assert (isinstance(raquery, radb.ast.Join))

        task1 = task_factory(raquery.inputs[0], step=self.step + 1, env=self.exec_environment)
        task2 = task_factory(raquery.inputs[1], step=self.step + count_steps(raquery.inputs[0]) + 1,
                             env=self.exec_environment)

        return [task1, task2]

    def mapper(self, line):
        relation, tuple = line.split('\t')
        json_tuple = json.loads(tuple)

        raquery = radb.parse.one_statement_from_string(self.querystring)
        condition = raquery.cond

        if isinstance(condition.inputs[0], radb.ast.AttrRef):
            result = JoinTask.extract_key(condition, json_tuple)
            yield (result[1], [result[0], tuple])
        elif isinstance(condition.inputs[0], radb.ast.ValExprBinaryOp):
            relation = None
            attributes = []
            for sub_condition in condition.inputs:
                result = JoinTask.extract_key(sub_condition, json_tuple)
                if relation is None:
                    relation = result[0]
                attributes.append(result[1])
            yield (str(attributes), [relation, tuple])
            # yield (str(attributes), [self.step, tuple])

    @staticmethod
    def extract_key(condition, json_tuple):
        if str(condition.inputs[0]) in json_tuple:
            return str(condition.inputs[0].rel), json_tuple[str(condition.inputs[0])]
        elif condition.inputs[0].name in json_tuple:
            return str(condition.inputs[0].rel), json_tuple[condition.inputs[0].name]
        elif str(condition.inputs[1]) in json_tuple:
            return str(condition.inputs[1].rel), json_tuple[str(condition.inputs[1])]
        else:
            return str(condition.inputs[1].rel), json_tuple[condition.inputs[1].name]

    def reducer(self, key, values):
        relation_1 = None
        relation_2 = None
        first_relation_records = []
        second_relation_records = []

        for value in values:
            if relation_1 is None:
                relation_1 = value[0]

            entry = json.loads(value[1])
            if relation_1 == value[0]:
                if entry not in first_relation_records:
                    first_relation_records.append(entry)
            else:
                if relation_2 is None:
                    relation_2 = value[0]
                if entry not in second_relation_records:
                    second_relation_records.append(entry)

        for record_1 in first_relation_records:
            for record_2 in second_relation_records:
                yield (self.step, JoinTask.join_values(record_1, record_2))

    @staticmethod
    def join_values(first_record, second_record):
        result = dict(first_record)
        result.update(second_record)
        return json.dumps(result)


class SelectTask(RelAlgQueryTask):

    def requires(self):
        raquery = radb.parse.one_statement_from_string(self.querystring)
        assert (isinstance(raquery, radb.ast.Select))

        return [task_factory(raquery.inputs[0], step=self.step + 1, env=self.exec_environment)]

    def mapper(self, line):
        relation, tuple = line.split('\t')
        json_tuple = json.loads(tuple)

        condition = radb.parse.one_statement_from_string(self.querystring).cond

        if SelectTask.check_conditions(condition, relation, json_tuple):
            yield (relation, tuple)

    @staticmethod
    def check_conditions(condition, relation, json_tuple):
        if isinstance(condition.inputs[0], radb.ast.AttrRef) or isinstance(condition.inputs[0], radb.ast.RAString):
            left_attribute = SelectTask.add_relation_to_attribute(condition.inputs[0], relation)
            right_attribute = SelectTask.add_relation_to_attribute(condition.inputs[1], relation)

            if isinstance(left_attribute, radb.ast.AttrRef) and left_attribute.rel is not None:
                if str(left_attribute) in json_tuple:
                    return str(json_tuple[str(left_attribute)]) == SelectTask.extract_value(right_attribute)
                elif left_attribute.name in json_tuple:
                    return str(json_tuple[left_attribute.name]) == SelectTask.extract_value(right_attribute)
            elif isinstance(right_attribute, radb.ast.AttrRef) and right_attribute.rel is not None:
                if str(right_attribute) in json_tuple:
                    return str(json_tuple[str(right_attribute)]) == SelectTask.extract_value(left_attribute)
                elif right_attribute.name in json_tuple:
                    return str(json_tuple[right_attribute.name]) == SelectTask.extract_value(left_attribute)

            return True
        else:
            if not SelectTask.check_conditions(condition.inputs[0], relation, json_tuple):
                return False
            return SelectTask.check_conditions(condition.inputs[1], relation, json_tuple)

    @staticmethod
    def add_relation_to_attribute(attribute, relation):
        if isinstance(attribute, radb.ast.AttrRef) and not attribute.name.startswith('\'') and attribute.rel is None:
            attribute.rel = relation
        return attribute

    @staticmethod
    def extract_value(attribute):
        if isinstance(attribute, radb.ast.RAString):
            return attribute.val.replace('\'', '')
        else:
            return attribute.val


class RenameTask(RelAlgQueryTask):

    def requires(self):
        raquery = radb.parse.one_statement_from_string(self.querystring)
        assert (isinstance(raquery, radb.ast.Rename))

        return [task_factory(raquery.inputs[0], step=self.step + 1, env=self.exec_environment)]

    def mapper(self, line):
        relation, tuple = line.split('\t')
        json_tuple = json.loads(tuple)

        raquery = radb.parse.one_statement_from_string(self.querystring)

        corrected_dict = {k.replace(relation, raquery.relname): v for k, v in json_tuple.items()}

        yield (relation, str(corrected_dict).replace('\'', '\"'))


class ProjectTask(RelAlgQueryTask):

    def requires(self):
        raquery = radb.parse.one_statement_from_string(self.querystring)
        assert (isinstance(raquery, radb.ast.Project))

        return [task_factory(raquery.inputs[0], step=self.step + 1, env=self.exec_environment)]

    def mapper(self, line):
        relation, tuple = line.split('\t')
        json_tuple = json.loads(tuple)

        raquery = radb.parse.one_statement_from_string(self.querystring)

        result = {}
        for attribute in raquery.attrs:
            if attribute.name in json_tuple:
                attribute = attribute.name
            else:
                attribute = str(SelectTask.add_relation_to_attribute(attribute, relation))
            result[attribute] = json_tuple[attribute]

        yield (str(result).replace('\'', '\"'), relation)

    def reducer(self, key, values):
        yield (next(values), key)


if __name__ == '__main__':
    luigi.run()
