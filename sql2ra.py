import radb.ast
#Copyright: Martin Meier 

SELECT = "SELECT"
DISTINCT = "DISTINCT"
FROM = "FROM "
WHERE = "WHERE "
AND = radb.ast.literal(radb.ast.sym.AND) + " "
STAR = radb.ast.literal(radb.ast.sym.STAR)
EQ = radb.ast.literal(radb.ast.sym.EQ)
DOT = radb.ast.literal(radb.ast.sym.DOT)


def translate(stmt):
    statement = " ".join(str(stmt).split())

    try:
        attributes, relations, conditions = parse_input(statement)
        result = create_project(attributes, create_select(conditions, create_cross(relations)))

        return result
    except AssertionError:
        return error()
    except IndexError:
        return error()


def error():
    return "The Syntax of your statement is incorrect."


def parse_input(statement):
    return (parse_attributes(statement, statement.upper()),
            parse_relations(statement, statement.upper()),
            parse_conditions(statement, statement.upper()))


def parse_attributes(statement, upper_statement):
    begin_index = upper_statement.find(SELECT) + len(SELECT)
    mod_statement = upper_statement[begin_index:].strip()
    if mod_statement.startswith(DISTINCT):
        begin_index += len(DISTINCT) + 1

    end_index = upper_statement.find(FROM)
    return statement[begin_index:end_index].split(',')


def parse_relations(statement, upper_statement):
    begin_index = upper_statement.find(FROM) + len(FROM)
    end_index = upper_statement.find(WHERE)

    if end_index < 0:
        end_index = len(statement)

    return statement[begin_index:end_index].split(',')


def parse_conditions(statement, upper_statement):
    begin_index = upper_statement.find(WHERE)
    if begin_index < 0:
        return ""
    else:
        begin_index += len(WHERE)

    end_index = len(statement)

    return statement[begin_index:end_index].replace(AND.lower(), AND).split(AND)


def create_project(attributes, statement):
    if attributes[0].strip() == STAR:
        return statement
    else:
        for i in range(0, len(attributes)):
            attributes[i] = create_attribute(attributes[i])

    return radb.ast.Project(attributes, statement)


def create_cross(relations):
    joined_relations = create_rename(relations[0])
    for i in range(1, len(relations)):
        joined_relations = radb.ast.Cross(joined_relations, create_rename(relations[i].strip()))

    return joined_relations


def create_rename(relation):
    values = relation.strip().split(" ")
    if len(values) == 1:
        return radb.ast.RelRef(relation.strip())
    elif len(values) == 2:
        return radb.ast.Rename(values[1].strip(), None, radb.ast.RelRef(values[0].strip()))


def create_attribute(attribute):
    values = attribute.split(DOT)
    if len(values) == 1:
        return radb.ast.AttrRef(None, attribute.strip())
    elif len(values) == 2:
        return radb.ast.AttrRef(values[0].strip(), values[1].strip())


def create_select(conditions, statement):
    if len(conditions) == 0:
        # no select needed
        return statement

    for i in range(0, len(conditions)):
        values = conditions[i].split(EQ)
        conditions[i] = radb.ast.ValExprBinaryOp(create_attribute(values[0]), radb.ast.sym.EQ,
                                                 create_attribute(values[1]))

    joined_conditions = conditions[0]
    for i in range(1, len(conditions)):
        joined_conditions = radb.ast.ValExprBinaryOp(joined_conditions, radb.ast.sym.AND, conditions[i])

    return radb.ast.Select(joined_conditions, statement)
