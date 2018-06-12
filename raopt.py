import radb.ast
#Copyright: Martin Meier 

def rule_break_up_selections(ra):
    statement_inputs = []

    if ra.inputs is not None:
        for i in range(0, len(ra.inputs)):
            statement_inputs.append(rule_break_up_selections(ra.inputs[i]))

        if isinstance(ra, radb.ast.Select):
            return select_conversion(ra)

    ra.inputs = statement_inputs
    return ra


def select_conversion(ra):
    conditions = split_conditions(ra.cond.inputs)

    if not isinstance(conditions[0], radb.ast.ValExprBinaryOp):
        return ra

    ra = radb.ast.Select(conditions[len(conditions) - 1], ra.inputs[0])
    for i in range(len(conditions) - 2, -1, -1):
        ra = radb.ast.Select(conditions[i], ra)
    return ra


def split_conditions(conditions):
    if len(conditions) > 1 and isinstance(conditions[0], radb.ast.AttrRef):
        return conditions

    result = []
    for condition in conditions:
        if isinstance(condition.inputs[0], radb.ast.ValExprBinaryOp):
            result.extend(split_conditions(condition.inputs))
        else:
            result.append(condition)
    return result


def rule_push_down_selections(ra, dd):
    if isinstance(ra, radb.ast.Select):
        ra = push_down_selections(ra, dd)
    else:
        statement_inputs = []
        if ra.inputs is not None:
            for i in range(0, len(ra.inputs)):
                statement_inputs.append(rule_push_down_selections(ra.inputs[i], dd))

        ra.inputs = statement_inputs

    return ra


def push_down_selections(ra, dd):
    selections = [ra]
    first_cross_join = None

    while selections[- 1].inputs is not None:
        inputs = selections[- 1].inputs[0]
        if isinstance(inputs, radb.ast.Select):
            selections.append(inputs)
        elif isinstance(inputs, radb.ast.Cross):
            first_cross_join = inputs
            break
        else:
            return ra

    return build_pushed_down_selections_ra(first_cross_join, selections, dd)


def build_pushed_down_selections_ra(first_cross_join, selections, dd):
    new_ra = first_cross_join
    for selection in selections:
        insertion_point = new_ra
        previous_statement = None
        previous_index = None
        while insertion_point is not None:
            if isinstance(insertion_point, radb.ast.Cross):
                for i in range(len(insertion_point.inputs)):
                    if isinstance(insertion_point.inputs[i], radb.ast.Cross):
                        continue
                    else:
                        insertion_index = get_selection_insertion_index(selection, insertion_point.inputs[i], dd, i)
                        if insertion_index is None:
                            # insertion point found
                            if previous_statement is None:
                                selection.inputs[0] = new_ra
                                new_ra = selection
                            else:
                                selection.inputs[0] = previous_statement.inputs[previous_index]
                                previous_statement.inputs[i] = selection
                            insertion_point = None
                        else:
                            previous_statement = insertion_point
                            previous_index = insertion_index
                            insertion_point = insertion_point.inputs[insertion_index]

                        break

            elif isinstance(insertion_point, radb.ast.Select):
                previous_statement = insertion_point
                previous_index = 0
                insertion_point = insertion_point.inputs[0]

            else:
                selection.inputs[0] = previous_statement.inputs[previous_index]
                previous_statement.inputs[previous_index] = selection
                break

    return new_ra


def get_selection_insertion_index(selection, relation, dd, index):
    relation = extract_relation(relation)
    relations = get_all_relations_of_cross(relation)

    left_attribute_included = check_attribute_included(relations, selection.cond.inputs[0], dd)

    right_attribute_included = left_attribute_included
    if len(selection.cond.inputs) > 1 and isinstance(selection.cond.inputs[1], radb.ast.AttrRef):
        if len(get_possible_relations_of_attribute(selection.cond.inputs[1], dd)) > 0:
            right_attribute_included = check_attribute_included(relations, selection.cond.inputs[1], dd)

    if right_attribute_included and left_attribute_included:
        return index
    elif not right_attribute_included and not left_attribute_included:
        return (index + 1) % 2  # invert the index
    else:
        return None


def extract_relation(relation):
    while isinstance(relation, radb.ast.Select):
        #  step over all selects
        relation = relation.inputs[0]

    if isinstance(relation, radb.ast.Rename):
        relation = relation.relname

    return relation


def check_attribute_included(relations, attribute, dd):
    possible_relations = get_possible_relations_of_attribute(attribute, dd)
    for relation in relations:
        if str(relation) in possible_relations:
            return True
    return False


def get_all_relations_of_cross(relation):
    if isinstance(relation, radb.ast.Cross):
        result = []
        result.extend(get_all_relations_of_cross(relation.inputs[0]))
        result.extend(get_all_relations_of_cross(relation.inputs[1]))
        return result
    else:
        return [relation]


def get_possible_relations_of_attribute(attribute, dd):
    if attribute.rel is not None:
        return {attribute.rel}
    else:
        possible_relations = []
        for relation in dd:
            if attribute.name in dd[relation]:
                possible_relations.append(relation)
        return possible_relations


def rule_merge_selections(ra):
    statement_inputs = []

    if ra.inputs is not None:
        if isinstance(ra, radb.ast.Select):
            return merge_selections(ra)

        for i in range(0, len(ra.inputs)):
            statement_inputs.append(rule_merge_selections(ra.inputs[i]))

    ra.inputs = statement_inputs
    return ra


def merge_selections(ra):
    while ra.inputs is not None and isinstance(ra.inputs[0], radb.ast.Select):
        ra.cond = radb.ast.ValExprBinaryOp(ra.cond, radb.ast.sym.AND, ra.inputs[0].cond)
        ra.inputs[0] = ra.inputs[0].inputs[0]
    return ra


def rule_introduce_joins(ra):
    statement_inputs = []

    if ra.inputs is not None:
        for i in range(0, len(ra.inputs)):
            statement_inputs.append(rule_introduce_joins(ra.inputs[i]))

    ra.inputs = statement_inputs

    if isinstance(ra, radb.ast.Select):
        if check_join_conversion(ra):
            ra = radb.ast.Join(ra.inputs[0].inputs[0], ra.cond, ra.inputs[0].inputs[1])
    return ra


def check_join_conversion(ra):
    if not isinstance(ra.cond, radb.ast.ValExprBinaryOp):
        return False

    sub_statement = ra.inputs[0]
    if not isinstance(sub_statement, radb.ast.Cross) and not isinstance(sub_statement, radb.ast.Join):
        return False

    if isinstance(ra.cond.inputs[0], radb.ast.ValExprBinaryOp):
        for condition in ra.cond.inputs:
            relation_left_attribute = get_attribute_relation(condition.inputs[0], sub_statement)
            relation_right_attribute = get_attribute_relation(condition.inputs[1], sub_statement)
            if relation_left_attribute == relation_right_attribute:
                return False
        return True
    else:
        relation_left_attribute = get_attribute_relation(ra.cond.inputs[0], sub_statement)
        relation_right_attribute = get_attribute_relation(ra.cond.inputs[1], sub_statement)
        return relation_left_attribute != relation_right_attribute


def get_attribute_relation(attribute, relation):
    attribute_relation = attribute.rel

    if isinstance(relation.inputs[0], radb.ast.Rename) or isinstance(relation.inputs[0], radb.ast.RelRef):
        sub_relation_index = 0
    else:
        sub_relation_index = 1

    rel_name = get_rel_name(relation.inputs[sub_relation_index])

    if rel_name == attribute_relation:
        return sub_relation_index
    else:
        return (sub_relation_index + 1) % 2


def get_rel_name(relation):
    if isinstance(relation, radb.ast.Rename):
        return relation.relname
    elif isinstance(relation, radb.ast.RelRef):
        return relation.rel
    else:
        return get_rel_name(relation.inputs[0])
