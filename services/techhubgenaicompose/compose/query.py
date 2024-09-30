
### This code is property of the GGAO ###

from .query_actions import ExpansionFactory


def expansion(exp_type, params, query, actions_confs):
        exp = ExpansionFactory(exp_type)
        return exp.process(query, params, actions_confs)
    