
### This code is property of the GGAO ###

from .query_actions import ExpansionFactory, FilterFactory, ReformulateFactory


def expansion(exp_type, params, query, actions_confs):
        exp = ExpansionFactory(exp_type)
        return exp.process(query, params, actions_confs)
    
def filter_query(filter_type, params, query, actions_confs):
        filter_f= FilterFactory(filter_type)
        return filter_f.process(query, params, actions_confs)

def reformulate_query(reformulate_type, params, query, actions_confs):
        reformulate_f = ReformulateFactory(reformulate_type)
        return reformulate_f.process(query, params, actions_confs)