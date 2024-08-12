class QueryPlan:
    def __init__(self, plan_data):
        self.plan_data = plan_data

    def __eq__(self, other):
        if not isinstance(other, QueryPlan):
            return NotImplemented
        return self.compare(self.plan_data, other.plan_data) == []
    
    def compare(self, plan1, plan2, path=""):
        return []
    
    def get_differences(self, other):
        return self.compare(self.plan_data, other.plan_data)