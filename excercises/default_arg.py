class Employee:
    company = "ACME Corp"
    # name:str 
    # age:int
    
        
    def __init__(self, n:str, a:int=20) -> None:
        self.name = n
        self.age = a
        
    def __str__(self) -> str:
        # print(f"\"{self.name}\" = {self.age}")
        return f'{{"name": "{self.name}", "age": {self.age}}}'
    
    def calculatSalary(self, items:list[int]=[]) -> int :
        """ func to test memory leak in default args
        """
        if len(items) == 0:
            items = [x * 2 for x in range(5)]
        
        total = 0    
        for x in items:
            total += x
            
        items.append( total)
        
        return total 
    
    @staticmethod
    def add(x:int, y:int) -> int:
        return x+y
    
    @classmethod
    def whoami(cls) -> str :
        return f'my name is {cls.__name__}'
    
    @classmethod
    def from_dict(cls, data: dict) -> 'Employee':
        """Creates an Employee instance from a dictionary (alternate constructor)."""
        return cls(n=data['name'], a=data['age'])
    

if __name__ == '__main__' :
    p = Employee("John", 34)
    print(p)
    print(Employee.add(4,5))
    print(Employee.whoami())

    # Using the alternate constructor
    employee_data = '{"name": "Jane", "age": 29}'
    p2 = Employee.from_dict(eval(employee_data))
    
    print(f"Created from dict: {p2}")
    
    # To modify the class variable for all instances, change it on the class itself
    Employee.company = "Tesla"
    print(f'Company for p: {p.company}, Company for p2: {p2.company}')
    
    ###
    print(f'salary p1 = {p.calculatSalary()}')
    print(f'salary p2 = {p2.calculatSalary()}')
