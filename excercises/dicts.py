import copy

di = {"a": "apple", "b": "biscuit", 4: [3,4,5,6]}

d2 = di.copy()
d3 = copy.deepcopy(di)

if di is d2 :
    print("di is d2")
else:
    print("di is not d2")
    
di[4].append(90)

print(f"di = {di} d2 = {d2}")
print(f"di = {di} d3 = {d3}")

di[8] = (4,5,6,7)
di["h"] = {"x": "value"}
print(f"di = {di} ,  d2 = {d2}")
for x in di.keys() :
    print(f"{x} = {di[x]}")

animals = ["dog", "cat", "mouse"]
for i, value in enumerate(animals):
    print(i, value)