
def count(input: list[int]) -> int:
    count = 0
    highest = 0
    
    for x in input :
        if x > highest :
            highest = x
            count += 1
            
    return count

if __name__ == '__main__':
    x = [3,4,5,2,3,8,6,4,5]
    r = count(x)
    
    print(f"output: {r}")