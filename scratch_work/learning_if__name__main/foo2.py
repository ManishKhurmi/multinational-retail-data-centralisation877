# Suppose this is foo2.py.
import os, sys; sys.path.insert(0, os.path.dirname(__file__)) # needed for some interpreters

def function_a():
    print("a1")
    from foo2 import function_b # This is an import from foo2 !!!!!!!!!
    print("a2")
    function_b()
    print("a3")

def function_b():
    print("b")

print("t1")
if __name__ == "__main__":
    print("m1")
    function_a()
    print("m2")
print("t2")

#########################################
# my prediction:
# t1
# m1 
# a1
######## incorrect past here ##########3
# a2 
# Function B 10.0
# a3
# m2
# t2

#########################################
# Correct result:
# t1
# m1
# a1
## we are up to 'from foo2 import function_b'
####################
# t1        # --> We then get t1, t2 because foo2 runs through but misses the part in the if __name__=='__main__' block
# t2        # ---^
####################
# a2
# b
# a3
# m2
# t2