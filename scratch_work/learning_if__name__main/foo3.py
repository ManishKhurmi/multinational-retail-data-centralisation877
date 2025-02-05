# Suppose this is foo3.py.
import os, sys; sys.path.insert(0, os.path.dirname(__file__)) # needed for some interpreters

def function_a():
    print("a1")
    from foo3 import function_b
    print("a2")
    function_b()
    print("a3")

def function_b():
    print("b")

print("t1")
print("m1")
function_a()
print("m2")
print("t2")


# # my prediction of the output from running -> python foo3.py
# print("t1") 
# # t1 
# print("m1")
# # m1
# function_a()
# # a1 
# ## we are now at-> from foo3 import function_b
# # t1 
# # m1 
# # a1
# ## we are mow at-> from foo3 import function_b 
# ### Infinite loop 
# print("m2")
# print("t2")

# # conclusion -> Ends up in an infinite loop