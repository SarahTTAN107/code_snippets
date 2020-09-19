import argparse

# argparse is a library for writing command-line interfaces with python
# this file demonstates some of the codes that I would definitely need frequently in data engineering work

parser = argparse.ArgumentParser(
    description="Replace this text with description of your pipeline"
    )

# add arguments to parser
# there are 2 kinds of arguments: 
# positional arguments, and optional arguments
### positional arguments:
### by default, type = str, so we need to tailor the type
### accordingly. We can set default for the arguments with default.
"""
parser.add_argument('num1', help="Number 1", type=float)
parser.add_argument('num2', help="Number 2", type=float)
parser.add_argument('operation', help="provide operator",
                    default = "+")
"""
# however, with positional arguments, the inputs provided must be in the right order
# otherwise the program will crash. Hence, we can make the parameters optional by adding 
# -- in front of each argument names
# we can also make a shorter abbreviation call for each of the parameters by using single -
# to use these shorthand abbrev, you must use -n=1 instead of a space
parser.add_argument('-n','--num1', help="Number 1", type=float)
parser.add_argument('-i','--num2', help="Number 2", type=float)
parser.add_argument('-o', '--operation', help="provide operator",
                    default = "+")

# once the user provides 3 arguments, those will be parsed
# now we have all the arguments in args now
args = parser.parse_args()
print(args)

# perform mathematical operations with the arguments
result = None
if args.operation == "+":
    result = args.num1 + args.num2
if args.operation == "-":
    result = args.num1 - args.num2
if args.operation == "*":
    result = args.num1 * args.num2
if args.operation == "pow":
    result = pow(args.num1, args.num2)
print(result)
