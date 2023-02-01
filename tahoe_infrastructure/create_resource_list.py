import aws_cdk.cx_api as cx_api
import os
import shutil
import json
stack_prefix = "tahoegarg4uw1"
path = os.path.join(os.sep.join(__file__.split(os.sep)[:-1]))
try:
    os.mkdir(os.path.join(path, "stacks_generated"))
except:
    print("stacks generated directory already exists")
for x in os.listdir(os.path.join(path, "cdk.out")):
    if ".template." in x and stack_prefix in x:
        shutil.copy2(os.path.join(path, "cdk.out", x), os.path.join(path, "stacks_generated", x))

cloud_assembly = cx_api.CloudAssembly(os.path.join(path, "cdk.out"))
resources = []
print(cloud_assembly.nested_assemblies)
for stack in cloud_assembly.stacks_recursively:
    print(stack.stack_name)
    resource = stack.template["Resources"]
    for r in resource:
        resources.append({"LogicalId": r, "Type": resource[r]['Type'], "Name": [resource[r]['Properties'][x] for x in resource[r]['Properties'] if "Name" in x and type(resource[r]['Properties'][x]) is str]})
with open(os.path.join(path, "stacks_generated", 'resources.json'), "w") as fp:
    json.dump(resources, fp)