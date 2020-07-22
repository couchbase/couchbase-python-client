from collections import defaultdict
from bs4 import BeautifulSoup
import requests
import os.path
import re
from jira import JIRA
import argparse
import datetime

server = "https://issues.couchbase.com"
options = dict(server=server)
jira = JIRA(options)
project_code = "PYCBC"
project = jira.project(project_code)
print("got project {}".format(project.versions))
parser = argparse.ArgumentParser(description="Generate release notes in Asciidoc format")
parser.add_argument('version',type=str)
args=parser.parse_args()
ver_num = args.version
project_version = next(iter(filter(lambda x: x.name == ver_num, project.versions)), None)
relnotes_raw = requests.get(
    "{}/secure/ReleaseNote.jspa?projectId={}&version={}".format(server, project.id,
                                                                project_version.id))
soup = BeautifulSoup(relnotes_raw.text, 'html.parser')
content = soup.find("section", class_="aui-page-panel-content")
outputdir = os.path.join("build")

date = datetime.date.today().strftime("{day} %B %Y").format(day=datetime.date.today().day)
try:
    os.makedirs(outputdir)
except:
    pass
with open(os.path.join(outputdir, "relnotes.adoc"), "w+") as outputfile:
    section_type = None
    result = defaultdict(lambda: [])
    mapping = {"Task": "Enhancements",
               "Improvement": "Enhancements",
               "New Feature": "Enhancements",
               "Bug": "Fixes"}
    version = re.match(r'^(.*)Version ([0-9]+\.[0-9]+\.[0-9]+).*$', content.title.text).group(2)
    print("got version {}".format(version))
    for entry in content.body.find_all():
        if re.match(r'h[0-9]+', entry.name):
            print("Got section :{}".format(entry.text))
            section_type = mapping.get(entry.text.strip(), None)
            if re.match("Edit/Copy Release Notes", entry.text):
                break
        else:
            if section_type:
                items = entry.find_all('li')
                output = []
                for item in items:
                    link = item.find('a')
                    output.append(dict(link=link.get('href'), issuenumber=link.text,
                                       description=re.sub(r'^(.*?)- ', '', item.text)))
                result[section_type] += output

    output = """
== Version {version} ({date})

[source,bash]
----
pip install couchbase=={version}
----

*API Docs:* http://docs.couchbase.com/sdk-api/couchbase-python-client-{version}/

{contents}
""".format(version=version, date=date, contents='\n'.join(
        "=== {type}\n\n{value}".format(type=type, value='\n'.join(
            """* {link}[{issuenumber}]:
{description}\n""".format(**item) for item in value)) for type, value in result.items()))

    print(output)
    outputfile.write(output)
